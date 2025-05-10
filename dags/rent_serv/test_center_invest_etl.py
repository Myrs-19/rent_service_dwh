from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime

import boto3

SQL_CREATE_EXTERNAL_TABLE = """
create external table cars_s3(
    mark text,                      -- марка автомобиля
    model text,                     -- модель автомобиля
    engine_capacity real,           -- объем двигателя
    year_of_release int,       	    -- год выпуска
    cost numeric,                   -- стоимость автомобиля
    file_date_of_relevance date     -- дата актуальности файла
)
location ('pxf://mseleznev-test-center-invest-cars-csv/{{execution_date.year}}-{{execution_date.month}}-{{execution_date.day}}.csv?PROFILE=s3:csv&SERVER=s3')
format 'CSV' (header)"""

SQL_CREATE_DELTA = """
create or replace view delta as
with source_(
    dataflow_id
    , dataflow_dttm
    , car_rk
    , valid_from_dttm
    , hashdiff_key
    , mark
    , model
    , engine_capacity
    , year_of_release
    , cost
    , file_date_of_relevance
) as (
    select
        '{{run_id}}'::text as dataflow_id
        , '{{execution_date}}'::timestamp as dataflow_dttm
        -- формируем ключ авто
        , md5(mark || '#' || model || '#' || engine_capacity || '#' || year_of_release) as car_rk
        , '{{execution_date}}'::timestamp as valid_from_dttm
        -- формируем хэш для сравнения полей
        , md5(mark || '#' || model || '#' || engine_capacity || '#' || year_of_release || '#' || cost) as hashdiff_key
        , mark
        , model
        , engine_capacity
        , year_of_release
        , cost * ({{ti.xcom_pull(task_ids="extract_rate", key="usd_rate")}}/1) as cost
        , file_date_of_relevance
    from
        cars_s3
)
-- формируем дельту для новых и измененных записей
, insert_update_delta(
    dataflow_id
    , dataflow_dttm
    , car_rk
    , valid_from_dttm
    , hashdiff_key
    , actual_flg
    , delete_flg
    , mark
    , model
    , engine_capacity
    , year_of_release
    , cost
    , file_date_of_relevance
) as (
    select 
        dataflow_id
        , dataflow_dttm
        , car_rk
        , valid_from_dttm
        , hashdiff_key
        , 1 actual_flg
        , 0 delete_flg
        , mark
        , model
        , engine_capacity
        , year_of_release
        , cost
        , file_date_of_relevance
    from 
        source_ as s
    where car_rk in (
		select 
            car_rk
        from (
		    -- формируем новые car_rk, hashdiff_key для измененных или новых записей из источника
		    select
		        car_rk
		        , hashdiff_key
		    from
		        source_
		    except
		    select 
		        car_rk
		        , hashdiff_key   
		    from 
		        -- выбираем только актуальные не удаленные записи
		        cars 
		    where actual_flg = 1 and delete_flg = 0
		) as foo
	) 
)
-- формируем дельту для удаленных записей
, delete_delta(
    dataflow_id
    , dataflow_dttm
    , car_rk
    , valid_from_dttm
    , hashdiff_key
    , actual_flg
    , delete_flg
    , mark
    , model
    , engine_capacity
    , year_of_release
    , cost
    , file_date_of_relevance
) as (
    select
        '{{run_id}}'::text as dataflow_id
        , '{{execution_date}}'::timestamp as dataflow_dttm
        , car_rk
        , '{{execution_date}}'::timestamp as valid_from_dttm
        , hashdiff_key
        , 1 actual_flg
        , 1 delete_flg
        , mark
        , model
        , engine_capacity
        , year_of_release
        , cost
        , file_date_of_relevance
    from cars
    where car_rk in (
        select 
            car_rk
        from (
            -- формируем car_rk, которых нет в источнике
            select
                car_rk
            from 
                cars
            where delete_flg = 0 and actual_flg = 1
            except
            select
                car_rk
            from 
                source_ 
        ) as foo
    ) 
    and actual_flg = 1 
    and delete_flg = 0
)
-- объединяем дельты
select 
    dataflow_id
    , dataflow_dttm
    , car_rk
    , valid_from_dttm
    , hashdiff_key
    , actual_flg
    , delete_flg
    , mark
    , model
    , engine_capacity
    , year_of_release
    , cost
    , file_date_of_relevance
from
    insert_update_delta
union all
select 
    dataflow_id
    , dataflow_dttm
    , car_rk
    , valid_from_dttm
    , hashdiff_key
    , actual_flg
    , delete_flg
    , mark
    , model
    , engine_capacity
    , year_of_release
    , cost
    , file_date_of_relevance
from
    delete_delta"""

SQL_INSERT_DELTA_INTO_TARGET = """
insert into cars (
    dataflow_id
	, dataflow_dttm
	, car_rk
	, valid_from_dttm
	, hashdiff_key
	, actual_flg
	, delete_flg
	, mark
	, model
	, engine_capacity
	, year_of_release
	, cost
	, file_date_of_relevance
)
select 	
    dataflow_id
	, dataflow_dttm
	, car_rk
	, valid_from_dttm
	, hashdiff_key
	, actual_flg
	, delete_flg
	, mark
	, model
	, engine_capacity
	, year_of_release
	, cost
	, file_date_of_relevance
from delta
"""

SQL_UPDATE_TARGET = """
UPDATE cars as c
SET actual_flg = 0
WHERE valid_from_dttm IN 
(
    -- для текущей записи из cars получаем минимальную дату актуальности
    -- при условии, что актуальных записей больше 1
	SELECT 
		min(valid_from_dttm)
	FROM 
		cars
	WHERE actual_flg = 1 and c.car_rk = car_rk
	GROUP BY car_rk
	HAVING COUNT(*) > 1
)"""

def extract_usd_rate(ti):
    import xml.etree.ElementTree as ET

    xml_data = ti.xcom_pull(task_ids="fetch_currency_rate")
    root = ET.fromstring(xml_data)
    for valute in root.findall(".//Valute"):
        if valute.find("CharCode").text == "USD":
            usd_rate = float(valute.find("Value").text.replace(",", "."))
            print('usd_rate=', usd_rate)
            ti.xcom_push(key="usd_rate", value=usd_rate)
            break

def is_exists(**kwargs):
    """
    Функция проверяет существует ли в s3 файл на день обработки и вовращает строку - названия таска, который будет запущен. 
    В словарь **kwargs передаем словарь с контекстом
    """
    # Конфигурация подключения к s3
    # ключи здесь хранить нельзя, можно, например, хранить либо в переменных окружения, либо в переменных airflow
    s3_client = boto3.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id='YCAJExrOWdWp-DEZ4Hb4FK1eo',
        aws_secret_access_key='YCO0Py3lbl3_V-agQG2sGYHVZd9qj4l9fDQQB7Ab'
    )

    bucket_name = 'mseleznev-test-center-invest-cars-csv'
    file_key = f"{kwargs['execution_date'].year}-{kwargs['execution_date'].month}-{kwargs['execution_date'].day}.csv"

    try:
        # проверка на наличие файла в s3 на дату обработки
        response = s3_client.head_object(Bucket=bucket_name, Key=file_key)
        return "start_process"
    except s3_client.exceptions.ClientError:
        return "end"

with DAG(
    dag_id="test_center_invest_etl6",
    # после 23:59 каждый день недели с понедельника по субботу.
    schedule="59 23 * * 1-5", 
    start_date=datetime(year=2024, month=11, day=13),
    catchup=False
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    # проверяем на наличие файла на дату обработки
    is_file_exists = BranchPythonOperator(
        task_id="is_file_exists",
        python_callable=is_exists,
        provide_context=True, # передаем контекст
    )

    start_process = EmptyOperator(
        task_id="start_process"
    )

    # дропаем внешнюю таблицу
    drop_external_table = PostgresOperator(
        task_id="drop_external_table",
        postgres_conn_id="postgres_default",
        database="gpadmin",
        sql="""drop external table if exists cars_s3 cascade"""
    )

    # создаем внешнюю таблицу на файл даты обработки
    create_external_table = PostgresOperator(
        task_id="create_external_table",
        postgres_conn_id="postgres_default",
        database="gpadmin",
        sql=SQL_CREATE_EXTERNAL_TABLE
    )

    # получаем курс доллара к рублю
    fetch_currency_rate = SimpleHttpOperator(
        task_id="fetch_currency_rate",
        method="GET",
        http_conn_id="http_default",  # Определите HTTP-соединение в Airflow
        endpoint="scripts/xml_daily.asp?date_req={{execution_date.day}}/{{execution_date.month}}/{{execution_date.year}}",
        response_filter=lambda response: response.text,
        log_response=True,
    )

    # извлечение курса
    extract_rate = PythonOperator(
        task_id="extract_rate",
        python_callable=extract_usd_rate,
    )

    # формируем дельту изменений
    create_delta = PostgresOperator(
        task_id="create_delta",
        postgres_conn_id="postgres_default",
        database="gpadmin",
        sql=SQL_CREATE_DELTA
    )

    # вставляем дельту изменений в таргет
    insert_delta_into_target = PostgresOperator(
        task_id="insert_delta_into_target",
        postgres_conn_id="postgres_default",
        database="gpadmin",
        sql=SQL_INSERT_DELTA_INTO_TARGET
    )

    # обновляем таргет - обновляем флаги актуальности для уже неактуальных записей
    update_target = PostgresOperator(
        task_id="update_target",
        postgres_conn_id="postgres_default",
        database="gpadmin",
        sql=SQL_UPDATE_TARGET
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed"
    )
    
    start >> is_file_exists
    is_file_exists >> [start_process, end]
    start_process >> drop_external_table >> create_external_table >> fetch_currency_rate 
    fetch_currency_rate >> extract_rate
    extract_rate >> create_delta >> insert_delta_into_target >> update_target 
    update_target >> end