from airflow import DAG
import datetime
import os

from airflow.sdk import Param

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DAG_ID = os.path.basename(__file__)[:-3]

args = {
    #"retries": 3
    "conn_id" : "srv_etl"
}

with DAG(
    dag_id = DAG_ID,
    start_date = datetime.datetime(year=2025, month=3, day=19),
    schedule = None,
    catchup = False,
    max_active_runs=1,
    default_args = args,
    template_searchpath=[
        "/home/mseleznev/Projects/rent_serv/code/sql_files"
    ],
    params = {
        'cut_param' : Param('', type="string"),
    }
) as dag:
    start = EmptyOperator(
        task_id = "start"
    )

    end = EmptyOperator(
        task_id = "end"
    )

    get_cut_param = SQLExecuteQueryOperator(
        task_id = "get_cut_param",
        sql = f"select cut_field_value from dwh_meta.cut_param where job_nm = '{DAG_ID}' and table_nm = 'ods.c_rostov' and cut_field_nm = 'dataflow_dttm'",
        handler = lambda cursor: cursor.fetchone()[0],
        do_xcom_push = True,
        autocommit = True
    )

    load = SQLExecuteQueryOperator(
        task_id = "load",
        sql = f"{DAG_ID}.sql"
    )

    get_new_cut_param = SQLExecuteQueryOperator(
        task_id = "get_new_cut_param",
        sql = f"select max(dataflow_dttm)::timestamp from ods.c_rostov",
        handler = lambda cursor: cursor.fetchone()[0],
        do_xcom_push = True,
        autocommit = True
    )

    update_cut_param = SQLExecuteQueryOperator(
        task_id = "update_cut_param",
        sql = "update dwh_meta.cut_param set cut_field_value = '{{ ti.xcom_pull(task_ids='get_new_cut_param') }}' where job_nm = '" + DAG_ID + "' and table_nm = 'ods.c_rostov' and cut_field_nm = 'dataflow_dttm'",
        autocommit = True
    )

    start >> get_cut_param >> load >> get_new_cut_param >> update_cut_param >> end

