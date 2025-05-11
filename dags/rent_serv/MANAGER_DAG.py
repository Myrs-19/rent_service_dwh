from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import datetime

import os

DAG_ID = os.path.basename(__file__)[:-3]

args = {
    #"retries": 3

}

with DAG(
    dag_id = DAG_ID,
    start_date = datetime.datetime(year=2025, month=3, day=19),
    schedule = "0 5,23 * * *", # каждый день в 5 утра и 23 вечера по utc
    catchup = False,
    max_active_runs=1,
    default_args = args
) as dag:
    start = EmptyOperator(
        task_id = "start",
        trigger_rule="one_success"
    )

    end = EmptyOperator(
        task_id = "end",
        trigger_rule="one_success"
    )

    ods_start = EmptyOperator(
        task_id = "ods_start",
        trigger_rule="one_success"
    )

    ods_end = EmptyOperator(
        task_id = "ods_end",
        trigger_rule="one_success"
    )

    rv_start = EmptyOperator(
        task_id = "rv_start",
        trigger_rule="one_success"
    )

    rv_end = EmptyOperator(
        task_id = "rv_end",
        trigger_rule="one_success"
    )

    '''ODS слой
    каждый поток запускается параллельно остальным
    '''

    # даг загрузки excel файла в таблицу ods слоя
    triger_ods_cian_load_rostov = TriggerDagRunOperator(
        task_id = "triger_ods_cian_load_rostov",
        trigger_dag_id = "ODS_CIAN_ROSTOV_LOAD",
        wait_for_completion = True
    )

    '''rv слой'''
    triger_H_APPARTMENT_OFFER_LOAD = TriggerDagRunOperator(
        task_id = "triger_h_appartment_offer_load",
        trigger_dag_id = "H_APPARTMENT_OFFER_LOAD",
        wait_for_completion = True
    )

    start >> ods_start

    ods_start >> triger_ods_cian_load_rostov
    triger_ods_cian_load_rostov >> ods_end

    ods_end >> rv_start

    rv_start >> triger_H_APPARTMENT_OFFER_LOAD

    triger_H_APPARTMENT_OFFER_LOAD >> rv_end

    rv_end >> end