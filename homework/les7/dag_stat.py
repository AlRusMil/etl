import logging
import pendulum
from airflow.utils.state import State
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from collect_statistic import CollectStatistic
from datetime import datetime


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 5, 28),
    "retries": 0,
    "email_on_failure": False,
    "on_failure_callback": False,
    "email_on_retry": False,
    "depends_on_past": False,
    "wait_for_downstream": False,
}


with DAG(
    dag_id="pg-collect-statistic",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['collect-statistic'],
) as dag1:
    sensor = ExternalTaskSensor(
        task_id='sensor',
        external_dag_id="pg-data-flow",
        external_task_id=None,
        allowed_states=[State.SUCCESS],
        check_existence=False,

        retries=0,
        mode='reschedule',
        poke_interval=10,
        timeout=60 * 5,
        soft_fail=True,
    )

    statistic1 = CollectStatistic(
        config={'schema_type': 'public', 'tables': 'nation,supplier'},
        task_id='stat_nation_supplier',
        pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
        pg_meta_conn_str="host='metadb' port=5432 dbname='meta_database' user='root' password='postgres'",
    )

    statistic2 = CollectStatistic(
        config={'schema_type': 'public', 'tables': 'customer'},
        task_id='stat_customer',
        pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
        pg_meta_conn_str="host='metadb' port=5432 dbname='meta_database' user='root' password='postgres'",
    )

    sensor >> statistic1 >> statistic2
