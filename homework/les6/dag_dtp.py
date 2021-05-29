from airflow import DAG
from postgres import DataTransferPostgres
from datetime import datetime


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 5, 27),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}


with DAG(
    dag_id="pg-data-flow",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['data-flow'],
) as dag1:
    t1 = DataTransferPostgres(
        config={'schema_type': 'public', 'tables': 'customer,lineitem,region,supplier'},
        query='select * from {table}',
        task_id='tables_1',
        source_pg_conn_str="host='db' port=5432 dbname='source_database' user='root' password='postgres'",
        pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
    )

    t2 = DataTransferPostgres(
        config={'schema_type': 'public', 'tables': 'nation,orders,part,partsupp'},
        query='select * from {table}',
        task_id='tables_2',
        source_pg_conn_str="host='db' port=5432 dbname='source_database' user='root' password='postgres'",
        pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
    )

    t1 >> t2
