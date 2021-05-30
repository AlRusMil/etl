from airflow import DAG
from postgres import DataTransferPostgres
from datetime import datetime


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 5, 28),
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
        config={'schema_type': 'public', 'tables': 'nation,region,supplier', 'jobid_colname': 'launch_id'},
        query='select *, {job_id} AS {jobid_colname} from {table}',
        task_id='tables_nrs',
        source_pg_conn_str="host='db' port=5432 dbname='source_database' user='root' password='postgres'",
        pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
        pg_meta_conn_str="host='metadb' port=5432 dbname='meta_database' user='root' password='postgres'",
    )

    t2 = DataTransferPostgres(
        config={'schema_type': 'public', 'tables': 'customer,part,partsupp', 'jobid_colname': 'launch_id'},
        query='select *, {job_id} AS {jobid_colname} from {table}',
        task_id='tables_cpp',
        source_pg_conn_str="host='db' port=5432 dbname='source_database' user='root' password='postgres'",
        pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
        pg_meta_conn_str="host='metadb' port=5432 dbname='meta_database' user='root' password='postgres'",
    )

    t3 = DataTransferPostgres(
        config={'schema_type': 'public', 'tables': 'orders', 'jobid_colname': 'launch_id'},
        query='select *, {job_id} AS {jobid_colname} from {table}',
        task_id='tables_o',
        source_pg_conn_str="host='db' port=5432 dbname='source_database' user='root' password='postgres'",
        pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
        pg_meta_conn_str="host='metadb' port=5432 dbname='meta_database' user='root' password='postgres'",
    )

    t1 >> t2 >> t3
