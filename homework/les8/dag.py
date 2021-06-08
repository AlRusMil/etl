from airflow import DAG
from postgres import DataTransferPostgres
from layers import SalOperator, DdsHOperator, DdsSOperator, DdsLOperator, DdsLSOperator
from datetime import datetime
import yaml
import os

with open(os.path.join(os.path.dirname(__file__), 'schema.yaml'), encoding='utf-8') as f:
    YAML_DATA = yaml.safe_load(f)

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 6, 7),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

SAE_QUERY = 'select *, {job_id} AS launch_id from {table}'  # ORDER BY 1 LIMIT 10000'

with DAG(
        dag_id="pg-data-flow",
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        max_active_runs=1,
        tags=['data-flow'],
) as dag1:
    sae = {
        table: DataTransferPostgres(
            config=dict(
                table='sae.{table}'.format(table=table)
            ),
            query=SAE_QUERY,
            task_id='sae_{table}'.format(table=table),
            source_pg_conn_str="host='db' port=5432 dbname='source_database' user='root' password='postgres'",
            pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
            pg_meta_conn_str="host='metadb' port=5432 dbname='meta_database' user='root' password='postgres'",
        )
        for table in YAML_DATA['sources']['tables'].keys()
    }

    sal = {
        table: SalOperator(
            config=dict(
                target_table=table,
                source_table=table,
            ),
            task_id='sal_{table}'.format(table=table),
            pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
            pg_meta_conn_str="host='metadb' port=5432 dbname='meta_database' user='root' password='postgres'",
        )
        for table in YAML_DATA['sources']['tables'].keys()
    }

    for target_table, task in sal.items():
        sae[target_table] >> task

    hubs = {
        hub_name: {
            table: DdsHOperator(
                task_id='dds.h_{hub_name}'.format(hub_name=hub_name),
                config={
                    'hub_name': hub_name,
                    'source_table': table,
                    'bk_column': bk_column
                },
                pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
                pg_meta_conn_str="host='metadb' port=5432 dbname='meta_database' user='root' password='postgres'",
            )
            for table, cols in YAML_DATA['sources']['tables'].items()
            for col in cols['columns']
            for bk_column, inf in col.items()
            if (inf.get('bk_for') == hub_name) and (inf.get('owner') is True)
        }
        for hub_name in YAML_DATA['groups']['hubs'].keys()
    }

    for hub, info in hubs.items():
        for source_table, task in info.items():
            sal[source_table] >> task

    hub_satellites = {
        hub_name: {
            table_name: DdsSOperator(
                task_id='dds.s_{hub_name}'.format(hub_name=hub_name),
                config=dict(
                    hub_name=hub_name,
                    bk_column=bk_column,
                    source_table=table_name,
                    satellite_columns=', '.join(satellite_columns),
                ),
                pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
                pg_meta_conn_str="host='metadb' port=5432 dbname='meta_database' user='root' password='postgres'",
            )
            for table_name, cols in YAML_DATA['sources']['tables'].items()
            for col in cols['columns']
            for bk_column, inf in col.items()
            if (inf.get('bk_for') == hub_name) and (inf.get('owner') is True)
        }
        for hub_name, info in YAML_DATA['groups']['hubs'].items()
        for _, satellite_columns in info['satellites'].items()
    }

    for hub, info in hub_satellites.items():
        for source_table, task in info.items():
            hubs[hub][source_table] >> task

    # Создаем словарь с информацией о линках
    # {
    #   link_name: {'hub_bk_config': {hub_name: bk_column
    #                              ...},
    #              'source_table': table
    # },
    # ...
    for_links = {}
    for link_name, link_info in YAML_DATA['groups']['links'].items():
        for_links[link_name] = {}

        for_links[link_name]['source_table'] = ''

        # Заполняем словарь, где ключами являются имена хабов.
        # Нужно для соблюдения корректного порядка следования хабов.
        # Так как целевая таблица формируется на основе этого словаря.
        for_links[link_name]['hub_bk_config'] = {
            hub: ''
            for hub in link_info['hubs']
        }

        for_links[link_name]['hub_st_config'] = {
            hub: table
            for hub in link_info['hubs']
            for table, cols in YAML_DATA['sources']['tables'].items()
            for col in cols['columns']
            for bk_column, inf in col.items()
            if (inf.get('bk_for') == hub) and (inf.get('owner') is True)
        }

        for table, cols in YAML_DATA['sources']['tables'].items():
            bkcolumn_in_table = 0
            for hub in link_info['hubs']:
                for col in cols['columns']:
                    for bk_column, inf in col.items():
                        if inf.get('bk_for') == hub:
                            for_links[link_name]['hub_bk_config'][hub] = bk_column
                            if inf.get('owner') is True:
                                for_links[link_name]['hub_st_config'][hub] = table
                            bkcolumn_in_table += 1
            if bkcolumn_in_table == len(link_info['hubs']):
                for_links[link_name].update(source_table=table)
                break

    links = {
        link_name: {
            for_links[link_name]['source_table']: DdsLOperator(
                task_id='l_{hubs}'.format(hubs='_'.join(f'{i}' for i in link_info['hubs'])),
                config=dict(
                    hub_bk=for_links[link_name]['hub_bk_config'],
                    source_table=for_links[link_name]['source_table'],
                ),
                pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
                pg_meta_conn_str="host='metadb' port=5432 dbname='meta_database' user='root' password='postgres'",
            )
        }
        for link_name, link_info in YAML_DATA['groups']['links'].items()
    }

    for link_name, info in links.items():
        for source_table, task in info.items():
            if source_table not in for_links[link_name]['hub_st_config'].values():
                sal[source_table] >> task
            for hub, table in for_links[link_name]['hub_st_config'].items():
                hubs[hub][table] >> task

    link_satellites = {
        link_name: {
            for_links[link_name]['source_table']: DdsLSOperator(
                task_id='l_s_{hubs}'.format(hubs='_'.join(f'{i}' for i in link_info['hubs'])),
                config=dict(
                    hub_bk=for_links[link_name]['hub_bk_config'],
                    source_table=for_links[link_name]['source_table'],
                    columns=[column_name
                             for col in YAML_DATA['sources']['tables'][for_links[link_name]['source_table']]['columns']
                             for column_name, inf in col.items()
                             if inf.get('satellite_for') is not None
                             if inf['satellite_for'].get('link') == link_name
                             ]
                ),
                pg_conn_str="host='db2' port=5432 dbname='target_database' user='root' password='postgres'",
                pg_meta_conn_str="host='metadb' port=5432 dbname='meta_database' user='root' password='postgres'",
            )
        }
        for link_name, link_info in YAML_DATA['groups']['links'].items()
        if link_info['satellites'] is True
    }

    for link_name, info in link_satellites.items():
        for source_table, task in info.items():
            links[link_name][source_table] >> task
