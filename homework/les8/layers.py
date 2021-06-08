import logging
import time
import datetime
import psycopg2
from airflow.utils.decorators import apply_defaults
from utils import DataFlowBaseOperator


class SalOperator(DataFlowBaseOperator):  # sae -> sal
    defaults = {
        'target_schema': 'sal',
        'source_schema': 'sae',
    }

    @apply_defaults
    def __init__(self, config, pg_conn_str, query=None, *args, **kwargs):
        super(SalOperator, self).__init__(
            config=config,
            pg_conn_str=pg_conn_str,
            *args,
            **kwargs
        )
        self.pg_conn_str = pg_conn_str
        self.config = dict(self.defaults, **config)
        self.query = query

    def execute(self, context):
        with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            for launch_id in ids:
                start = time.time()
                self.config.update(
                    launch_id=launch_id,
                )

                cols_sql = """
                select column_name
                     , data_type
                  from information_schema.columns
                 where table_schema = '{target_schema}'
                   and table_name = '{target_table}'
                   and column_name not in ('launch_id', 'effective_dttm');
                """.format(**self.config)

                cursor.execute(cols_sql)
                cols_list = list(cursor.fetchall())
                cols_dtypes = ",\n".join(('{}::{}'.format(col[0], col[1]) for col in cols_list))
                cols = ",\n".join(col[0] for col in cols_list)
                # Add where launch_id = {launch_id}
                if self.query:
                    transfer_sql = """
                    with x as ({query})
                    insert into {target_schema}.{target_table} (launch_id, {cols})
                    select {job_id}::int as launch_id,\n{cols_dtypes}\n from x
                    where launch_id = {launch_id}
                    """.format(query=self.query, cols_dtypes=cols_dtypes, cols=cols, **self.config)
                else:
                    transfer_sql = """
                    insert into {target_schema}.{target_table} (launch_id, {cols})
                    select {job_id}::int as launch_id,\n{cols_dtypes}\n from {source_schema}.{source_table}
                    where launch_id = {launch_id} 
                    """.format(cols_dtypes=cols_dtypes, cols=cols, **self.config)
                self.log.info('Executing query: {}'.format(transfer_sql))
                cursor.execute(transfer_sql)

                self.config.update(
                    source_schema=self.config['source_schema'],
                    duration=datetime.timedelta(seconds=time.time() - start),
                    row_count=cursor.rowcount
                )
                self.log.info('Inserted rows: {row_count}'.format(**self.config))
                self.write_etl_log(self.config)


class DdsHOperator(DataFlowBaseOperator):  # sal -> dds for hubs
    defaults = {
        'target_schema': 'dds',
        'source_schema': 'sal',
    }

    @apply_defaults
    def __init__(self, config, pg_conn_str, *args, **kwargs):
        self.config = dict(
            self.defaults,
            target_table='h_{hub_name}'.format(**config),
            hub_bk='{hub_name}_bk'.format(**config),
            **config
        )
        super(DdsHOperator, self).__init__(
            config=config,
            pg_conn_str=pg_conn_str,
            *args,
            **kwargs
        )
        self.pg_conn_str = pg_conn_str

    def execute(self, context):
        with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:

            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            for launch_id in ids:
                start = time.time()

                self.config.update(
                    launch_id=launch_id
                )

                insert_sql = '''
                with x as (
                   select {bk_column}
                        , {job_id}
                     from {source_schema}.{source_table} s
                    where {bk_column} is not null
                      and s.launch_id = {launch_id}
                    group by 1
                )
                insert into {target_schema}.{target_table} ({hub_bk}, launch_id)
                select * from x
                    on conflict ({hub_bk})           
                    do nothing;
                '''.format(**self.config)

                self.log.info('Executing query: {}'.format(insert_sql))
                cursor.execute(insert_sql)

                self.config.update(
                    row_count=cursor.rowcount
                )
                self.log.info('Row count: {row_count}'.format(**self.config))
                self.config.update(
                    duration=datetime.timedelta(seconds=time.time() - start)
                )
                self.write_etl_log(self.config)


class DdsSOperator(DataFlowBaseOperator):   # sal -> dds for satellites of hubs
    defaults = {
        'target_schema': 'dds',
        'source_schema': 'sal',
    }

    @apply_defaults
    def __init__(self, config, pg_conn_str, *args, **kwargs):
        super(DdsSOperator, self).__init__(
            config=config,
            pg_conn_str=pg_conn_str,
            *args,
            **kwargs
        )
        self.config = dict(
            self.defaults,
            target_table='s_{hub_name}'.format(**config),
            **config
        )
        self.pg_conn_str = pg_conn_str

    def execute(self, context):
        with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            for launch_id in ids:
                start = time.time()
                self.config.update(
                    launch_id=launch_id,
                )

                insert_sql = '''
                with x as (
                    select distinct
                           h_{hub_name}_id
                         , {satellite_columns}
                      from {source_schema}.{source_table} s
                      join dds.h_{hub_name} h
                      on s.{bk_column} = h.{hub_name}_bk
                      where s.launch_id = {launch_id}
                )
                insert into {target_schema}.{target_table} (h_{hub_name}_id, {satellite_columns}, launch_id)
                select h_{hub_name}_id
                     , {satellite_columns}
                     , {job_id}
                  from x;
                '''.format(**self.config)

                self.log.info('Executing query: {}'.format(insert_sql))
                cursor.execute(insert_sql)
                self.config.update(
                    row_count=cursor.rowcount,
                    duration=datetime.timedelta(seconds=time.time() - start)
                )
                self.log.info('Row count: {row_count}'.format(**self.config))
                self.write_etl_log(self.config)


class DdsLOperator(DataFlowBaseOperator):   # sal -> dds for links
    defaults = {
        'target_schema': 'dds',
        'source_schema': 'sal',
    }

    @apply_defaults
    def __init__(self, config, pg_conn_str, *args, **kwargs):
        super(DdsLOperator, self).__init__(
            config=config,
            pg_conn_str=pg_conn_str,
            *args,
            **kwargs
        )
        self.config = dict(
            self.defaults,
            target_table='l_{hubs}'.format(hubs='_'.join(f'{i}' for i in config['hub_bk'].keys())),
            hub_ids=', '.join(f'h_{i}_id' for i in config['hub_bk'].keys()),
            **config
        )
        self.pg_conn_str = pg_conn_str

    def execute(self, context):
        with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            # Загатовки для sql-запросов в части join'ов
            joins = ''''''
            join = 'join dds.h_{hub_name} h{i} on s.{bk_column} = h{i}.{hub_name}_bk \n'
            for i, (hub_name, bk_column) in enumerate(self.config['hub_bk'].items(), 1):
                joins += join.format(i=i, hub_name=hub_name, bk_column=bk_column)

            for launch_id in ids:
                start = time.time()
                self.config.update(
                    launch_id=launch_id,
                )

                insert_sql = '''
                with x as (
                    select distinct
                           {hub_ids}
                      from {source_schema}.{source_table} s
                      {joins}
                      where s.launch_id = {launch_id}
                )
                insert into {target_schema}.{target_table} ({hub_ids}, launch_id)
                select {hub_ids}
                     , {job_id}
                  from x;
                '''.format(joins=joins, **self.config)

                self.log.info('Executing query: {}'.format(insert_sql))
                cursor.execute(insert_sql)
                self.config.update(
                    row_count=cursor.rowcount,
                    duration=datetime.timedelta(seconds=time.time() - start)
                )
                self.log.info('Row count: {row_count}'.format(**self.config))
                self.write_etl_log(self.config)


class DdsLSOperator(DataFlowBaseOperator):  # sal -> dds for satellites of links
    defaults = {
        'target_schema': 'dds',
        'source_schema': 'sal',
    }

    @apply_defaults
    def __init__(self, config, pg_conn_str, *args, **kwargs):
        super(DdsLSOperator, self).__init__(
            config=config,
            pg_conn_str=pg_conn_str,
            *args,
            **kwargs
        )
        self.config = dict(
            self.defaults,
            target_table='l_s_{hubs}'.format(hubs='_'.join(f'{i}' for i in config['hub_bk'].keys())),
            hub_ids=', '.join(f'h_{i}_id' for i in config['hub_bk'].keys()),
            link_name='_'.join(f'{i}' for i in config['hub_bk'].keys()),
            cols=', '.join(f'{i}' for i in config['columns']),
            **config
        )
        self.pg_conn_str = pg_conn_str

    def execute(self, context):
        with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            # Загатовки для sql-запросов в части join'ов
            joins_to_source = ''''''
            join = 'join dds.h_{hub_name} h{i} on s.{bk_column} = h{i}.{hub_name}_bk \n'
            for i, (hub_name, bk_column) in enumerate(self.config['hub_bk'].items(), 1):
                joins_to_source += join.format(i=i, hub_name=hub_name, bk_column=bk_column)

            # Загатовки для sql-запросов в конце в блоке с on
            ons_to_x = ''''''
            on = 'x.h_{hub_name}_id = l.h_{hub_name}_id AND \n'
            for hub_name, _ in self.config['hub_bk'].items():
                ons_to_x += on.format(hub_name=hub_name)
            ons_to_x = ons_to_x[:-5]

            for launch_id in ids:
                start = time.time()
                self.config.update(
                    launch_id=launch_id,
                )

                insert_sql = '''
                with x as (
                    select distinct
                           {hub_ids}, {cols}
                    from {source_schema}.{source_table} s
                    {joins_to_source}
                    where s.launch_id = {launch_id}
                )
                insert into {target_schema}.{target_table} (l_{link_name}_id, {cols}, launch_id)
                select l_{link_name}_id
                     , {cols}
                     , {job_id}
                  from x
                  join {target_schema}.l_{link_name} l
                  on {ons_to_x};
                '''.format(joins_to_source=joins_to_source, ons_to_x=ons_to_x, **self.config)

                self.log.info('Executing query: {}'.format(insert_sql))
                cursor.execute(insert_sql)
                self.config.update(
                    row_count=cursor.rowcount,
                    duration=datetime.timedelta(seconds=time.time() - start)
                )
                self.log.info('Row count: {row_count}'.format(**self.config))
                self.write_etl_log(self.config)
