import logging
import os
import time
import datetime
import psycopg2
from contextlib import contextmanager
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from utils import DataFlowBaseOperator


class CollectStatistic(DataFlowBaseOperator):
    @apply_defaults
    def __init__(self, config, pg_conn_str, pg_meta_conn_str, *args, **kwargs):
        super(CollectStatistic, self).__init__(
            config=config, pg_conn_str=pg_conn_str, pg_meta_conn_str=pg_meta_conn_str,
            *args,
            **kwargs
        )
        self.config = config
        self.pg_conn_str = pg_conn_str
        self.pg_meta_conn_str = pg_meta_conn_str

    def provide_data(self, csv_file, provide_config, context):
        pass

    def execute(self, context):
        for table in self.config['tables'].split(','):
            self.config.update(table=table)
            # Получаем наименования колонок в таблице целевой базы, за исключением "служебных" полей
            with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:
                cursor.execute(
                    """
                select column_name
                from information_schema.columns
                where table_schema = '{schema_type}'
                      and table_name = '{table}'
                      and column_name not in ('launch_id', 'effective_dttm');
                """.format(
                        **self.config
                    )
                )
                result = cursor.fetchall()
                columns = [row for row, in result]
                # В результате имеется config с информацией по имени схемы, имени таблицы и списку колонок в этой таблице
                self.config.update(columns=columns)

            # Требуется собрать для каждой колонки информацию о том, какие даты есть в логах.
            # Но при этом по этим датам еще статистика не собрана.
            # Исходя из задания следует, что load_date и target_launch_id взаимооднозначны.
            # То есть на каждую дату приходится один job_id.
            # Соответственно, собираем информацию об этих id.

            # Словарь с наименованиями тех столбцов, которые требуется обработать
            # То есть такие столбцы, для которых в целевой таблице есть такие даты, по которым статистика не собрана.
            # Вид словаря: 'column_name': [launch_id list]
            d_column_ids_info = {}
            with psycopg2.connect(self.pg_meta_conn_str) as conn_meta, conn_meta.cursor() as cursor_meta:
                # Перебираем все столбцы
                for column in self.config['columns']:
                    # Получаем список всех target_launch_id для рассматриваемой таблицы.
                    # При этом берем те id, чьи даты отсутствуют в таблице сбора статистики.
                    # То есть те id (а значит и соответствующие даты), по которым статистика не собиралась.
                    cursor_meta.execute(
                        """
                    select target_launch_id
                    from log
                    where
                        target_schema= '{schema_type}'
                        and target_table= '{table}'
                        and load_date not in (
                                                select load_date
                                                from statistic
                                                where table_name= '{table}'
                                                      and column_name= '{column}'
                                                )
                        """.format(schema_type=self.config['schema_type'],
                                   table=self.config['table'],
                                   column=column)
                    )
                    result = cursor_meta.fetchall()
                    launch_id_list = [launch_id for launch_id, in result]
                    if launch_id_list:
                        d_column_ids_info[column] = launch_id_list

            # Формируем список конфигов по всем столбцам.
            # И для каждого столбца по всем требуемым launch_id (если за несколько дат).
            stat_configs = []
            with psycopg2.connect(self.pg_conn_str) as conn_stat, conn_stat.cursor() as cursor_stat:
                # Просмотр информации об launch_id по каждому столбцу
                for column, launch_id_list in d_column_ids_info.items():
                    for launch_id in launch_id_list:
                        local_config = {'table': self.config['table']}
                        local_config.update(
                            column=column,
                            launch_id=launch_id)
                        stat = self.count_stat(cursor_stat, local_config)
                        local_config.update(
                            cnt_nulls=stat[0],
                            cnt_all=stat[1],
                        )
                        stat_configs.append(local_config)

            for stat_config in stat_configs:
                self.write_etl_statistic(stat_config)

    def count_stat(self, cursor, local_config):
            cursor.execute(
                """
            select count(*)
            from {table}
            where launch_id={launch_id} 
                """.format(**local_config)
            )
            cnt_all = int(cursor.fetchone()[0])

            cursor.execute(
                """
            select count({column})
            from {table}
            where launch_id={launch_id} 
                """.format(**local_config)
            )
            cnt_nulls = cnt_all - int(cursor.fetchone()[0])
            return cnt_nulls, cnt_all


