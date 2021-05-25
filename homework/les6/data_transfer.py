import logging
import os
import psycopg2
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataTransfer(BaseOperator):
    @apply_defaults
    def __init__(self, config, pg_conn_str, *args, **kwargs):
        super(DataTransfer, self).__init__(
            *args,
            **kwargs
        )
        self.config = config
        self.pg_conn_str = pg_conn_str

    def provide_data(self, csv_file, tablename, context):
        pass

    def execute(self, context):
        copy_statement = """
        COPY {target_schema}.{target_table} ({columns}) FROM STDIN with
        DELIMITER '\t'
        CSV
        ESCAPE '\\'
        NULL '';
        """

        for table in self.config['tables'].split(','):
            self.config.update(
                target_schema=self.config['schema_type'],
                target_table=table,
            )
            with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:
                cursor.execute(
                    """
                select column_name
                  from information_schema.columns
                 where table_schema = '{target_schema}'
                   and table_name = '{target_table}';
                """.format(
                        **self.config
                    )
                )
                result = cursor.fetchall()
                columns = ", ".join('"{}"'.format(row) for row, in result)
                self.config.update(columns=columns)

                with open("transfer.csv", "w", encoding="utf-8") as csv_file:
                    self.provide_data(csv_file, table, context)

                self.log.info("writing succed")

                with open('transfer.csv', 'r', encoding="utf-8") as f:
                    cursor.copy_expert(copy_statement.format(**self.config), f)
        os.remove('transfer.csv')
