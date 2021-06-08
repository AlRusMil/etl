from data_transfer import DataTransfer
import csv
import psycopg2


class DataTransferPostgres(DataTransfer):
    def __init__(
        self, source_pg_conn_str, query, *args, **kwargs
    ):
        super(DataTransferPostgres, self).__init__(
            source_pg_conn_str=source_pg_conn_str, query=query, *args, **kwargs
        )
        self.source_pg_conn_str = source_pg_conn_str
        self.query = query

    def provide_data(self, csv_file, provide_config, context):
        with psycopg2.connect(self.source_pg_conn_str) as pg_conn, pg_conn.cursor() as pg_cursor:
            query_to_execute = self.query.format(**provide_config)
            self.log.info("Executing query: {}".format(query_to_execute))
            pg_cursor.execute(query_to_execute)
            csvwriter = csv.writer(
                csv_file,
                delimiter="\t",
                quoting=csv.QUOTE_NONE,
                lineterminator="\n",
                escapechar='\\'
            )

            while True:
                rows = pg_cursor.fetchmany(size=1000)
                if rows:
                    for row in rows:
                        _row = list(row)
                        csvwriter.writerow(_row)
                else:
                    break
