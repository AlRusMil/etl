import psycopg2
import os

# Параметры подключения к БД
db_config_src = {'host': 'localhost', 'port': '54320', 'dbname': 'my_database', 'user': 'root', 'password': 'postgres'}
db_config_dst = {'host': 'localhost', 'port': '5433', 'dbname': 'my_database', 'user': 'root', 'password': 'postgres'}

# Строки подключения к базам: источнику данных и целевой для загрузки данных
conn_str_source = f"host={db_config_src['host']} port={db_config_src['port']} " \
                  f"dbname={db_config_src['dbname']} user={db_config_src['user']} password={db_config_src['password']}"
conn_str_dest = f"host={db_config_dst['host']} port={db_config_dst['port']} " \
                  f"dbname={db_config_dst['dbname']} user={db_config_dst['user']} password={db_config_dst['password']}"

# Подключаемся к базе-источнику
with psycopg2.connect(conn_str_source) as conn_src, conn_src.cursor() as cursor_src:
    # Получаем список всех таблиц текущей базы данных
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name"
    cursor_src.execute(query)
    table_list = cursor_src.fetchall()
    # Перебираем все таблицы из базы-источника
    for i, table in enumerate(table_list):
        # Выводим прогресс обработки
        print("*"*20)
        print(f"Processed {i} of {len(table_list)} tables. Current table in process: {table[0]}")

        # Выводим количество строк в таблице, откуда копируем данные
        cursor_src.execute(f"SELECT count(*) FROM {table[0]}")
        print(f"Number of lines in {table[0]}: {cursor_src.fetchone()[0]}")

        # Копируем данные из таблицы в промежуточный файл
        query_copy = f"COPY {table[0]} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open(f'{table[0]}_file.csv', 'w') as f:
            cursor_src.copy_expert(query_copy, f)

        # Выводим количество строк в полученном файле
        with open(f'{table[0]}_file.csv', 'r') as f:
            count = sum(1 for _ in f)
            print(f"Данные были сохранены в файл {table[0]}_file.csv в количестве {count} строк.")

        # Копируем данные из промежуточного файла в таблицы целевой базы
        with psycopg2.connect(conn_str_dest) as conn_dst, conn_dst.cursor() as cursor_dst:
            # Выводим количество строк в целевой таблице до добавления данных
            cursor_dst.execute(f"SELECT count(*) FROM {table[0]}")
            print(f"Number of lines in {table[0]} BEFORE INSERTING: {cursor_dst.fetchone()[0]}")

            # Непосредственно копирование данных в целевую таблицу
            query_copy = f"COPY {table[0]} from STDIN WITH DELIMITER ',' CSV HEADER;"
            with open(f'{table[0]}_file.csv', 'r') as f:
                cursor_dst.copy_expert(query_copy, f)

            # Выводим количество строк в целевой таблице после добавления данных
            cursor_dst.execute(f"SELECT count(*) FROM {table[0]}")
            print(f"Number of lines in {table[0]} AFTER INSERTING: {cursor_dst.fetchone()[0]}")
        # Удаляем промежуточный файл
        os.remove(f'{os.getcwd()}/{table[0]}_file.csv')
        # Выводим информацию об удалении промежуточного файла
        print(f'Файл {os.getcwd()}/{table[0]}_file.csv удален.')
    print("*" * 20)
    print("All tables processed successfully.")
