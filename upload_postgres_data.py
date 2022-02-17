import os

import psycopg2
import psycopg2.extensions
from hdfs import InsecureClient # library docs https://hdfscli.readthedocs.io/en/latest/index.html


db_creds = {
    'host': '0.0.0.0',
    'port': '5432',
    'dbname': 'dshop_bu',
    'user': 'pguser',
    'password': 'secret'
}


def upload_postgres_data():

    client = InsecureClient(f'http://127.0.0.1:50070/', user='user')

    query_table_names = "SELECT table_name FROM information_schema.tables " \
                        "WHERE table_schema='public' " \
                        "ORDER BY table_name"

    conn = psycopg2.connect(**db_creds)
    if not conn:
        print("Error connecting to DB")
        return None
    cursor = conn.cursor()

    result = None

    try:
        cursor.execute(query_table_names)
        result = cursor.fetchall()

    except psycopg2.Error as e:
        print("Error performing query:", e)
        if conn:
            conn.rollback()
    finally:
        cursor.close()
        conn.close()

    db_tables = [x[0] for x in result]
    print(db_tables)

    dir_name = '/bronze/from_postgres'
    client.makedirs(dir_name)

    for table in db_tables:

        print(f"Working with table: {table}")

        conn = psycopg2.connect(**db_creds)
        if not conn:
            print("Error connecting to DB")
            return None
        cursor = conn.cursor()

        # TODO: Change queries for security reasons
        query_for_a_table = f"SELECT * FROM {table}"

        try:
            cursor.execute(query_for_a_table)

            file_name = dir_name + '/' + table + '.csv'

            with client.write(file_name, overwrite=True) as csv_file:
                cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)

        except psycopg2.Error as e:
            print("Error performing query:", e)
            if conn:
                conn.rollback()
        finally:
            cursor.close()
            conn.close()


if __name__ == '__main__':
    upload_postgres_data()

