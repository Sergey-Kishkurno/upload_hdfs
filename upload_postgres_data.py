import os
import json

import psycopg2
import psycopg2.extensions
from hdfs import InsecureClient # library docs https://hdfscli.readthedocs.io/en/latest/index.html



db_creds = {
    #  keywords matter - are predefined
    'host': 'localhost',
    # 'dbname': 'dshop',
    'dbname': 'dshop_bu',
    'user': 'pguser',
    'password': 'secret'
}


def _download_to_files():

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
        print(f"{type(e).__module__.removesuffix('.errors')}:{type(e).__name__}: {str(e).rstrip()}")
        if conn:
            conn.rollback()
    finally:
        cursor.close()

    db_tables = [x[0] for x in result]
    # print(db_tables)

    for table in db_tables:

        conn = psycopg2.connect(**db_creds)
        if not conn:
            print("Error connecting to DB")
            return None
        cursor = conn.cursor()

        query_for_a_table = f"SELECT * FROM {table}"

        try:
            cursor.execute(query_for_a_table)

            with open(file= 'data/'+ table + '.csv', mode='w') as csv_file:
                cursor.copy_expert(f'COPY {table} TO STDOUT WITH HEADER CSV', csv_file)

        except psycopg2.Error as e:
            print(f"{type(e).__module__.removesuffix('.errors')}:{type(e).__name__}: {str(e).rstrip()}")
            if conn:
                conn.rollback()
        finally:
            cursor.close()



def upload_postgres_data():

    _download_to_files()

    client = InsecureClient(f'http://127.0.0.1:50070/', user='user')
    # create directories in HDFS
    client.makedirs('/from_postgres')

    # create file in HDFS
    # data = [{"name": "Anne", "salary": 10000}, {"name": "Victor", "salary": 9500}]

    # upload file to HDFS -
    client.upload('/from_postgres', './postgres_data', n_threads=0)


if __name__ == '__main__':
    upload_postgres_data()

