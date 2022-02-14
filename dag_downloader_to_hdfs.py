from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from upload_postgres_data import upload_postgres_data
from upload_api_data import upload_api_data


default_args = {
    'owner': "airflow",
    'email': ["airflow@airflow.com"],
    'email_on_failure': False,
    'retries': 1
}

dag = DAG(
        'dag_downloader_to_hdfs',
        schedule_interval='@daily',
        start_date=datetime(2022,
                            month=2,
                            day=13,
                            hour=9,
                            minute=0),
      )


# starting dummy task
t0 = DummyOperator(
    task_id='starting_point',
    dag=dag,
)

# Next task downloads from API in format off .txt (utf-8) files
t1_1 = PythonOperator(
    task_id='upload_api_data',
    dag=dag,
    python_callable=upload_api_data
)

# Next task downloads from 'dshop_bu' DB (which contains already data from 'dshop' DB) in csv format
# - so, no need for another postgres task
t1_2 = PythonOperator(
    task_id = 'upload_postgres_data',
    dag=dag,
    python_callable=upload_postgres_data
)

# ending dummy task
t2 = DummyOperator(
    task_id='finish_point',
    dag=dag,
)


t0 >> t1_2 >> t2