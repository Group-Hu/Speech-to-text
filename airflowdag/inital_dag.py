import airflow

# from airflow.models import DAG
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from audio import 

import audio,consumer_audio,consumer


with DAG("my_dag",start_date=datetime(),schedule_interval="@once",catchup=False) as dag:
    fetching_data=PythonOperator(
        task_id='fecthing data',
        python_callable=
    )


