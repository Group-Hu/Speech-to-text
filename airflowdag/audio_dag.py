import airflow

# from airflow.models import DAG
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from producer_audio import producer_audio
from consumer_audio import consumer_audio




# with DAG("my_dag",start_date=datetime(),schedule_interval="@once",catchup=False) as dag:
#     fetching_data=PythonOperator(
#         task_id='fecthing data',
#         python_callable=producer_audio,
#         dag=dag
#     ),

#     consumer_data=PythonOperator(
#         task_id='consume audio',
#         python_callable=consumer_audio,
#         dag=dag
#     )

dag=DAG(
    dag_id="my_test_dag",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    schedule_interval='@once'
)

    consumer_data=PythonOperator(
        task_id='consume audio',
        python_callable=consumer_audio,
        dag=dag
    )

    fetching_data=PythonOperator(
        task_id='fecthing data',
        python_callable=producer_audio,
        dag=dag
    )


consumer_data>>fetching_data 

