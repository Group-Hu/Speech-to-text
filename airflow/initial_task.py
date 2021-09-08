import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from src.models.initial_model_functions import load_preprocess,fit_model


args =


{
    'owner':'airflow',
    'start_date':airflow.utils.dates.days_go(1),
    'provide_context':True 



}

dag=DAG
{
    dag_id='initial_model_DAG',
    default_args=args,
    schedule_interval= '@once',
    catchup=False,
}

task1= PythonOperator
(
    task_id='load_preprocess',
    python_callable_name='load_preprocess
)

dit['1']
transcript.get()
record['1.wav']='rais'
