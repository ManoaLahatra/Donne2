from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

# Importing the task functions
from air_pollution_api import extract_data
from transform import transform_data
from load import load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'first_dag',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule='@daily',  # Remplacer schedule_interval par schedule
    start_date=pendulum.today('UTC').add(days=-1),  # Utiliser pendulum pour la date de début
    catchup=False,
)

# Define the tasks
extract_1 = PythonOperator(
    task_id='extract_1',
    python_callable=extract_data,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Set the task dependencies
extract_1 >> transform >> load
