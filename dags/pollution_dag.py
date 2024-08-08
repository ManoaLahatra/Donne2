from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import sys
from task.air_pollution_api import extract_data

sys.path.append('/home/haja/PycharmProjects/airPollution/dags/task')
from task.transform import transform_data
sys.path.append('/home/haja/PycharmProjects/airPollution/dags/task')
from task.load import load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'pollution_dags',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule='@daily',
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
)

extract_1 = PythonOperator(
    task_id='extract_1',
    python_callable=extract_data,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

extract_1 >> transform >> load
