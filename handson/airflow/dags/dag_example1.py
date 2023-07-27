from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return 'Hello'

dag = DAG(
    dag_id='dag_example1',
    description='Hello world example',
    schedule_interval='0 12 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

hello = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag
)

world = BashOperator(
    task_id='world_task',
    bash_command="echo World!",
    dag=dag
)



hello >> world
