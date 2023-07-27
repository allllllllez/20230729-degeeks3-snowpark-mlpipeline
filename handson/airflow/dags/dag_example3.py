from airflow import DAG
from airflow_dbt.operators.dbt_operator import (
    DbtRunOperator
)
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(0),
    'retries': 0,
}

with DAG(dag_id='dag_example3', default_args=default_args, schedule_interval=None) as dag:
    dbt_run = DbtRunOperator(
        task_id='dbt_run',
        profiles_dir='/home/handson/dbt/ml_dbt',  # dbt --profiles-dir オプションと同じ
        dir='/home/handson/dbt/ml_dbt',
        models='+regression_model'
    )

dbt_run
