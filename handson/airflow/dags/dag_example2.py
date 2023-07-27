from pendulum import datetime

from airflow import DAG
from airflow_dbt.operators.dbt_operator import (
    DbtRunOperator
)


with DAG(
    dag_id="dag_example2",
    start_date=datetime(2020, 12, 23),
    description="execute dbt run",
    schedule_interval=None,
    catchup=False,
) as dag:

    dbt_run = DbtRunOperator(
        task_id='dbt_run',
        profiles_dir='/home/handson/dbt/ml_dbt',  # dbt --profiles-dir オプションと同じ
        dir='/home/handson/dbt/ml_dbt',
        models='example*'
    )

    dbt_run
