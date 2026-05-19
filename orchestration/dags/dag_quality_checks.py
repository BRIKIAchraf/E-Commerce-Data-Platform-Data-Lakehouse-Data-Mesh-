from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='dag_quality_checks',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Triggering Great Expectations checkpoint validations on active tables
    ge_orders_validation = BashOperator(
        task_id='validate_orders_expectations',
        bash_command='great_expectations checkpoint run daily_orders_checkpoint'
    )

    ge_orders_validation
