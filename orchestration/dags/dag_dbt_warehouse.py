from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='dag_dbt_warehouse',
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 4 * * *', # Daily at 4:00 AM, after Spark Medallion ingestion
    catchup=False
) as dag:

    # Executes dbt pipeline to refresh staging, intermediate and Gold tables in Snowflake
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command='cd /opt/airflow/warehouse/dbt && dbt run --profiles-dir .'
    )

    dbt_test = BashOperator(
        task_id='dbt_run_tests',
        bash_command='cd /opt/airflow/warehouse/dbt && dbt test --profiles-dir .'
    )

    dbt_run >> dbt_test
