from airflow import DAG
from airflow.operators.empty import EmptyOperator
from orchestration.plugins.spark_submit_operator import CustomSparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id='dag_lakehouse_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 2 * * *',  # Every night at 2:00 AM
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start')

    bronze_ingest = CustomSparkSubmitOperator(
        task_id='raw_to_bronze_ingestion',
        application='batch/spark_jobs/bronze_ingestion.py'
    )

    silver_cleaning = CustomSparkSubmitOperator(
        task_id='bronze_to_silver_cleaning',
        application='batch/spark_jobs/silver_cleaning.py'
    )

    feature_engineering = CustomSparkSubmitOperator(
        task_id='silver_to_ml_features',
        application='batch/spark_jobs/feature_engineering.py'
    )

    end = EmptyOperator(task_id='end')

    start >> bronze_ingest >> silver_cleaning >> feature_engineering >> end
