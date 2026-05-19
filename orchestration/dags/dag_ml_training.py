from airflow import DAG
from airflow.operators.empty import EmptyOperator
from orchestration.plugins.spark_submit_operator import CustomSparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id='dag_ml_training',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@weekly', # Weekly retrain
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start')

    # Spark ALS Recommender training
    train_model = CustomSparkSubmitOperator(
        task_id='spark_ml_als_training',
        application='ml/training/train_recommender.py'
    )

    end = EmptyOperator(task_id='end')

    start >> train_model >> end
