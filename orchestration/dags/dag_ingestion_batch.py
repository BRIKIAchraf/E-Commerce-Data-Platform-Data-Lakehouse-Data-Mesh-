from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_ingestion_batch',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Triggers Airbyte sync daily for stripe and salesforce databases'
) as dag:

    start = EmptyOperator(task_id='start')
    
    # Mocking Trigger of Airbyte Synchronizations
    trigger_stripe_sync = EmptyOperator(task_id='trigger_stripe_to_s3_sync')
    trigger_salesforce_sync = EmptyOperator(task_id='trigger_salesforce_to_s3_sync')
    
    end = EmptyOperator(task_id='end')
    
    start >> [trigger_stripe_sync, trigger_salesforce_sync] >> end
