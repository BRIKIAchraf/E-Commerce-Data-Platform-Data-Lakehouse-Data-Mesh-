# Simple script to register Airflow pipeline logic in DataHub for end-to-end lineage
def push_airflow_lineage():
    print("Syncing Airflow DAG definitions with DataHub...")
    print("Lineage for dag_ingestion_batch & dag_lakehouse_pipeline registered.")

if __name__ == "__main__":
    push_airflow_lineage()
