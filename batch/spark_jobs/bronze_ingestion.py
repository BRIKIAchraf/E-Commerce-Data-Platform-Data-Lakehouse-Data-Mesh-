from batch.utils.spark_session import get_spark_session
from batch.utils.delta_utils import merge_delta_tables
import os

def ingest_raw_to_bronze():
    spark = get_spark_session("BronzeIngestionJob")
    
    raw_orders_path = "s3a://ecommerce-raw-data-lake/orders/"
    bronze_orders_path = "s3a://ecommerce-curated-data-lake/bronze/orders"
    
    print(f"Reading raw JSON data from: {raw_orders_path}")
    raw_df = spark.read.json(raw_orders_path)
    
    # Simple incremental merge expression on Bronze orders table
    print(f"Upserting data into Bronze Delta Lake: {bronze_orders_path}")
    merge_delta_tables(
        spark=spark,
        source_df=raw_df,
        target_path=bronze_orders_path,
        join_expr="t.order_id = s.order_id"
    )
    
    print("Bronze Ingestion completed successfully.")
    spark.stop()

if __name__ == "__main__":
    ingest_raw_to_bronze()
