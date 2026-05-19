from batch.utils.spark_session import get_spark_session
from batch.utils.delta_utils import merge_delta_tables
from pyspark.sql.functions import col, to_timestamp, when, trim

def clean_bronze_to_silver():
    spark = get_spark_session("SilverCleaningJob")
    
    bronze_orders_path = "s3a://ecommerce-curated-data-lake/bronze/orders"
    silver_orders_path = "s3a://ecommerce-curated-data-lake/silver/orders"
    
    print(f"Reading from Bronze Delta table: {bronze_orders_path}")
    bronze_df = spark.read.format("delta").load(bronze_orders_path)
    
    # 1. Cleanings & Schema Normalizations
    cleaned_df = bronze_df \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("user_id", trim(col("user_id"))) \
        .withColumn("total_amount", col("total_amount").cast("double")) \
        .withColumn("created_at", to_timestamp(col("created_at") / 1000)) \
        .withColumn("updated_at", to_timestamp(col("updated_at") / 1000)) \
        .dropDuplicates(["order_id"]) # Deduplication
        
    print(f"Writing deduped/cleaned records to Silver Delta table: {silver_orders_path}")
    merge_delta_tables(
        spark=spark,
        source_df=cleaned_df,
        target_path=silver_orders_path,
        join_expr="t.order_id = s.order_id"
    )
    
    print("Silver cleaning pipeline execution complete.")
    spark.stop()

if __name__ == "__main__":
    clean_bronze_to_silver()
