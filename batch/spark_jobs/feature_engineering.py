from batch.utils.spark_session import get_spark_session
from pyspark.sql.functions import col, count, sum, avg, current_timestamp

def generate_user_ml_features():
    spark = get_spark_session("FeatureEngineeringJob")
    
    silver_orders_path = "s3a://ecommerce-curated-data-lake/silver/orders"
    ml_features_path = "s3a://ecommerce-ml-data-lake/features/user_features"
    
    print(f"Loading silver orders data to build ML features: {silver_orders_path}")
    orders_df = spark.read.format("delta").load(silver_orders_path)
    
    # Build RFM/Aggregated user historical purchasing profiles
    user_features_df = orders_df \
        .groupBy("user_id") \
        .agg(
            count("order_id").alias("total_purchases"),
            sum("total_amount").alias("lifetime_value"),
            avg("total_amount").alias("average_order_value")
        ) \
        .withColumn("features_updated_at", current_timestamp())
        
    print(f"Writing generated ML features to S3 ML store: {ml_features_path}")
    user_features_df.write.format("parquet").mode("overwrite").save(ml_features_path)
    
    print("ML User feature engineering successfully run.")
    spark.stop()

if __name__ == "__main__":
    generate_user_ml_features()
