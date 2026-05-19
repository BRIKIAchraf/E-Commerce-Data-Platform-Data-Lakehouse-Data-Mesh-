from pyspark.sql import SparkSession

def get_spark_session(app_name="ECommerceDataPlatform"):
    """Factory to build standard session with local/cloud configs and Delta Lake support"""
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "localhost:4566")  # Localstack default
    
    return builder.getOrCreate()
