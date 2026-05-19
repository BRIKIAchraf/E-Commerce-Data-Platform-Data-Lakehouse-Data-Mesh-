import pytest

@pytest.fixture(scope="session")
def spark_session():
    """Local PySpark session for unit tests"""
    try:
        from pyspark.sql import SparkSession
        import subprocess
        subprocess.run(["java", "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("PyTestSparkLocal") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        # Verify if JVM is fully operational for operations
        spark.createDataFrame([("test",)], ["col"]).collect()
        yield spark
        spark.stop()
    except (ImportError, Exception) as e:
        pytest.skip(f"Spark/Java is not installed or configured: {e}")
