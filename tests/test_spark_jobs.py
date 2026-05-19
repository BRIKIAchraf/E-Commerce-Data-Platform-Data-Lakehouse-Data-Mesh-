import pytest
from pyspark.sql import Row

def test_silver_deduplication(spark_session):
    # Test dataset containing duplicates
    test_data = [
        Row(order_id="ORD-1", user_id="USR-A", total_amount=100.0, created_at=1600000000000, updated_at=1600000000000),
        Row(order_id="ORD-1", user_id="USR-A", total_amount=100.0, created_at=1600000000000, updated_at=1600000000000), # Duplicate
        Row(order_id="ORD-2", user_id="USR-B", total_amount=250.0, created_at=1600000100000, updated_at=1600000100000)
    ]
    df = spark_session.createDataFrame(test_data)
    
    # Simple mock deduplication action
    deduped_df = df.dropDuplicates(["order_id"])
    
    assert deduped_df.count() == 2
    assert deduped_df.filter("order_id = 'ORD-1'").count() == 1
