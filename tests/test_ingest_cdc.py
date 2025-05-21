import sys
sys.path.append('/app/src')
from pyspark.sql import SparkSession
import pytest
from ingest_cdc import ingest_cdc

def test_ingest_cdc():
    # Basic Spark session (no Delta Lake needed for Parquet)
    spark = SparkSession.builder \
        .appName("Test") \
        .getOrCreate()
    
    # Mock CDC data with all expected columns
    data = [(1, "2025-05-01 10:00:00", "DEPOSIT", 100.0, 123, "INSERT", 1, "system", "2025-05-01")]
    cdc_df = spark.createDataFrame(data, ["user_id", "event_time", "transaction_type", "amount", "account_id", "cdc_operation", "cdc_sequence_num", "source_system", "event_date"])
    
    # Write to local path
    cdc_df.write.mode("overwrite").parquet("/tmp/input/cdc_test")
    result_df = ingest_cdc(spark, "/tmp/input/cdc_test")
    
    # Verify results
    assert result_df.count() == 1
    row = result_df.collect()[0]
    assert row.user_id == 1
    assert row.transaction_type == "DEPOSIT"
    assert row.amount == 100.0
    
    spark.stop()