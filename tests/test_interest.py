import sys
sys.path.append('/app/src')
from pyspark.sql import SparkSession
import pytest
from calculate_interest import calculate_interest

def test_calculate_interest():
    # Configure Spark session with Delta Lake support
    spark = SparkSession.builder \
        .appName("Test") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Mock wallet history: user 1, balance 200, unchanged for 2 days
    history_data = [(1, "2025-05-01", 200.0), (1, "2025-05-02", 200.0)]
    history_df = spark.createDataFrame(history_data, ["user_id", "date", "balance"])
    
    # Mock CDC: no withdrawals
    cdc_data = [(1, "2025-05-01", 200.0, "DEPOSIT", "2025-05-01 10:00:00")]
    cdc_df = spark.createDataFrame(cdc_data, ["user_id", "event_date", "amount", "transaction_type", "event_time"])
    
    # Use local path
    interest_df = calculate_interest(spark, history_df, cdc_df, 0.001, "", "/tmp/output/transactions_test")
    
    # Verify results
    assert interest_df.count() == 3  # 1 CDC row + 2 interest rows (one for each day)
    interest_row = interest_df.filter(interest_df.date == "2025-05-02").collect()[0]
    assert abs(interest_row.interest_amount - 0.2) < 0.01
    
    spark.stop()