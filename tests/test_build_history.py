import sys
sys.path.append('/app/src')
from pyspark.sql import SparkSession
import pytest
from build_history import build_wallet_history

def test_build_wallet_history():
    # Configure Spark session with Delta Lake support
    spark = SparkSession.builder \
        .appName("Test") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Mock CDC data: user 1 deposits 100 on day 1, 50 on day 2
    data = [(1, "2025-05-01", 100.0), (1, "2025-05-02", 50.0)]
    cdc_df = spark.createDataFrame(data, ["user_id", "event_date", "amount"])
    
    # Use local path
    history_df = build_wallet_history(spark, cdc_df, "/tmp/output/wallet_history_test")
    
    # Verify results
    assert history_df.count() == 2
    rows = history_df.orderBy("date").collect()
    assert rows[0].balance == 100.0
    assert rows[1].balance == 150.0
    
    spark.stop()