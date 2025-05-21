from pyspark.sql import SparkSession
import logging
import os
from ingest_cdc import ingest_cdc
from build_history import build_wallet_history
from calculate_interest import calculate_interest
from sync_to_postgres import sync_to_postgres

# Set up logging
logging.basicConfig(filename='/opt/spark-logs/pipeline.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Initialize Spark with Delta Lake and S3 configurations
    spark = SparkSession.builder \
        .appName("CDIBonus") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp/s3a-temp") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "5000") \
        .config("spark.local.dir", "/tmp/s3a-temp") \
        .config("spark.worker.dir", "/tmp/s3a-temp") \
        .config("spark.hadoop.hadoop.tmp.dir", "/tmp/s3a-temp") \
        .config("spark.driver.host", "spark-submit") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()

    # Reduce log level
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Define paths and default rate
        default_rate = 0.001
        rates_path = "s3a://input/rates/"
        
        # Step 1: Ingest CDC data
        cdc_df = ingest_cdc(spark, "s3a://input/cdc/")
        
        # Step 2: Build wallet history
        cdc_df = cdc_df.repartition(10)
        history_df = build_wallet_history(spark, cdc_df, "s3a://output/wallet_history")
        
        # Step 3: Calculate interest
        interest_df = calculate_interest(spark, history_df, cdc_df, 
                                       default_rate=default_rate, 
                                       rates_path=rates_path,
                                       output_path="s3a://output/transactions")
        
        # Step 4: Sync to PostgreSQL
        sync_to_postgres(spark)
        
        logging.info("Pipeline completed successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()