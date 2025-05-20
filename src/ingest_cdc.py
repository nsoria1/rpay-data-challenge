from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date
import logging

def ingest_cdc(spark: SparkSession, input_path: str):
    """
    Read CDC Parquet files from MinIO, validate, transform, and return a DataFrame.
    
    Args:
        spark: SparkSession
        input_path: MinIO path (e.g., s3a://input/cdc/)
    
    Returns:
        DataFrame with validated and transformed CDC data
    
    Raises:
        ValueError: If required columns are missing in the input data
        Exception: If reading Parquet files fails
    """
    logging.info(f"Reading CDC data from {input_path}")

    # Read CDC data from Parquet files
    cdc_df = spark.read.parquet(input_path)

    # Transform event_time to timestamp and extract event_date
    cdc_df = cdc_df.withColumn("event_time", to_timestamp("event_time")) \
                   .withColumn("event_date", to_date("event_time"))

    # Cast amount to double
    cdc_df = cdc_df.withColumn("amount", cdc_df["amount"].cast("double"))

    # Explicitly select all columns we need, including transaction_type
    cdc_df = cdc_df.select(
        "event_time",
        "user_id",
        "account_id",
        "amount",
        "transaction_type",  # Ensure this column is retained
        "cdc_operation",
        "cdc_sequence_num",
        "source_system",
        "event_date"
    )

    # Filter out null values for key columns
    cdc_df = cdc_df.filter(cdc_df["event_time"].isNotNull()) \
                   .filter(cdc_df["user_id"].isNotNull()) \
                   .filter(cdc_df["amount"].isNotNull()) \
                   .filter(cdc_df["event_date"].isNotNull())

    # Repartition by event_date for better parallelism
    cdc_df = cdc_df.repartition("event_date").repartition(10)

    return cdc_df