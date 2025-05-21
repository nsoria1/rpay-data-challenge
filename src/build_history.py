from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, lag
from pyspark.sql.window import Window
import logging

def build_wallet_history(spark: SparkSession, cdc_df, output_path: str):
    """
    Build a daily wallet history table from CDC events.
    
    Args:
        spark: SparkSession
        cdc_df: DataFrame with CDC data
        output_path: MinIO path for wallet history (e.g., s3a://output/wallet_history)
    
    Returns:
        DataFrame with daily balances per user
    """
    logging.info("Building wallet history")
    
    # Aggregate daily balance changes per user
    balance_changes = cdc_df.groupBy("user_id", "event_date") \
                           .agg(sum_("amount").alias("daily_change"))
    
    # Compute cumulative balance per user
    window_spec = Window.partitionBy("user_id").orderBy("event_date").rowsBetween(Window.unboundedPreceding, 0)
    history_df = balance_changes.withColumn("balance", sum_("daily_change").over(window_spec)) \
                                .select(col("user_id"), col("event_date").alias("date"), "balance")
    
    # Cache the history DataFrame for reuse in interest calculation
    history_df.cache()
    
    # Write to MinIO as Delta table, partitioned by date
    try:
        history_df.write.format("delta").mode("overwrite").partitionBy("date").save(output_path)
        logging.info(f"Saved wallet history to {output_path}")
    except Exception as e:
        logging.error(f"Failed to write wallet history: {str(e)}")
        raise
    
    user_count = history_df.select("user_id").distinct().count()
    logging.info(f"Wallet history contains {history_df.count()} records for {user_count} users")
    return history_df