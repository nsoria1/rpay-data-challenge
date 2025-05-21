from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, to_date
from pyspark.sql.window import Window
import logging

def load_daily_rates(spark: SparkSession, rates_path: str, default_rate: float):
    """
    Load daily interest rates from MinIO, fall back to default if unavailable.
    
    Args:
        spark: SparkSession
        rates_path: MinIO path for rates (e.g., s3a://input/rates/)
        default_rate: Default rate to use if file is unavailable
    
    Returns:
        DataFrame with date and rate, or None if using default
    """
    try:
        rates_df = spark.read.parquet(rates_path)
        if not {"date", "rate"}.issubset(rates_df.columns):
            logging.warning(f"Rates file at {rates_path} missing required columns, using default rate {default_rate}")
            return None
        rates_df = rates_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
                           .withColumn("rate", col("rate").cast("double")) \
                           .filter(col("date").isNotNull()) \
                           .filter(col("rate").isNotNull())
        logging.info(f"Loaded {rates_df.count()} daily rates from {rates_path}")
        return rates_df
    except Exception as e:
        logging.warning(f"Failed to load rates file from {rates_path}: {str(e)}, using default rate {default_rate}")
        return None

def calculate_interest(spark: SparkSession, history_df, cdc_df, default_rate: float, rates_path: str, output_path: str):
    """
    Calculate daily interest for qualifying balances and generate transactions.
    
    Args:
        spark: SparkSession
        history_df: DataFrame with wallet history (user_id, date, balance)
        cdc_df: DataFrame with CDC data (to check transaction types)
        default_rate: Default daily interest rate (e.g., 0.001 for 0.1%)
        rates_path: MinIO path for daily rates (e.g., s3a://input/rates/)
        output_path: MinIO path for transactions (e.g., s3a://output/transactions)
    
    Returns:
        DataFrame with interest transactions
    """
    logging.info("Calculating interest")
    
    rates_df = load_daily_rates(spark, rates_path, default_rate)
    
    movement_df = cdc_df.filter(col("transaction_type").isin("WITHDRAWAL", "TRANSFER_OUT")) \
                       .groupBy("user_id", "event_date") \
                       .agg({"*": "count"}) \
                       .withColumnRenamed("event_date", "date") \
                       .withColumnRenamed("count(1)", "has_movement")
    
    history_with_movement = history_df.join(movement_df, ["user_id", "date"], "left") \
                                     .withColumn("has_movement", col("has_movement").isNotNull())
    
    window_spec = Window.partitionBy("user_id").orderBy("date")
    history_with_movement = history_with_movement.withColumn("prev_movement", lag("has_movement").over(window_spec)) \
                                                .withColumn("is_unmoved", 
                                                            when(col("prev_movement").isNull(), True) \
                                                            .when(col("prev_movement") == False, True) \
                                                            .otherwise(False))
    
    qualifying_df = history_with_movement.filter((col("balance") > 100) & (col("is_unmoved") == True))
    
    if rates_df is not None:
        qualifying_df = qualifying_df.join(rates_df, ["date"], "left") \
                                    .withColumn("daily_rate", col("rate").cast("double").fillna(default_rate))
    else:
        qualifying_df = qualifying_df.withColumn("daily_rate", col("balance") * 0 + default_rate)
    
    interest_df = qualifying_df.select(
        col("user_id"),
        col("date"),
        (col("balance") * col("daily_rate")).alias("interest_amount")
    )
    
    cdc_transactions = cdc_df.select(
        col("user_id"),
        col("event_date").alias("date"),
        col("amount").alias("interest_amount")
    )
    transactions_df = cdc_transactions.unionByName(interest_df)
    
    try:
        transactions_df.write.format("delta").mode("overwrite").partitionBy("date").save(output_path)
        transaction_count = transactions_df.count()
        total_interest = transactions_df.agg({"interest_amount": "sum"}).collect()[0][0] or 0
        logging.info(f"Saved {transaction_count} transactions to {output_path}, total interest: ${total_interest:.2f}")
    except Exception as e:
        logging.error(f"Failed to write transactions: {str(e)}")
        raise
    
    return transactions_df