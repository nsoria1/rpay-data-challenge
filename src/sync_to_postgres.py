import logging
from pyspark.sql import SparkSession
import os

# Set up logging
logging.basicConfig(filename='/opt/spark-logs/pipeline.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def sync_to_postgres(spark: SparkSession):
    """Sync Delta tables to PostgreSQL, truncating existing data for idempotency."""
    try:
        # Load Delta tables
        transactions_df = spark.read.format("delta").load("s3a://output/transactions")
        wallet_history_df = spark.read.format("delta").load("s3a://output/wallet_history")

        # Read PostgreSQL credentials from environment variables
        postgres_user = os.getenv("POSTGRES_USER", "admin")
        postgres_password = os.getenv("POSTGRES_PASSWORD", "password")
        jdbc_url = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/cdi_bonus")

        connection_properties = {
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver"
        }

        # Truncate and write transactions
        transactions_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "transactions") \
            .option("truncate", "true") \
            .option("stringtype", "unspecified") \
            .mode("overwrite") \
            .options(**connection_properties) \
            .save()

        # Truncate and write wallet_history
        wallet_history_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "wallet_history") \
            .option("truncate", "true") \
            .option("stringtype", "unspecified") \
            .mode("overwrite") \
            .options(**connection_properties) \
            .save()

        # Log row counts
        logging.info(f"Transactions synced: {transactions_df.count()} rows")
        logging.info(f"Wallet history synced: {wallet_history_df.count()} rows")

    except Exception as e:
        logging.error(f"Failed to sync to PostgreSQL: {str(e)}")
        raise