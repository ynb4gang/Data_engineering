import logging
import sys
import json
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ============================================================
# Logging Setup
# ============================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TransactionFeaturePipeline")

# ============================================================
# Configuration Loader
# ============================================================
def load_config(path):
    with open(path, 'r') as f:
        return json.load(f)

# ============================================================
# SLA Monitor (Mock)
# ============================================================
def sla_check(start_time, max_minutes=30):
    elapsed = (datetime.now() - start_time).total_seconds() / 60
    if elapsed > max_minutes:
        logger.warning("⚠️ SLA breached: Pipeline exceeded allowed time.")

# ============================================================
# Spark Session
# ============================================================
def create_spark():
    return SparkSession.builder 
        .appName("TransactionFeaturePipeline") 
        .enableHiveSupport() 
        .config("spark.sql.shuffle.partitions", "200") 
        .config("spark.sql.adaptive.enabled", "true") 
        .getOrCreate()

# ============================================================
# Read Transactional Data
# ============================================================
def read_data(spark, config):
    logger.info("📥 Reading transaction, account, and customer data from Hive")
    spark.sql(f"USE {config['hive_database']}")
    transactions = spark.sql("SELECT * FROM transaction_part")
    accounts = spark.sql("SELECT * FROM account")
    customers = spark.sql("SELECT * FROM contractor")
    return transactions, accounts, customers

# ============================================================
# Handle Late-Arriving Data
# ============================================================
def filter_recent_transactions(transactions, days_back):
    latest_date = transactions.agg(max("TRANSACTION_DATE")).collect()[0][0]
    cutoff_date = latest_date - timedelta(days=days_back)
    return transactions.filter(col("TRANSACTION_DATE") >= lit(cutoff_date))

# ============================================================
# Feature Engineering
# ============================================================
def create_features(transactions, accounts, customers):
    logger.info("🛠️ Creating transactional features")

    # Window to get last transaction per account
    window_spec = Window.partitionBy("ACCOUNT_ID_INT").orderBy(col("TRANSACTION_DATE").desc())

    enriched = transactions \
        .withColumn("AMOUNT_SIGNED", col("TRANSACTION_AMOUNT") * col("TRANSACTION_SIGN")) \
        .withColumn("ROW_NUM", row_number().over(window_spec))

    agg = enriched.groupBy("ACCOUNT_ID_INT").agg(
        spark_sum("AMOUNT_SIGNED").alias("BALANCE"),
        count("TRANSACTION_ID_INT").alias("TX_COUNT"),
        spark_sum(when(col("TRANSACTION_SIGN") == 1, col("TRANSACTION_AMOUNT"))).alias("TOTAL_INCOME"),
        spark_sum(when(col("TRANSACTION_SIGN") == -1, col("TRANSACTION_AMOUNT"))).alias("TOTAL_EXPENSE"),
        max("TRANSACTION_DATE").alias("LAST_TRANSACTION_DATE")
    )

    result = agg \
        .join(accounts, on="ACCOUNT_ID_INT", how="left") \
        .join(customers, accounts.CONTRACTOR_ID_INT == customers.CONTRACTOR_ID_INT, "left")

    return result

# ============================================================
# Save Feature Store Table
# ============================================================
def save_features(df, config):
    path = config["output_path"]
    logger.info(f"💾 Saving features to {path}")
    df.write.mode("overwrite") \
        .format("parquet") \
        .option("compression", "zstd") \
        .save(path)

# ============================================================
# Main Pipeline Logic
# ============================================================
def main(config_path):
    start_time = datetime.now()

    try:
        config = load_config(config_path)
        spark = create_spark()

        transactions, accounts, customers = read_data(spark, config)
        recent_tx = filter_recent_transactions(transactions, days_back=90)

        features_df = create_features(recent_tx, accounts, customers)
        features_df.cache()

        save_features(features_df, config)
        sla_check(start_time, max_minutes=config.get("sla_minutes", 30))

        logger.info("✅ Pipeline finished successfully")

    except Exception as e:
        logger.exception("🔥 Pipeline failed: %s", e)
        sys.exit(1)

# ============================================================
# Entry Point
# ============================================================
if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python transaction_feature_pipeline.py <config_path>")
        sys.exit(1)
    main(sys.argv[1])
