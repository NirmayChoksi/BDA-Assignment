"""Spark ETL job: enrich the raw UPI transactions.

Reads the raw CSV from HDFS, adds derived date/segment columns, and writes
partitioned Parquet back to HDFS. Hive then reads that Parquet through the
`upi.transactions_enriched` table (see sql/ddl.sql).

Run with:
    .venv/bin/spark-submit --master local[*] spark/etl.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW = "hdfs:///data/raw/transactions.csv"
CURATED = "hdfs:///data/curated/upi_enriched"

spark = SparkSession.builder.appName("upi-etl").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Raw CSV has awkward header names ("amount (INR)"), normalise them to match
# the names already used by the Hive queries.
df = (
    spark.read.csv(RAW, header=True, inferSchema=True)
    .withColumnRenamed("transaction id", "txn_id")
    .withColumnRenamed("transaction type", "txn_type")
    .withColumnRenamed("amount (INR)", "amount")
    .withColumnRenamed("transaction_status", "txn_status")
    .withColumnRenamed("timestamp", "ts")
)

# Derived columns
df = (
    df.withColumn("transaction_date", F.to_date("ts"))
    .withColumn("month_name", F.date_format("ts", "MMMM"))
    .withColumn("quarter", F.quarter("ts"))
    .withColumn("week_of_year", F.weekofyear("ts"))
    .withColumn("day_type", F.when(F.col("is_weekend") == 1, "Weekend").otherwise("Weekday"))
    .withColumn("is_peak_hour", F.when(F.col("hour_of_day").between(17, 21), 1).otherwise(0))
    .withColumn(
        "amount_category",
        F.when(F.col("amount") < 500, "Small")
        .when(F.col("amount") < 2000, "Medium")
        .otherwise("Large"),
    )
    .withColumn("is_successful", F.when(F.col("txn_status") == "SUCCESS", 1).otherwise(0))
    # year/month become the Parquet partition keys (written as directories)
    .withColumn("year", F.year("ts"))
    .withColumn("month", F.month("ts"))
)

# sanity check: catches column-name mismatches between Spark and Hive
nulls = df.filter(F.col("ts").isNull() | F.col("txn_status").isNull()).count()
assert nulls == 0, f"{nulls} rows have null ts/txn_status - check column renames"

df.write.mode("overwrite").partitionBy("year", "month").parquet(CURATED)
print(f"wrote {df.count()} rows to {CURATED}")
spark.stop()
