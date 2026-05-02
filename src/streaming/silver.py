"""
streaming/silver.py — Reads raw data from the bronze Iceberg table,
applies cleaning rules, deduplicates, adds derived columns,
enriches with taxi zone names, and writes to lakehouse.taxi.silver.
"""

import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, StringType,
)

# ---------------------------------------------------------------------------
# Schema — used to parse raw JSON stored in bronze
# ---------------------------------------------------------------------------
TAXI_SCHEMA = StructType([
    StructField("VendorID",               LongType(),   True),
    StructField("tpep_pickup_datetime",   LongType(),   True),
    StructField("tpep_dropoff_datetime",  LongType(),   True),
    StructField("passenger_count",        DoubleType(), True),
    StructField("trip_distance",          DoubleType(), True),
    StructField("RatecodeID",             DoubleType(), True),
    StructField("store_and_fwd_flag",     StringType(), True),
    StructField("PULocationID",           LongType(),   True),
    StructField("DOLocationID",           LongType(),   True),
    StructField("payment_type",           LongType(),   True),
    StructField("fare_amount",            DoubleType(), True),
    StructField("extra",                  DoubleType(), True),
    StructField("mta_tax",                DoubleType(), True),
    StructField("tip_amount",             DoubleType(), True),
    StructField("tolls_amount",           DoubleType(), True),
    StructField("improvement_surcharge",  DoubleType(), True),
    StructField("total_amount",           DoubleType(), True),
    StructField("congestion_surcharge",   DoubleType(), True),
    StructField("Airport_fee",            DoubleType(), True),
    StructField("cbd_congestion_fee",     DoubleType(), True),
])

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BRONZE_TABLE = "lakehouse.taxi.bronze"
SILVER_TABLE = "lakehouse.taxi.silver"
CHECKPOINT_DIR = "./checkpoints/silver"
ZONE_LOOKUP_PATH = "data/taxi_zone_lookup.parquet"

DEDUP_KEY = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
]

# ---------------------------------------------------------------------------
# SparkSession — all config comes from spark-defaults.conf
# ---------------------------------------------------------------------------
print("Building SparkSession ...")
spark = SparkSession.builder.appName("silver_taxi").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("SparkSession ready.")

# ---------------------------------------------------------------------------
# Wait for bronze table to exist (bronze container may still be starting)
# ---------------------------------------------------------------------------
print(f"Waiting for {BRONZE_TABLE} to be created ...")
while True:
    try:
        spark.table(BRONZE_TABLE)
        print(f"{BRONZE_TABLE} found.")
        break
    except Exception:
        print(f"  {BRONZE_TABLE} not ready yet, retrying in 10s ...")
        time.sleep(10)

# ---------------------------------------------------------------------------
# Create table if it doesn't exist
# ---------------------------------------------------------------------------
print("Creating namespace lakehouse.taxi (if not exists) ...")
spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")

print(f"Creating table {SILVER_TABLE} (if not exists) ...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
        tpep_pickup_datetime    TIMESTAMP,
        tpep_dropoff_datetime   TIMESTAMP,
        passenger_count         INT,
        trip_distance           DOUBLE,
        PULocationID            INT,
        DOLocationID            INT,
        fare_amount             DOUBLE,
        total_amount            DOUBLE,
        payment_type            INT,
        trip_duration_minutes   DOUBLE,
        pickup_date             DATE,
        pickup_zone             STRING,
        dropoff_zone            STRING,
        ingested_at             TIMESTAMP
    )
    USING iceberg
""")
print("Table ready.")

# ---------------------------------------------------------------------------
# Load zone lookup (small table, loaded once)
# ---------------------------------------------------------------------------
print(f"Loading zone lookup from {ZONE_LOOKUP_PATH} ...")
zone_df = spark.read.parquet(ZONE_LOOKUP_PATH).select(
    F.col("LocationID").cast("int").alias("LocationID"),
    F.col("Zone").alias("Zone"),
)
print(f"Zone lookup loaded: {zone_df.count()} zones.")

pickup_zones = zone_df.select(
    F.col("LocationID").alias("pu_loc_id"),
    F.col("Zone").alias("pickup_zone"),
)
dropoff_zones = zone_df.select(
    F.col("LocationID").alias("do_loc_id"),
    F.col("Zone").alias("dropoff_zone"),
)

# ---------------------------------------------------------------------------
# Read from bronze as a stream
# ---------------------------------------------------------------------------
print(f"Reading stream from {BRONZE_TABLE} ...")
bronze_stream = spark.readStream.table(BRONZE_TABLE)

# ---------------------------------------------------------------------------
# Process each micro-batch
# ---------------------------------------------------------------------------
def process_batch(batch_df, batch_id):
    count_before = batch_df.count()
    if count_before == 0:
        print(f"  Batch {batch_id}: empty, skipping.")
        return

    print(f"\n  Batch {batch_id}: {count_before:,} rows from bronze")

    # --- Parse raw JSON into typed columns ---
    df = batch_df.select(
        F.from_json(F.col("raw_json"), TAXI_SCHEMA).alias("data")
    ).select("data.*")

    # --- Cast columns ---
    df = df.select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("PULocationID").cast("int").alias("PULocationID"),
        F.col("DOLocationID").cast("int").alias("DOLocationID"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("payment_type").cast("int").alias("payment_type"),
    )

    # --- Cleaning rules ---
    # 1) Remove rows with missing timestamps
    df = df.filter(
        F.col("tpep_pickup_datetime").isNotNull() &
        F.col("tpep_dropoff_datetime").isNotNull()
    )

    # 2) Dropoff must be strictly after pickup
    df = df.filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))

    # 3) Remove trips longer than 24 hours
    df = df.filter(
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) <= 24 * 60 * 60
    )

    # 4) Remove non-positive trip distances
    df = df.filter(F.col("trip_distance") > 0)

    # 5) Remove negative fares
    df = df.filter(F.col("fare_amount") >= 0)

    # 6) Remove negative total amounts
    df = df.filter(F.col("total_amount") >= 0)

    # 7) Remove negative passenger counts
    df = df.filter(
        F.col("passenger_count").isNull() | (F.col("passenger_count") >= 0)
    )

    # 8) Remove invalid location IDs
    df = df.filter(
        (F.col("PULocationID") > 0) &
        (F.col("DOLocationID") > 0)
    )

    # --- Deduplication ---
    df = df.dropDuplicates(DEDUP_KEY)

    # --- Derived columns ---
    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) / 60.0
    )
    df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))

    # --- Zone enrichment ---
    df = df.join(F.broadcast(pickup_zones), df["PULocationID"] == pickup_zones["pu_loc_id"], "left")
    df = df.join(F.broadcast(dropoff_zones), df["DOLocationID"] == dropoff_zones["do_loc_id"], "left")
    df = df.drop("pu_loc_id", "do_loc_id")

    # --- Add ingested_at timestamp ---
    df = df.withColumn("ingested_at", F.current_timestamp())

    count_after = df.count()
    print(f"  Batch {batch_id}: {count_after:,} rows after cleaning (removed {count_before - count_after:,})")

    df.writeTo(SILVER_TABLE).append()


# ---------------------------------------------------------------------------
# Start streaming query
# ---------------------------------------------------------------------------
print(f"Starting streaming query: {BRONZE_TABLE} -> {SILVER_TABLE}")
print(f"Checkpoint directory: {CHECKPOINT_DIR}")

query = (
    bronze_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(availableNow=True)
    .start()
)

print("Streaming query started. Processing all available bronze rows ...")

query.awaitTermination()
print("Silver ingestion complete.")
