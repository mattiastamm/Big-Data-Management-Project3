"""
streaming/bronze.py — Spark Structured Streaming job.
Reads raw JSON messages from Kafka topic 'taxi-trips' and appends
every field as-is to the Iceberg table lakehouse.taxi.bronze.
No transformations. Bronze is intentionally raw.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, StringType,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP   = "kafka:29092"
KAFKA_TOPIC       = "taxi-trips"
CHECKPOINT_DIR    = "./checkpoints/bronze"
TABLE             = "lakehouse.taxi.bronze"

# ---------------------------------------------------------------------------
# Schema — matches the JSON produced by produce.py
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
# SparkSession — all config comes from spark-defaults.conf
# ---------------------------------------------------------------------------
print("Building SparkSession ...")
spark = SparkSession.builder.appName("bronze_taxi").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("SparkSession ready.")

# ---------------------------------------------------------------------------
# Create namespace + table if they don't exist
# ---------------------------------------------------------------------------
print("Creating namespace lakehouse.taxi (if not exists) ...")
spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")

print(f"Creating table {TABLE} (if not exists) ...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        VendorID               BIGINT,
        tpep_pickup_datetime   BIGINT,
        tpep_dropoff_datetime  BIGINT,
        passenger_count        DOUBLE,
        trip_distance          DOUBLE,
        RatecodeID             DOUBLE,
        store_and_fwd_flag     STRING,
        PULocationID           BIGINT,
        DOLocationID           BIGINT,
        payment_type           BIGINT,
        fare_amount            DOUBLE,
        extra                  DOUBLE,
        mta_tax                DOUBLE,
        tip_amount             DOUBLE,
        tolls_amount           DOUBLE,
        improvement_surcharge  DOUBLE,
        total_amount           DOUBLE,
        congestion_surcharge   DOUBLE,
        Airport_fee            DOUBLE,
        cbd_congestion_fee     DOUBLE
    )
    USING iceberg
""")
print("Table ready.")

# ---------------------------------------------------------------------------
# Read from Kafka
# ---------------------------------------------------------------------------
print(f"Subscribing to Kafka topic '{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP} ...")
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# ---------------------------------------------------------------------------
# Parse JSON payload
# ---------------------------------------------------------------------------
parsed_stream = (
    raw_stream
    .select(F.col("value").cast("string").alias("json_str"))
    .select(F.from_json(F.col("json_str"), TAXI_SCHEMA).alias("data"))
    .select("data.*")
)

# ---------------------------------------------------------------------------
# Write to Iceberg (bronze) — append, exactly-once via checkpointing
# ---------------------------------------------------------------------------
def write_batch(batch_df, batch_id):
    count = batch_df.count()
    print(f"  Batch {batch_id}: writing {count} rows to {TABLE}")
    batch_df.writeTo(TABLE).append()


print(f"Starting streaming query -> {TABLE}")
print(f"Checkpoint directory: {CHECKPOINT_DIR}")

query = (
    parsed_stream.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="10 seconds")
    .start()
)

print("Streaming query started. Waiting for data ...")
print("(Stop with Ctrl-C or query.stop())\n")
print("After ingestion you can run:")
print(f"  SELECT count(*) FROM {TABLE};")
print(f"  SELECT * FROM {TABLE} LIMIT 3;")

query.awaitTermination()
