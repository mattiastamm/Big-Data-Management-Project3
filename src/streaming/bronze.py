"""
streaming/bronze.py — Spark Structured Streaming job.
Reads raw JSON messages from Kafka topic 'taxi-trips' and appends
the unparsed JSON string to the Iceberg table lakehouse.taxi.bronze.
No schema parsing. Bronze is an immutable raw landing zone.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP   = "kafka:29092"
KAFKA_TOPIC       = "taxi-trips"
CHECKPOINT_DIR    = "./checkpoints/bronze"
TABLE             = "lakehouse.taxi.bronze"

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
        raw_json     STRING,
        ingested_at  TIMESTAMP
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
# Extract raw JSON string — no parsing, no schema enforcement
# ---------------------------------------------------------------------------
raw_json_stream = (
    raw_stream
    .select(
        F.col("value").cast("string").alias("raw_json"),
        F.current_timestamp().alias("ingested_at"),
    )
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
    raw_json_stream.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(availableNow=True)
    .start()
)

print("Streaming query started. Waiting for data ...")
print("(Stop with Ctrl-C or query.stop())\n")
print("After ingestion you can run:")
print(f"  SELECT count(*) FROM {TABLE};")
print(f"  SELECT * FROM {TABLE} LIMIT 3;")

query.awaitTermination()
