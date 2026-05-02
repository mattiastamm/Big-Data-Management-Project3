"""
streaming/gold.py — Reads from the silver Iceberg table and maintains
a gold table with the top 5 pickup zones by trip count.
Recomputes from all silver data each time new rows arrive.
"""

import time
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SILVER_TABLE   = "lakehouse.taxi.silver"
GOLD_TABLE     = "lakehouse.taxi.gold"
CHECKPOINT_DIR = "./checkpoints/gold"

# ---------------------------------------------------------------------------
# SparkSession — all config comes from spark-defaults.conf
# ---------------------------------------------------------------------------
print("Building SparkSession ...")
spark = SparkSession.builder.appName("gold_taxi").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("SparkSession ready.")

# ---------------------------------------------------------------------------
# Wait for silver table to exist (silver container may still be starting)
# ---------------------------------------------------------------------------
print(f"Waiting for {SILVER_TABLE} to be created ...")
while True:
    try:
        spark.table(SILVER_TABLE)
        print(f"{SILVER_TABLE} found.")
        break
    except Exception:
        print(f"  {SILVER_TABLE} not ready yet, retrying in 10s ...")
        time.sleep(10)

# ---------------------------------------------------------------------------
# Read from silver as a stream
# ---------------------------------------------------------------------------
print(f"Reading stream from {SILVER_TABLE} ...")
silver_stream = spark.readStream.table(SILVER_TABLE)

# ---------------------------------------------------------------------------
# Process each micro-batch
# ---------------------------------------------------------------------------
def process_batch(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        print(f"  Batch {batch_id}: empty, skipping.")
        return

    print(f"  Batch {batch_id}: {count:,} new rows in silver, recomputing gold ...")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {GOLD_TABLE}
        USING iceberg
        PARTITIONED BY (pickup_zone)
        AS
        SELECT pickup_zone, count(*) AS trip_count
        FROM {SILVER_TABLE}
        GROUP BY pickup_zone
        ORDER BY trip_count DESC
        LIMIT 5
    """)

    print(f"  Batch {batch_id}: gold table updated.")
    spark.sql(f"SELECT * FROM {GOLD_TABLE}").show(truncate=False)


# ---------------------------------------------------------------------------
# Start streaming query
# ---------------------------------------------------------------------------
print(f"Starting streaming query: {SILVER_TABLE} -> {GOLD_TABLE}")
print(f"Checkpoint directory: {CHECKPOINT_DIR}")

query = (
    silver_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="30 seconds")
    .start()
)

print("Streaming query started. Waiting for data ...")
print("(Stop with Ctrl-C or query.stop())\n")
print("After ingestion you can run:")
print(f"  SELECT * FROM {GOLD_TABLE};")

query.awaitTermination()
