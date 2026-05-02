"""
cdc/bronze_cdc.py — Reads Debezium CDC events from Kafka and appends them
as-is to Iceberg bronze tables (one per source table).

Design:
- Append-only; bronze is never updated or deleted.
- Tombstone messages (null Kafka value produced after a hard delete) are
  recorded with is_tombstone=True so the audit trail is complete.
- Payload is extracted from the Debezium envelope ($.payload.*); the full
  raw JSON is also stored for reprocessing if the schema changes.
- Uses availableNow=True so Airflow gets a clean task boundary.

Snapshot mode: "initial" — Debezium reads all existing rows first (op='r'),
then tails the WAL for subsequent changes. This gives a complete initial state
without requiring any manual bootstrap step.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

KAFKA_BOOTSTRAP = "kafka:9092"

TOPICS = {
    "customers": {
        "topic":      "dbserver1.public.customers",
        "table":      "lakehouse.bronze.customers_cdc",
        "checkpoint": "./checkpoints/bronze_cdc_customers",
    },
    "drivers": {
        "topic":      "dbserver1.public.drivers",
        "table":      "lakehouse.bronze.drivers_cdc",
        "checkpoint": "./checkpoints/bronze_cdc_drivers",
    },
}

print("Building SparkSession ...")
spark = SparkSession.builder.appName("bronze_cdc").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("SparkSession ready.")

spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.bronze")

for name, cfg in TOPICS.items():
    print(f"Creating table {cfg['table']} (if not exists) ...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg['table']} (
            kafka_offset     BIGINT,
            kafka_partition  INT,
            kafka_timestamp  TIMESTAMP,
            is_tombstone     BOOLEAN,
            op               STRING,
            before_json      STRING,
            after_json       STRING,
            source_lsn       STRING,
            ts_ms            BIGINT,
            raw_value        STRING,
            ingested_at      TIMESTAMP
        )
        USING iceberg
    """)

# ---------------------------------------------------------------------------
# Per-batch processor — shared across both topic queries
# ---------------------------------------------------------------------------
def process_cdc_batch(batch_df, batch_id, table_name):
    count = batch_df.count()
    if count == 0:
        print(f"  Batch {batch_id} ({table_name}): empty, skipping.")
        return

    # Tombstone: Kafka record where value is NULL (signals compaction delete)
    df = batch_df.withColumn("is_tombstone", F.col("value").isNull())
    df = df.withColumn("raw_value", F.col("value").cast("string"))

    # Extract Debezium envelope fields using JSON path ($.payload.*)
    df = df.withColumn("op",
            F.get_json_object("raw_value", "$.payload.op"))
    df = df.withColumn("before_json",
            F.get_json_object("raw_value", "$.payload.before"))
    df = df.withColumn("after_json",
            F.get_json_object("raw_value", "$.payload.after"))
    df = df.withColumn("source_lsn",
            F.get_json_object("raw_value", "$.payload.source.lsn"))
    df = df.withColumn("ts_ms",
            F.get_json_object("raw_value", "$.payload.ts_ms").cast("long"))

    result = df.select(
        F.col("offset").cast("long").alias("kafka_offset"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("is_tombstone"),
        F.col("op"),
        F.col("before_json"),
        F.col("after_json"),
        F.col("source_lsn"),
        F.col("ts_ms"),
        F.col("raw_value"),
        F.current_timestamp().alias("ingested_at"),
    )

    print(f"  Batch {batch_id} ({table_name}): writing {count} rows")
    result.writeTo(table_name).append()


# ---------------------------------------------------------------------------
# Start one streaming query per topic, both with availableNow=True
# ---------------------------------------------------------------------------
queries = []
for name, cfg in TOPICS.items():
    print(f"Starting CDC bronze stream for {name} (topic: {cfg['topic']}) ...")
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", cfg["topic"])
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    q = (
        raw.writeStream
        .foreachBatch(
            lambda df, bid, t=cfg["table"]: process_cdc_batch(df, bid, t)
        )
        .option("checkpointLocation", cfg["checkpoint"])
        .trigger(availableNow=True)
        .start()
    )
    queries.append(q)

for q in queries:
    q.awaitTermination()

print("CDC bronze ingestion complete.")
