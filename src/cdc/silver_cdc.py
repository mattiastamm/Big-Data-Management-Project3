"""
cdc/silver_cdc.py — Reads ALL rows from each bronze CDC table, deduplicates
to the latest event per primary key, then MERGEs into the silver Iceberg table
so that silver mirrors the current state of the PostgreSQL source.

Idempotency guarantee:
  1. Deduplication keeps exactly one row per PK (the latest by ts_ms, then
     kafka_offset). The source presented to MERGE is therefore a pure
     point-in-time snapshot.
  2. MERGE semantics are deterministic: given the same source, the target
     always ends up in the same state regardless of how many times this job
     runs. A second run for the same bronze data is a no-op.

MERGE logic:
  - MATCHED   AND op = 'd'            → DELETE
  - MATCHED   AND op IN ('u','c','r') → UPDATE SET *
  - NOT MATCHED AND op != 'd'         → INSERT *
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, BooleanType, LongType,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BRONZE_CUSTOMERS = "lakehouse.bronze.customers_cdc"
BRONZE_DRIVERS   = "lakehouse.bronze.drivers_cdc"
SILVER_CUSTOMERS = "lakehouse.silver.customers"
SILVER_DRIVERS   = "lakehouse.silver.drivers"

CUSTOMERS_AFTER_SCHEMA = StructType([
    StructField("id",         IntegerType(), True),
    StructField("name",       StringType(),  True),
    StructField("email",      StringType(),  True),
    StructField("country",    StringType(),  True),
    StructField("created_at", LongType(),    True),  # microseconds epoch
])

DRIVERS_AFTER_SCHEMA = StructType([
    StructField("id",             IntegerType(), True),
    StructField("name",           StringType(),  True),
    StructField("license_number", StringType(),  True),
    StructField("rating",         DoubleType(),  True),
    StructField("city",           StringType(),  True),
    StructField("active",         BooleanType(), True),
    StructField("created_at",     LongType(),    True),
])

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
print("Building SparkSession ...")
spark = SparkSession.builder.appName("silver_cdc").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("SparkSession ready.")

spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.silver")

# ---------------------------------------------------------------------------
# Silver DDL (idempotent)
# ---------------------------------------------------------------------------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_CUSTOMERS} (
        id          INT,
        name        STRING,
        email       STRING,
        country     STRING,
        updated_ts  BIGINT
    )
    USING iceberg
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_DRIVERS} (
        id               INT,
        name             STRING,
        license_number   STRING,
        rating           DOUBLE,
        city             STRING,
        active           BOOLEAN,
        updated_ts       BIGINT
    )
    USING iceberg
""")

# ---------------------------------------------------------------------------
# Wait for bronze tables to exist
# ---------------------------------------------------------------------------
for tbl in (BRONZE_CUSTOMERS, BRONZE_DRIVERS):
    print(f"Waiting for {tbl} ...")
    while True:
        try:
            spark.table(tbl)
            print(f"  {tbl} found.")
            break
        except Exception:
            print(f"  {tbl} not ready, retrying in 10s ...")
            time.sleep(10)

# ---------------------------------------------------------------------------
# Core MERGE function
# ---------------------------------------------------------------------------
def merge_cdc(bronze_table, silver_table, pk, after_schema, silver_cols):
    """
    Deduplicate bronze → MERGE INTO silver.

    silver_cols: list of column names that belong in the silver target
                 (excludes 'op' and Kafka metadata).
    """
    print(f"\n--- {bronze_table} → {silver_table} ---")

    bronze_df = spark.table(bronze_table).filter(~F.col("is_tombstone"))
    total = bronze_df.count()
    print(f"  {total} non-tombstone rows in bronze")

    if total == 0:
        print("  Nothing to merge.")
        return

    # Keep only the latest event per PK
    w = Window.partitionBy(pk).orderBy(
        F.col("ts_ms").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )
    latest = (
        bronze_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # For non-delete events use after_json; for deletes use before_json
    # (we need the PK at minimum to issue the DELETE)
    row_json = F.when(F.col("op") != "d", F.col("after_json")).otherwise(F.col("before_json"))
    latest = latest.withColumn("_row", F.from_json(row_json, after_schema))

    # Build the source DataFrame with exactly the target schema + op
    source = latest.select(
        F.col("op"),
        F.col("ts_ms").alias("updated_ts"),
        *[F.col(f"_row.{c}").alias(c) for c in silver_cols if c != "updated_ts"],
    )

    n_deletes = source.filter(F.col("op") == "d").count()
    n_upserts = source.filter(F.col("op") != "d").count()
    print(f"  {n_upserts} upserts, {n_deletes} deletes after dedup")

    source.createOrReplaceTempView("_cdc_source")

    # Build explicit SET and VALUES clauses so UPDATE SET * and INSERT * work
    # correctly even when source has the extra 'op' column.
    set_clause = ", ".join(
        f"target.{c} = source.{c}"
        for c in ["updated_ts"] + [c for c in silver_cols if c != "updated_ts"]
    )
    ins_cols   = ", ".join(silver_cols)
    ins_vals   = ", ".join(f"source.{c}" for c in silver_cols)

    spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING _cdc_source AS source
        ON target.{pk} = source.{pk}
        WHEN MATCHED AND source.op = 'd'
            THEN DELETE
        WHEN MATCHED AND source.op IN ('u', 'c', 'r')
            THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED AND source.op != 'd'
            THEN INSERT ({ins_cols}) VALUES ({ins_vals})
    """)

    silver_count = spark.table(silver_table).count()
    print(f"  MERGE complete. {silver_table} now has {silver_count} rows.")


# ---------------------------------------------------------------------------
# Run both tables
# ---------------------------------------------------------------------------
merge_cdc(
    BRONZE_CUSTOMERS, SILVER_CUSTOMERS,
    pk="id",
    after_schema=CUSTOMERS_AFTER_SCHEMA,
    silver_cols=["id", "name", "email", "country", "updated_ts"],
)

merge_cdc(
    BRONZE_DRIVERS, SILVER_DRIVERS,
    pk="id",
    after_schema=DRIVERS_AFTER_SCHEMA,
    silver_cols=["id", "name", "license_number", "rating", "city", "active", "updated_ts"],
)

print("\nCDC silver MERGE complete.")
