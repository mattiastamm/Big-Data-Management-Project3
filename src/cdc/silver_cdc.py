"""
cdc/silver_cdc.py — Reads ALL rows from each bronze CDC table, deduplicates
to the latest event per primary key, then applies DELETE + MERGE (upsert-only)
into the silver Iceberg table so it mirrors the current PostgreSQL state.

Two-phase strategy (matches the working pattern from Untitled.ipynb):
  1. Build a temp view `cdc_latest` containing one flat row per PK
     (latest by ts_ms, then kafka_offset). Columns are extracted with
     get_json_object so the source plan is purely SQL — no nested struct
     access, no DataFrame lineage carrying an Iceberg scan into MERGE.
  2. DELETE FROM silver WHERE id IN (cdc_latest WHERE op='d')
  3. MERGE INTO silver USING (cdc_latest WHERE op IN ('u','c','r')) — UPSERT only.

Splitting DELETE out avoids the Spark 4.1 + Iceberg 1.10.1 MERGE planner bug
("No plan for TableReference") that fires when one MERGE has both DELETE and
UPDATE/INSERT branches sourced from another Iceberg table.

Idempotency:
  - cdc_latest is deterministic (window over ts_ms, kafka_offset).
  - DELETE + MERGE upsert give the same target state for the same source.
"""

import time
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BRONZE_CUSTOMERS = "lakehouse.bronze.customers_cdc"
BRONZE_DRIVERS   = "lakehouse.bronze.drivers_cdc"
SILVER_CUSTOMERS = "lakehouse.silver.customers"
SILVER_DRIVERS   = "lakehouse.silver.drivers"

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
# Field specs per table — (silver_col, json_cast_type)
# Each entry tells us how to pull a column out of the after/before JSON blob.
# ---------------------------------------------------------------------------
CUSTOMER_FIELDS = [
    ("name",    "string"),
    ("email",   "string"),
    ("country", "string"),
]

DRIVER_FIELDS = [
    ("name",            "string"),
    ("license_number",  "string"),
    ("rating",          "double"),
    ("city",            "string"),
    ("active",          "boolean"),
]


def _build_field_select(fields, json_col):
    """Build SQL fragments to extract typed fields from a JSON column.

    Uses try_cast so that any malformed values (e.g. Debezium-encoded
    base64 NUMERIC bytes from before decimal.handling.mode was set to
    'double') become NULL rather than failing the whole job.
    """
    parts = []
    for col, cast in fields:
        parts.append(
            f"try_cast(get_json_object({json_col}, '$.{col}') AS {cast}) AS after_{col}"
        )
    return ",\n            ".join(parts)


def merge_cdc(bronze_table, silver_table, pk, fields):
    """
    Build cdc_latest temp view, then DELETE + MERGE upsert into silver.
    fields: list of (column_name, cast_type) for non-PK columns.
    """
    print(f"\n--- {bronze_table} → {silver_table} ---")

    bronze = spark.table(bronze_table).filter("NOT is_tombstone")
    total = bronze.count()
    print(f"  {total} non-tombstone rows in bronze")
    if total == 0:
        print("  Nothing to merge.")
        return

    bronze.createOrReplaceTempView("_bronze_src")

    after_extracts = _build_field_select(fields, "after_json")

    # Flat dedup view: one row per PK, latest event wins.
    # Keep only the columns we need — no struct access, no Iceberg lineage
    # bleeding into the MERGE plan.
    dedup_sql = f"""
        CREATE OR REPLACE TEMP VIEW cdc_latest AS
        SELECT * FROM (
            SELECT
                op,
                CAST(COALESCE(
                    get_json_object(after_json,  '$.{pk}'),
                    get_json_object(before_json, '$.{pk}')
                ) AS INT) AS {pk},
                {after_extracts},
                ts_ms AS updated_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(COALESCE(
                        get_json_object(after_json,  '$.{pk}'),
                        get_json_object(before_json, '$.{pk}')
                    ) AS INT)
                    ORDER BY ts_ms DESC, kafka_offset DESC
                ) AS rn
            FROM _bronze_src
            WHERE op IS NOT NULL
        )
        WHERE rn = 1
    """
    spark.sql(dedup_sql)

    n_deletes = spark.sql("SELECT COUNT(*) AS c FROM cdc_latest WHERE op = 'd'").collect()[0]["c"]
    n_upserts = spark.sql("SELECT COUNT(*) AS c FROM cdc_latest WHERE op IN ('u','c','r')").collect()[0]["c"]
    print(f"  {n_upserts} upserts, {n_deletes} deletes after dedup")

    # ---- Phase 1: DELETE ----
    if n_deletes > 0:
        spark.sql(f"""
            DELETE FROM {silver_table}
            WHERE {pk} IN (SELECT {pk} FROM cdc_latest WHERE op = 'd')
        """)
        print(f"  Deleted up to {n_deletes} rows from {silver_table}.")

    # ---- Phase 2: MERGE upsert-only ----
    if n_upserts > 0:
        set_parts = ", ".join(
            [f"{c} = source.after_{c}" for c, _ in fields]
            + ["updated_ts = source.updated_ts"]
        )
        ins_cols = ", ".join([pk] + [c for c, _ in fields] + ["updated_ts"])
        ins_vals = ", ".join(
            [f"source.{pk}"]
            + [f"source.after_{c}" for c, _ in fields]
            + ["source.updated_ts"]
        )

        merge_sql = f"""
            MERGE INTO {silver_table} AS target
            USING (SELECT * FROM cdc_latest WHERE op IN ('u','c','r')) AS source
            ON target.{pk} = source.{pk}
            WHEN MATCHED THEN UPDATE SET {set_parts}
            WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
        """
        spark.sql(merge_sql)
        print(f"  MERGE upsert applied.")

    silver_count = spark.table(silver_table).count()
    print(f"  {silver_table} now has {silver_count} rows.")


# ---------------------------------------------------------------------------
# Run both tables
# ---------------------------------------------------------------------------
merge_cdc(BRONZE_CUSTOMERS, SILVER_CUSTOMERS, pk="id", fields=CUSTOMER_FIELDS)
merge_cdc(BRONZE_DRIVERS,   SILVER_DRIVERS,   pk="id", fields=DRIVER_FIELDS)

print("\nCDC silver MERGE complete.")
