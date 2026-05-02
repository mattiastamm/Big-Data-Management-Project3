"""
streaming/gold.py — Batch job triggered by Airflow.
Recomputes the top-5 pickup zones by trip count from all silver data
and overwrites the gold Iceberg table.

Idempotent: CREATE OR REPLACE gives the same result for the same source data.
"""

import time
from pyspark.sql import SparkSession

SILVER_TABLE   = "lakehouse.taxi.silver"
GOLD_TABLE     = "lakehouse.taxi.gold"

print("Building SparkSession ...")
spark = SparkSession.builder.appName("gold_taxi").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("SparkSession ready.")

# ---------------------------------------------------------------------------
# Wait for silver table to exist
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
# Recompute gold — top-5 pickup zones by trip count
# ---------------------------------------------------------------------------
print(f"Recomputing {GOLD_TABLE} from {SILVER_TABLE} ...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_TABLE}
    USING iceberg
    PARTITIONED BY (pickup_zone)
    AS
    SELECT pickup_zone, count(*) AS trip_count
    FROM   {SILVER_TABLE}
    GROUP BY pickup_zone
    ORDER BY trip_count DESC
    LIMIT 5
""")

print("Gold table updated:")
spark.sql(f"SELECT * FROM {GOLD_TABLE}").show(truncate=False)
print("Gold job complete.")
