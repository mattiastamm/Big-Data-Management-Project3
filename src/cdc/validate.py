"""
cdc/validate.py — Validates that the silver CDC tables mirror the PostgreSQL
source by comparing row counts and spot-checking a sample of IDs.

Exit code 0 = PASS, 1 = FAIL (makes Airflow mark the task failed).
"""

import os
import sys
import psycopg2
from pyspark.sql import SparkSession

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB   = os.environ.get("PG_DB",   "sourcedb")
PG_USER = os.environ.get("PG_USER", "cdc_user")
PG_PASS = os.environ.get("PG_PASSWORD", "cdc_pass")

print("Building SparkSession ...")
spark = SparkSession.builder.appName("cdc_validate").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("SparkSession ready.")

# ---------------------------------------------------------------------------
# Query PostgreSQL
# ---------------------------------------------------------------------------
conn = psycopg2.connect(
    host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM customers;")
pg_customers = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM drivers;")
pg_drivers = cur.fetchone()[0]

cur.execute("SELECT id FROM customers ORDER BY id;")
pg_customer_ids = {row[0] for row in cur.fetchall()}

cur.execute("SELECT id FROM drivers ORDER BY id;")
pg_driver_ids = {row[0] for row in cur.fetchall()}

conn.close()

# ---------------------------------------------------------------------------
# Query Iceberg silver
# ---------------------------------------------------------------------------
silver_customers = spark.table("lakehouse.silver.customers").count()
silver_drivers   = spark.table("lakehouse.silver.drivers").count()

silver_customer_ids = {
    row.id
    for row in spark.table("lakehouse.silver.customers").select("id").collect()
}
silver_driver_ids = {
    row.id
    for row in spark.table("lakehouse.silver.drivers").select("id").collect()
}

# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------
print("\n=== CDC Validation ===")
print(f"customers : PostgreSQL={pg_customers:4d}  Silver={silver_customers:4d}")
print(f"drivers   : PostgreSQL={pg_drivers:4d}  Silver={silver_drivers:4d}")

ok = True

if pg_customers != silver_customers:
    print(f"  MISMATCH customers count: {pg_customers} vs {silver_customers}")
    ok = False

if pg_drivers != silver_drivers:
    print(f"  MISMATCH drivers count: {pg_drivers} vs {silver_drivers}")
    ok = False

# Check that every PG id exists in silver (IDs present in PG but missing from silver)
missing_customers = pg_customer_ids - silver_customer_ids
missing_drivers   = pg_driver_ids - silver_driver_ids

if missing_customers:
    print(f"  MISMATCH customers IDs in PG but not silver: {sorted(missing_customers)}")
    ok = False

if missing_drivers:
    print(f"  MISMATCH driver IDs in PG but not silver: {sorted(missing_drivers)}")
    ok = False

if ok:
    print("PASS: Silver mirrors the PostgreSQL source.")
    sys.exit(0)
else:
    print("FAIL: Silver does not match the PostgreSQL source.")
    sys.exit(1)
