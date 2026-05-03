"""
gold_demand_patterns.py — Joins silver_taxi with silver CDC customers to
produce a demand-pattern gold table keyed by (country, hour_of_day).

Join strategy
-------------
NYC taxi trips have no customer_id or country column. We derive country by
mapping each pickup_zone deterministically to a country from the live CDC
country list:

    country_rank = abs(crc32(pickup_zone)) % N_distinct_countries

where N is the number of distinct countries currently in silver.customers.
No customer IDs are involved — only the distinct country list from CDC.
silver_taxi already carries zone name strings (silver.py joins zone lookup),
so taxi_zone_lookup is not needed here.

When a country is added or removed from the CDC customers table, the mapping
shifts on the next run, satisfying the "country update propagates" requirement.

Output schema  (lakehouse.gold.demand_patterns)
---------------
country          STRING   — from silver.customers (distinct countries)
hour             INT      — 0–23 pickup hour
trip_count       BIGINT   — total trips for this country+hour
avg_fare         DOUBLE   — average fare_amount
top_pickup_zone  STRING   — mode pickup zone (most frequent)
weekday_trips    BIGINT
weekend_trips    BIGINT
is_peak_hour     BOOLEAN  — TRUE for the single peak hour per country
"""

import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

SILVER_CUSTOMERS = "lakehouse.silver.customers"
SILVER_TAXI      = "lakehouse.taxi.silver"
GOLD_TABLE       = "lakehouse.gold.demand_patterns"

print("Building SparkSession ...")
spark = SparkSession.builder.appName("gold_demand_patterns").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("SparkSession ready.")

spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.gold")

# ---------------------------------------------------------------------------
# Wait for both silver tables
# ---------------------------------------------------------------------------
for tbl in (SILVER_CUSTOMERS, SILVER_TAXI):
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
# Get distinct countries from CDC silver (no customer IDs needed)
# ---------------------------------------------------------------------------
trips = spark.table(SILVER_TAXI)

countries = (
    spark.table(SILVER_CUSTOMERS)
    .select("country")
    .distinct()
    .orderBy("country")
    .withColumn("rn", F.row_number().over(Window.orderBy("country")) - 1)  # 0-based
)
n_countries = countries.count()
print(f"Distinct CDC countries: {n_countries}, Silver taxi trips: {trips.count()}")

if n_countries == 0:
    print("No countries in silver — skipping gold computation.")
    raise SystemExit(0)

# ---------------------------------------------------------------------------
# Map each trip's pickup_zone → country via stable hash
# silver_taxi already has zone name strings — no zone lookup needed
# ---------------------------------------------------------------------------
trips_with_country = (
    trips
    .withColumn("country_rn", F.abs(F.crc32(F.col("pickup_zone"))) % n_countries)
    .join(F.broadcast(countries.select(F.col("rn").alias("c_rn"), "country")),
          F.col("country_rn") == F.col("c_rn"),
          "left")
    .withColumn("country", F.coalesce(F.col("country"), F.lit("Unknown")))
    .drop("country_rn", "c_rn")
)

# ---------------------------------------------------------------------------
# Derive hour and day-type
# ---------------------------------------------------------------------------
trips_enriched = (
    trips_with_country
    .withColumn("hour",     F.hour("tpep_pickup_datetime"))
    .withColumn("day_type", F.when(
        F.dayofweek("tpep_pickup_datetime").isin(1, 7), "weekend"
    ).otherwise("weekday"))
)

# ---------------------------------------------------------------------------
# Aggregate: trip_count, avg_fare, weekday/weekend counts
# ---------------------------------------------------------------------------
agg = trips_enriched.groupBy("country", "hour").agg(
    F.count("*").alias("trip_count"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.sum(F.when(F.col("day_type") == "weekday", 1).otherwise(0)).alias("weekday_trips"),
    F.sum(F.when(F.col("day_type") == "weekend", 1).otherwise(0)).alias("weekend_trips"),
)

# ---------------------------------------------------------------------------
# Most common pickup zone per (country, hour)
# ---------------------------------------------------------------------------
zone_counts = trips_enriched.groupBy("country", "hour", "pickup_zone").agg(
    F.count("*").alias("zone_count")
)
top_zone_win = Window.partitionBy("country", "hour").orderBy(F.col("zone_count").desc())
top_zones = (
    zone_counts
    .withColumn("_rn", F.row_number().over(top_zone_win))
    .filter("_rn = 1")
    .select("country", "hour", F.col("pickup_zone").alias("top_pickup_zone"))
)

# ---------------------------------------------------------------------------
# Join everything, add is_peak_hour flag
# ---------------------------------------------------------------------------
result = agg.join(top_zones, ["country", "hour"], "left")

peak_win = Window.partitionBy("country").orderBy(F.col("trip_count").desc())
result = result.withColumn(
    "is_peak_hour",
    F.row_number().over(peak_win) == 1
)

# ---------------------------------------------------------------------------
# Write gold table (full recompute — idempotent)
# ---------------------------------------------------------------------------
print(f"Writing {GOLD_TABLE} ...")
result.writeTo(GOLD_TABLE).using("iceberg").createOrReplace()

row_count = spark.table(GOLD_TABLE).count()
print(f"{GOLD_TABLE} written: {row_count} rows")

# ---------------------------------------------------------------------------
# Answer the two required queries
# ---------------------------------------------------------------------------
print("\n--- Q1: Which country travels most during late-night hours (22:00–04:00)? ---")
spark.sql(f"""
    SELECT country,
           SUM(trip_count) AS late_night_trips
    FROM   {GOLD_TABLE}
    WHERE  hour >= 22 OR hour <= 4
    GROUP BY country
    ORDER BY late_night_trips DESC
    LIMIT 5
""").show(truncate=False)

print("\n--- Q2: Peak hour for top-spending country vs overall peak hour ---")
spark.sql(f"""
    WITH country_spend AS (
        SELECT country,
               SUM(avg_fare * trip_count) / SUM(trip_count) AS weighted_avg_fare
        FROM   {GOLD_TABLE}
        GROUP BY country
        ORDER BY weighted_avg_fare DESC
        LIMIT 1
    ),
    top_country_peak AS (
        SELECT g.country, g.hour AS peak_hour, g.trip_count
        FROM   {GOLD_TABLE} g
        JOIN   country_spend cs ON g.country = cs.country
        WHERE  g.is_peak_hour = TRUE
    ),
    overall_peak AS (
        SELECT hour AS overall_peak_hour, SUM(trip_count) AS total_trips
        FROM   {GOLD_TABLE}
        GROUP BY hour
        ORDER BY total_trips DESC
        LIMIT 1
    )
    SELECT t.country,
           t.peak_hour        AS top_spender_peak_hour,
           t.trip_count       AS top_spender_peak_trips,
           o.overall_peak_hour,
           o.total_trips      AS overall_peak_trips
    FROM   top_country_peak t
    CROSS JOIN overall_peak o
""").show(truncate=False)

print("\nGold demand patterns complete.")
