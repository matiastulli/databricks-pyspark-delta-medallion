# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Gold: aggregates, checked before they're published
# MAGIC
# MAGIC Builds two tables in `<catalog>.<gold_schema>` from `<catalog>.<silver_schema>.trips`:
# MAGIC
# MAGIC | table | one row per | columns |
# MAGIC |---|---|---|
# MAGIC | `daily_trips` | pickup date | trips, revenue, average distance, fare and duration |
# MAGIC | `busiest_pickup_zones` | pickup ZIP code | rank by trips, trips, revenue, average fare |
# MAGIC
# MAGIC **Order matters: compute → check → write.** The data quality checks run on the new aggregates *before*
# MAGIC anything is written. If any check fails, the notebook raises, the run fails, and the gold tables keep
# MAGIC their last good version. All checks run before raising, so one failed run reports every problem.
# MAGIC
# MAGIC Gold is rebuilt in full every run (Delta overwrite). The aggregates are tiny, and a Delta overwrite is
# MAGIC atomic: readers see either the old table or the new one, never a half-written one.

# COMMAND ----------

dbutils.widgets.text("catalog", "medallion")
dbutils.widgets.text("silver_schema", "01_silver")
dbutils.widgets.text("gold_schema", "02_gold")

catalog = dbutils.widgets.get("catalog")
silver_table = f"`{catalog}`.`{dbutils.widgets.get('silver_schema')}`.trips"
gold_schema = f"`{catalog}`.`{dbutils.widgets.get('gold_schema')}`"
daily_trips_table = f"{gold_schema}.daily_trips"
busiest_pickup_zones_table = f"{gold_schema}.busiest_pickup_zones"

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

silver = spark.table(silver_table)

daily_trips = (
    silver.groupBy("pickup_date")
    .agg(
        F.count("*").alias("trips"),
        F.sum("fare_amount").alias("revenue"),
        F.round(F.avg("trip_distance_miles"), 2).alias("avg_distance_miles"),
        F.avg("fare_amount").cast("decimal(10,2)").alias("avg_fare"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
    )
    .orderBy("pickup_date")
)

zone_totals = silver.groupBy("pickup_zip").agg(
    F.count("*").alias("trips"),
    F.sum("fare_amount").alias("revenue"),
    F.avg("fare_amount").cast("decimal(10,2)").alias("avg_fare"),
)
# dense_rank: ZIPs with the same number of trips share a rank.
busiest_pickup_zones = (
    zone_totals.withColumn("rank", F.dense_rank().over(Window.orderBy(F.desc("trips"))))
    .select("rank", "pickup_zip", "trips", "revenue", "avg_fare")
    .orderBy("rank", "pickup_zip")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks
# MAGIC
# MAGIC Each check compares an actual value with the value it must have. The checks cover silver's contract
# MAGIC (gold relies on it), the shape of each gold table, and **reconciliation**: gold must add up to exactly
# MAGIC what silver holds, so no trips or revenue are lost or double-counted by the aggregation.

# COMMAND ----------

silver_stats = silver.agg(
    F.count("*").alias("rows"),
    F.countDistinct("trip_id").alias("distinct_trip_ids"),
    F.count_if(F.col("trip_id").isNull()).alias("null_trip_ids"),
    F.sum("fare_amount").alias("revenue"),
).first()

daily_stats = daily_trips.agg(
    F.count("*").alias("rows"),
    F.countDistinct("pickup_date").alias("distinct_dates"),
    F.sum("trips").alias("trips"),
    F.sum("revenue").alias("revenue"),
    F.count_if(F.col("revenue") < 0).alias("negative_revenue_days"),
).first()

zone_stats = busiest_pickup_zones.agg(
    F.count("*").alias("rows"),
    F.countDistinct("pickup_zip").alias("distinct_zips"),
    F.sum("trips").alias("trips"),
    F.sum("revenue").alias("revenue"),
    F.count_if(~F.col("pickup_zip").rlike("^[0-9]{5}$")).alias("malformed_zips"),
    F.min("rank").alias("top_rank"),
).first()

# (check name, actual, expected)
checks = [
    ("silver is not empty", silver_stats["rows"] > 0, True),
    ("silver trip_id is never null", silver_stats["null_trip_ids"], 0),
    ("silver trip_id is unique", silver_stats["distinct_trip_ids"], silver_stats["rows"]),
    ("daily_trips is not empty", daily_stats["rows"] > 0, True),
    ("daily_trips has one row per pickup_date", daily_stats["distinct_dates"], daily_stats["rows"]),
    ("daily_trips has no negative revenue", daily_stats["negative_revenue_days"], 0),
    ("daily_trips trips add up to silver rows", daily_stats["trips"], silver_stats["rows"]),
    ("daily_trips revenue adds up to silver revenue", daily_stats["revenue"], silver_stats["revenue"]),
    ("busiest_pickup_zones has one row per pickup_zip", zone_stats["distinct_zips"], zone_stats["rows"]),
    ("busiest_pickup_zones ZIPs are 5 digits", zone_stats["malformed_zips"], 0),
    ("busiest_pickup_zones ranking starts at 1", zone_stats["top_rank"], 1),
    ("busiest_pickup_zones trips add up to silver rows", zone_stats["trips"], silver_stats["rows"]),
    ("busiest_pickup_zones revenue adds up to silver revenue", zone_stats["revenue"], silver_stats["revenue"]),
]

results = [{"check": name, "passed": actual == expected, "actual": str(actual), "expected": str(expected)} for name, actual, expected in checks]
for result in results:
    print(("PASS " if result["passed"] else "FAIL ") + result["check"] + ("" if result["passed"] else f": got {result['actual']}, expected {result['expected']}"))


class DataQualityError(Exception):
    pass


failed = [result for result in results if not result["passed"]]
if failed:
    raise DataQualityError(
        f"{len(failed)} of {len(results)} data quality checks failed, gold was not written: "
        + "; ".join(f"{r['check']} (got {r['actual']}, expected {r['expected']})" for r in failed)
    )

# COMMAND ----------

# All checks passed: publish. overwriteSchema lets a changed aggregation replace the table's schema too.
for table_name, frame, comment in [
    (daily_trips_table, daily_trips, "NYC taxi trips per pickup date: trips, revenue and averages. Rebuilt from silver on every run"),
    (busiest_pickup_zones_table, busiest_pickup_zones, "NYC taxi pickup ZIP codes ranked by trips. Rebuilt from silver on every run"),
]:
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    spark.sql(f"COMMENT ON TABLE {table_name} IS '{comment}'")

# COMMAND ----------

import json

summary = {
    "silver_rows": silver_stats["rows"],
    "checks_passed": len(results),
    "daily_trips": {"table": daily_trips_table.replace("`", ""), "rows": spark.table(daily_trips_table).count()},
    "busiest_pickup_zones": {
        "table": busiest_pickup_zones_table.replace("`", ""),
        "rows": spark.table(busiest_pickup_zones_table).count(),
        "top_3": [row.asDict() for row in spark.table(busiest_pickup_zones_table).orderBy("rank").limit(3).select("rank", "pickup_zip", "trips").collect()],
    },
}
print(summary)
dbutils.notebook.exit(json.dumps(summary, default=str))
