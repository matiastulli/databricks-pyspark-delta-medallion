# Databricks notebook source
# MAGIC %md
# MAGIC # Build trip metrics (gold): aggregates, checked before they're published
# MAGIC
# MAGIC Builds two tables in `<catalog>.<gold_schema>` from `<catalog>.<silver_schema>.trips`:
# MAGIC
# MAGIC | table | one row per | how it's built |
# MAGIC |---|---|---|
# MAGIC | `agg_trips_daily` | pickup date | **incrementally**: silver's change feed says which dates moved, and only those are recomputed and merged |
# MAGIC | `agg_trips_by_pickup_zip` | pickup ZIP code | **rebuilt in full**: it's a ranking, and a rank depends on every row, so one changed trip can shift many rows |
# MAGIC
# MAGIC **Incremental reading.** `delta.enableChangeDataFeed` makes silver record its row-level inserts, updates and
# MAGIC deletes per version. This job keeps the last version it consumed in `ops.processed_versions` and reads only what
# MAGIC came after. A deleted row counts as much as an inserted one: its date has to be recomputed either way.
# MAGIC
# MAGIC Two cases fall back to a full rebuild: no watermark yet (the feed only records versions after it was enabled),
# MAGIC and an unchanged silver means there is nothing to do at all.
# MAGIC
# MAGIC **Checks and rollback.** A full rebuild still runs the checks *before* writing. An incremental merge can't: the
# MAGIC result only exists once it's merged. So gold's version is noted first, the merge runs, the checks run, and if
# MAGIC any fails the table is put back with `RESTORE TABLE … TO VERSION AS OF`, then the run fails. Either way a bad
# MAGIC aggregate never stays published.
# MAGIC
# MAGIC The tables themselves are created and changed only by DDL migrations in `src/02_gold/ddl/` (the `apply_ddl` job).

# COMMAND ----------

# Make src/medallion importable: a notebook runs with its own folder (src/<schema>/) as the working
# directory, so the shared package is one level up.
import os
import sys

sys.path.insert(0, os.path.abspath(".."))

# COMMAND ----------

dbutils.widgets.text("catalog", "medallion")
dbutils.widgets.text("silver_schema", "01_silver")
dbutils.widgets.text("gold_schema", "02_gold")
dbutils.widgets.dropdown("full_rebuild", "false", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
silver_table = f"`{catalog}`.`{dbutils.widgets.get('silver_schema')}`.trips"
gold_schema = f"`{catalog}`.`{dbutils.widgets.get('gold_schema')}`"
daily_trips_table = f"{gold_schema}.agg_trips_daily"
busiest_pickup_zones_table = f"{gold_schema}.agg_trips_by_pickup_zip"
watermarks_table = f"`{catalog}`.ops.processed_versions"
PROCESS = "build_trip_metrics"

# Tables are created only by DDL migrations (the apply_ddl job), never here.
for table in [daily_trips_table, busiest_pickup_zones_table, watermarks_table]:
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"{table} doesn't exist: run `databricks bundle run apply_ddl` first")

# COMMAND ----------

from pyspark.sql import functions as F

from medallion import gold
from medallion.contract import raise_if_schema_mismatch
from medallion.incremental import changed_keys, plan_run
from medallion.quality import gold_checks, raise_if_any_failed

silver = spark.table(silver_table)

def table_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table}").selectExpr("max(version)").first()[0]

watermark = (
    spark.table(watermarks_table)
    .where((F.col("process") == PROCESS) & (F.col("source_table") == silver_table.replace("`", "")))
    .selectExpr("max(last_version)")
    .first()[0]
)
silver_version = table_version(silver_table)
plan = plan_run(None if dbutils.widgets.get("full_rebuild") == "true" else watermark, silver_version)
print(f"silver is at version {silver_version}, last processed {watermark} -> {plan['mode']}")

# COMMAND ----------

import json

if plan["mode"] == "none":
    summary = {"mode": "none", "silver_version": silver_version, "message": "silver hasn't changed since the last run"}
    print(summary)
    dbutils.notebook.exit(json.dumps(summary, default=str))

# COMMAND ----------

# The ranking is always rebuilt in full: one changed trip can move many ZIP codes up or down.
busiest_pickup_zones = gold.busiest_pickup_zones(silver)

if plan["mode"] == "incremental":
    changes = (
        spark.read.option("readChangeFeed", "true")
        .option("startingVersion", plan["start_version"])
        .option("endingVersion", plan["end_version"])
        .table(silver_table)
    )
    dates = changed_keys(changes, "pickup_date")
    print(f"versions {plan['start_version']}–{plan['end_version']} touched {len(dates)} date(s): {dates[:5]}{' …' if len(dates) > 5 else ''}")
    daily_trips = gold.daily_trips(silver.where(F.col("pickup_date").isin(dates)))
else:
    dates = None
    daily_trips = gold.daily_trips(silver)

# What gets written must match the DDL exactly (names and types): Delta alone would accept a missing nullable column
# (every gold column is nullable) or a castable type.
for table_name, frame in [(daily_trips_table, daily_trips), (busiest_pickup_zones_table, busiest_pickup_zones)]:
    raise_if_schema_mismatch(frame.dtypes, spark.table(table_name).dtypes, table_name)

# COMMAND ----------

from delta.tables import DeltaTable

gold_version_before = table_version(daily_trips_table)

if plan["mode"] == "incremental":
    # Merge only the dates that moved. A date whose trips were all deleted has no row left in the recomputed set, so
    # it is deleted from gold too; otherwise the aggregate would keep counting trips that are gone.
    recomputed = daily_trips.alias("source")
    (
        DeltaTable.forName(spark, daily_trips_table)
        .alias("target")
        .merge(recomputed, "target.pickup_date = source.pickup_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    gone = [d for d in dates if daily_trips.where(F.col("pickup_date") == F.lit(d)).isEmpty()]
    if gone:
        spark.sql(f"DELETE FROM {daily_trips_table} WHERE pickup_date IN ({', '.join(repr(str(d)) for d in gone)})")
        print(f"removed {len(gone)} date(s) with no trips left")
else:
    daily_trips.writeTo(daily_trips_table).overwrite(F.lit(True))

busiest_pickup_zones.writeTo(busiest_pickup_zones_table).overwrite(F.lit(True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks
# MAGIC
# MAGIC Each check compares an actual value with the value it must have: silver's contract (gold relies on it), the
# MAGIC shape of each gold table, and **reconciliation**, gold adding up to exactly what silver holds. Reconciliation is
# MAGIC what catches an incremental run that drifted: a date it should have rebuilt and didn't shows up as a mismatch.
# MAGIC
# MAGIC The tables are already written at this point, so a failure restores `agg_trips_daily` to the version from before
# MAGIC this run and then fails the run.

# COMMAND ----------

results = gold_checks(silver, spark.table(daily_trips_table), spark.table(busiest_pickup_zones_table))
for result in results:
    print(("PASS " if result["passed"] else "FAIL ") + result["check"] + ("" if result["passed"] else f": got {result['actual']}, expected {result['expected']}"))

if any(not result["passed"] for result in results):
    spark.sql(f"RESTORE TABLE {daily_trips_table} TO VERSION AS OF {gold_version_before}")
    print(f"restored {daily_trips_table} to version {gold_version_before}")
raise_if_any_failed(results)

# COMMAND ----------

# Only now, with the checks passed, does the watermark move: a failed run reprocesses the same versions next time.
(
    spark.createDataFrame([(PROCESS, silver_table.replace("`", ""), silver_version)], "process string, source_table string, last_version bigint")
    .withColumn("updated_at", F.current_timestamp())
    .writeTo(watermarks_table)
    .append()
)

summary = {
    "mode": plan["mode"],
    "silver_version": silver_version,
    "previous_watermark": watermark,
    "dates_rebuilt": len(dates) if dates is not None else "all",
    "silver_rows": silver.count(),
    "checks_passed": len(results),
    "agg_trips_daily": {"table": daily_trips_table.replace("`", ""), "rows": spark.table(daily_trips_table).count()},
    "agg_trips_by_pickup_zip": {
        "table": busiest_pickup_zones_table.replace("`", ""),
        "rows": spark.table(busiest_pickup_zones_table).count(),
        "top_3": [row.asDict() for row in spark.table(busiest_pickup_zones_table).orderBy("rank").limit(3).select("rank", "pickup_zip", "trips").collect()],
    },
}
print(summary)
dbutils.notebook.exit(json.dumps(summary, default=str))
