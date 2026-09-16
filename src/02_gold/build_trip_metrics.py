# Databricks notebook source
# MAGIC %md
# MAGIC # Build trip metrics (gold): aggregates, checked before they're published
# MAGIC
# MAGIC Builds two tables in `<catalog>.<gold_schema>` from `<catalog>.<silver_schema>.trips`:
# MAGIC
# MAGIC | table | one row per | columns |
# MAGIC |---|---|---|
# MAGIC | `agg_trips_daily` | pickup date | trips, revenue, average distance, fare and duration |
# MAGIC | `agg_trips_by_pickup_zip` | pickup ZIP code | rank by trips, trips, revenue, average fare |
# MAGIC
# MAGIC **Order matters: compute → check → write.** The data quality checks run on the new aggregates *before*
# MAGIC anything is written. If any check fails, the notebook raises, the run fails, and the gold tables keep
# MAGIC their last good version. All checks run before raising, so one failed run reports every problem.
# MAGIC
# MAGIC Gold is rebuilt in full every run (Delta overwrite). The aggregates are tiny, and a Delta overwrite is
# MAGIC atomic: readers see either the old table or the new one, never a half-written one. The tables themselves are
# MAGIC created and changed only by DDL migrations in `src/02_gold/ddl/` (the `apply_ddl` job).

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

catalog = dbutils.widgets.get("catalog")
silver_table = f"`{catalog}`.`{dbutils.widgets.get('silver_schema')}`.trips"
gold_schema = f"`{catalog}`.`{dbutils.widgets.get('gold_schema')}`"
daily_trips_table = f"{gold_schema}.agg_trips_daily"
busiest_pickup_zones_table = f"{gold_schema}.agg_trips_by_pickup_zip"

# COMMAND ----------

from medallion import gold

silver = spark.table(silver_table)

# The aggregates live in src/medallion/gold.py, where they are unit-tested.
daily_trips = gold.daily_trips(silver)
busiest_pickup_zones = gold.busiest_pickup_zones(silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks
# MAGIC
# MAGIC Each check compares an actual value with the value it must have. The checks cover silver's contract
# MAGIC (gold relies on it), the shape of each gold table, and **reconciliation**: gold must add up to exactly
# MAGIC what silver holds, so no trips or revenue are lost or double-counted by the aggregation.

# COMMAND ----------

from medallion.quality import gold_checks, raise_if_any_failed

# The checks live in src/medallion/quality.py, where they are unit-tested.
results = gold_checks(silver, daily_trips, busiest_pickup_zones)
for result in results:
    print(("PASS " if result["passed"] else "FAIL ") + result["check"] + ("" if result["passed"] else f": got {result['actual']}, expected {result['expected']}"))

raise_if_any_failed(results)

# COMMAND ----------

# Tables are created only by DDL migrations (the apply_ddl job), never here.
for table in [daily_trips_table, busiest_pickup_zones_table]:
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"{table} doesn't exist: run `databricks bundle run apply_ddl` first")

from pyspark.sql import functions as F

from medallion.contract import raise_if_schema_mismatch

# What gets written must match the DDL exactly (names and types): Delta alone would accept a missing nullable column
# (every gold column is nullable) or a castable type. Checked for both tables before either is written.
for table_name, frame in [(daily_trips_table, daily_trips), (busiest_pickup_zones_table, busiest_pickup_zones)]:
    raise_if_schema_mismatch(frame.dtypes, spark.table(table_name).dtypes, table_name)

# All checks passed: publish. writeTo matches columns by name and never changes the table's schema, so an aggregation
# that no longer matches the DDL fails here instead of silently redefining the table; the fix is a new migration.
for table_name, frame in [(daily_trips_table, daily_trips), (busiest_pickup_zones_table, busiest_pickup_zones)]:
    frame.writeTo(table_name).overwrite(F.lit(True))  # replace every row in one atomic commit

# COMMAND ----------

import json

summary = {
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
