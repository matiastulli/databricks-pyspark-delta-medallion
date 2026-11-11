# Databricks notebook source
# MAGIC %md
# MAGIC # Monitor pipeline
# MAGIC
# MAGIC Checks the pipeline's health from its data, records every result in `ops.check_results`, and **fails the run if
# MAGIC any check fails**, so the failure email every job has (the `add_alerting` mutator in `resources/__init__.py`) is
# MAGIC the alert.
# MAGIC
# MAGIC | Check | Fails when |
# MAGIC |---|---|
# MAGIC | freshness | a bronze table's newest `_ingested_at` is older than its source's `max_age_hours` in `config/sources.toml` (sources without it aren't checked) |
# MAGIC | quarantine rate | quarantined trips are more than `max_quarantine_rate` of all distinct trips |
# MAGIC
# MAGIC In dev the schedule is paused like every other, and freshness fails for any source not run by hand recently, so
# MAGIC run it on demand there.
# MAGIC
# MAGIC Job failures and durations aren't checked here: `system.lakeflow` already records them (`ops.job_runs`). The
# MAGIC pass/fail logic lives in `src/medallion/observability.py`, where it is unit-tested.

# COMMAND ----------

# Make src/medallion importable: a notebook runs with its own folder (src/ops/_monitoring/) as the working
# directory, so the shared package is two levels up.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

# COMMAND ----------

dbutils.widgets.text("catalog", "medallion")
dbutils.widgets.text("bronze_schema", "00_bronze")
dbutils.widgets.text("silver_schema", "01_silver")
dbutils.widgets.text("max_quarantine_rate", "0.01")
dbutils.widgets.text("run_id", "")

catalog = dbutils.widgets.get("catalog")
bronze_schema = f"`{catalog}`.`{dbutils.widgets.get('bronze_schema')}`"
silver_schema = f"`{catalog}`.`{dbutils.widgets.get('silver_schema')}`"
results_table = f"`{catalog}`.ops.check_results"
JOB = "monitor_pipeline"

if not spark.catalog.tableExists(results_table):
    raise RuntimeError(f"{results_table} doesn't exist: run `databricks bundle run apply_ddl` first")

# COMMAND ----------

from medallion.observability import freshness_check, quarantine_rate_check
from medallion.sources import load_sources

results = []

# Freshness. The newest row and "now" come from one query, so they share a clock and a time zone.
for source in load_sources():
    if source.max_age_hours is None:
        continue
    table = f"{bronze_schema}.{source.name}"
    row = spark.sql(f"SELECT max(_ingested_at) AS latest, current_timestamp() AS now FROM {table}").first()
    results.append(freshness_check(source.name, row["latest"], row["now"], source.max_age_hours))

# Quarantine rate: silver and quarantine hold one row per distinct trip, and every trip is in exactly one of them.
# Two counts, not one snapshot: a clean_trips run in between could move the rate by a few rows, which a 1% bound absorbs.
valid = spark.table(f"{silver_schema}.trips").count()
quarantined = spark.table(f"{silver_schema}.trips_quarantine").count()
results.append(quarantine_rate_check("trips_quarantine", valid, quarantined, float(dbutils.widgets.get("max_quarantine_rate"))))

for result in results:
    print(("PASS " if result["passed"] else "FAIL ") + f"{result['check']}: {result['actual']} (expected {result['expected']})")

# COMMAND ----------

import json

from pyspark.sql import functions as F

from medallion.contract import raise_if_schema_mismatch
from medallion.observability import CHECK_RESULT_SCHEMA, check_result_rows
from medallion.quality import raise_if_any_failed

from delta.tables import DeltaTable

# Record first, then fail: the results of a failing run are the ones worth keeping. Insert-only MERGE on
# (job, run_id, check_name), because serverless retries a failed task and the retry must not record the same checks
# twice. A null run_id (an interactive run) never matches, so it always records.
rows = spark.createDataFrame(check_result_rows(results, JOB, dbutils.widgets.get("run_id")), CHECK_RESULT_SCHEMA).withColumn("checked_at", F.current_timestamp())
raise_if_schema_mismatch(rows.dtypes, spark.table(results_table).dtypes, results_table)
(
    DeltaTable.forName(spark, results_table).alias("target")
    .merge(rows.alias("source"), "target.job = source.job AND target.run_id = source.run_id AND target.check_name = source.check_name")
    .whenNotMatchedInsertAll()
    .execute()
)

summary = {"checks": len(results), "failed": [r["check"] for r in results if not r["passed"]]}
print(summary)
raise_if_any_failed(results)
dbutils.notebook.exit(json.dumps(summary))
