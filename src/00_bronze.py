# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Bronze: ingest raw trips
# MAGIC
# MAGIC Copies `samples.nyctaxi.trips` into `<catalog>.<bronze_schema>.trips` as a Delta table, **as is**,
# MAGIC plus ingestion metadata columns (prefixed with `_` so they never collide with source columns):
# MAGIC
# MAGIC | column | meaning |
# MAGIC |---|---|
# MAGIC | `_batch_id` | one UUID per run, so every row can be traced back to the run that loaded it |
# MAGIC | `_ingested_at` | when the run wrote the row |
# MAGIC | `_source_table` | where the data came from |
# MAGIC | `_source_file` | the Delta data file the row was read from (Spark's hidden `_metadata` column) |
# MAGIC
# MAGIC Bronze is **append-only**: each run adds a new batch and never updates or deletes. The source has no
# MAGIC trip ID, so re-running loads the same trips again under a new `_batch_id`. That's intended. Bronze
# MAGIC keeps the full load history, and silver (step 3) deduplicates.

# COMMAND ----------

# Job parameters (set by scripts/run_notebook.sh from .env); the defaults make interactive runs work too.
dbutils.widgets.text("catalog", "medallion")
dbutils.widgets.text("bronze_schema", "00_bronze")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")

source_table = "samples.nyctaxi.trips"
# Backticks: the schema name starts with a digit.
target_table = f"`{catalog}`.`{bronze_schema}`.trips"

# COMMAND ----------

import json
import uuid

from pyspark.sql import functions as F

batch_id = str(uuid.uuid4())

raw = spark.table(source_table)

bronze = raw.select(
    "*",
    F.lit(batch_id).alias("_batch_id"),
    F.current_timestamp().alias("_ingested_at"),
    F.lit(source_table).alias("_source_table"),
    F.col("_metadata.file_path").alias("_source_file"),
)

# COMMAND ----------

# saveAsTable creates the managed Delta table on the first run and appends to it afterwards.
bronze.write.format("delta").mode("append").saveAsTable(target_table)

spark.sql(f"COMMENT ON TABLE {target_table} IS 'Raw NYC taxi trips from {source_table}, append-only, one _batch_id per load'")

# COMMAND ----------

# Check what this run wrote, and return a summary as the run output (`databricks jobs get-run-output`).
written = spark.table(target_table).where(F.col("_batch_id") == batch_id).count()
source_rows = raw.count()
assert written == source_rows, f"batch {batch_id} wrote {written} rows, source has {source_rows}"

summary = {
    "table": target_table.replace("`", ""),
    "batch_id": batch_id,
    "rows_written": written,
    "total_rows": spark.table(target_table).count(),
    "batches": spark.table(target_table).select("_batch_id").distinct().count(),
}
print(summary)
dbutils.notebook.exit(json.dumps(summary))
