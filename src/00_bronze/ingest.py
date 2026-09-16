# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest one source into bronze
# MAGIC
# MAGIC One generic notebook for every bronze table. The `source` parameter names an entry in
# MAGIC `config/sources.toml`. Each entry gets its own ingestion job (`ingest_<name>`, on its own schedule) that runs
# MAGIC this notebook. Adding a table means adding a config entry, not a notebook or a job.
# MAGIC
# MAGIC The source is copied **as is** into `<catalog>.<bronze_schema>.<target>`, plus ingestion metadata columns
# MAGIC (prefixed with `_` so they never collide with source columns):
# MAGIC
# MAGIC | column | meaning |
# MAGIC |---|---|
# MAGIC | `_batch_id` | one UUID per run, so every row can be traced back to the run that loaded it |
# MAGIC | `_ingested_at` | when the run wrote the row |
# MAGIC | `_source_table` | where the data came from |
# MAGIC | `_source_file` | the Delta data file the row was read from (Spark's hidden `_metadata` column) |
# MAGIC
# MAGIC The source's `mode` decides what a rerun does:
# MAGIC - **`append`**: adds a new batch and keeps every earlier one. Silver deduplicates (e.g. `trips`).
# MAGIC - **`overwrite`**: replaces the table with a full snapshot. This suits reference data re-delivered whole,
# MAGIC   and storage stays flat. A Delta overwrite is atomic, and the previous snapshot stays in the table history.

# COMMAND ----------

# Make src/medallion importable: a notebook runs with its own folder (src/<schema>/) as the working
# directory, so the shared package is one level up.
import os
import sys

sys.path.insert(0, os.path.abspath(".."))

# COMMAND ----------

# All parameters come from the job: ingest_<name> sets `source` to its own entry.
dbutils.widgets.text("source", "trips")
dbutils.widgets.text("catalog", "medallion")
dbutils.widgets.text("bronze_schema", "00_bronze")

from medallion.sources import get_source

source = get_source(dbutils.widgets.get("source"))
# Backticks: the schema name starts with a digit.
target_table = f"`{dbutils.widgets.get('catalog')}`.`{dbutils.widgets.get('bronze_schema')}`.{source.target}"
print(f"{source.name}: {source.table} -> {target_table} ({source.mode})")

# COMMAND ----------

import json
import uuid

from pyspark.sql import functions as F

batch_id = str(uuid.uuid4())

raw = spark.table(source.table)

bronze = raw.select(
    "*",
    F.lit(batch_id).alias("_batch_id"),
    F.current_timestamp().alias("_ingested_at"),
    F.lit(source.table).alias("_source_table"),
    F.col("_metadata.file_path").alias("_source_file"),
)

# COMMAND ----------

# saveAsTable creates the managed Delta table on the first run; afterwards `mode` decides append vs replace.
bronze.write.format("delta").mode(source.mode).saveAsTable(target_table)

spark.sql(f"COMMENT ON TABLE {target_table} IS 'Raw {source.table}, loaded in {source.mode} mode, one _batch_id per load'")

# COMMAND ----------

# Check what this run wrote, and return a summary as the run output.
written = spark.table(target_table).where(F.col("_batch_id") == batch_id).count()
source_rows = raw.count()
assert written == source_rows, f"batch {batch_id} wrote {written} rows, {source.table} has {source_rows}"

summary = {
    "source": source.name,
    "table": target_table.replace("`", ""),
    "mode": source.mode,
    "batch_id": batch_id,
    "rows_written": written,
    "total_rows": spark.table(target_table).count(),
}
print(summary)
dbutils.notebook.exit(json.dumps(summary))
