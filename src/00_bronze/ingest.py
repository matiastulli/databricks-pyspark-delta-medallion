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
dbutils.widgets.text("source", "nyctaxi_trips")
dbutils.widgets.text("catalog", "medallion")
dbutils.widgets.text("bronze_schema", "00_bronze")

from medallion.sources import get_source

source = get_source(dbutils.widgets.get("source"))
# Backticks: the schema name starts with a digit.
target_table = f"`{dbutils.widgets.get('catalog')}`.`{dbutils.widgets.get('bronze_schema')}`.{source.name}"
print(f"{source.name}: {source.table} -> {target_table} ({source.mode})")

# COMMAND ----------

import json
import uuid

from pyspark.sql import functions as F

from medallion.bronze import add_ingestion_metadata

batch_id = str(uuid.uuid4())

raw = spark.table(source.table)
# Metadata columns are added in src/medallion/bronze.py, where they are unit-tested.
bronze = add_ingestion_metadata(raw, batch_id, source.table)

# COMMAND ----------

# Tables are created only by DDL migrations (the apply_ddl job), never here.
for table in [target_table]:
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"{table} doesn't exist: run `databricks bundle run apply_ddl` first")

from medallion.contract import raise_if_schema_mismatch

# What gets written must match the DDL exactly (names and types): Delta alone would accept a missing nullable column
# or a castable type. writeTo never changes the table's schema; a source that changed needs a new migration.
raise_if_schema_mismatch(bronze.dtypes, spark.table(target_table).dtypes, target_table)
if source.mode == "append":
    bronze.writeTo(target_table).append()
else:
    bronze.writeTo(target_table).overwrite(F.lit(True))  # replace every row: a full snapshot, in one atomic commit

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
