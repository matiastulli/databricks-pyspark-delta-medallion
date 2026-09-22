# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest files into bronze with Auto Loader
# MAGIC
# MAGIC The file counterpart of `ingest.py`. The `source` parameter names a `kind = "files"` entry in
# MAGIC `config/sources.toml`, and the job runs this notebook for it.
# MAGIC
# MAGIC - **Auto Loader** (`cloudFiles`) tracks which files it has already read, so a run costs only the new ones. No
# MAGIC   listing of the whole volume, and no "load everything again".
# MAGIC - **`trigger(availableNow=True)`** turns the stream into a batch job: take everything that has arrived, then
# MAGIC   stop. Same code as a live stream, run on a schedule.
# MAGIC - **The checkpoint is the bookmark**, kept in the volume next to the data. `offsets/` is written before a batch
# MAGIC   and `commits/` after, so a restart replays exactly the batch that died. **Deleting it is not a reset, it's a
# MAGIC   full reprocess**, which for this append-only table would duplicate every row.
# MAGIC - **The schema comes from the target table**, not from inference, so files that no longer match the DDL fail
# MAGIC   here instead of quietly changing the table. That also makes `cloudFiles.schemaLocation` unnecessary.
# MAGIC - **`maxFilesPerTrigger`** caps a batch, so a backlog after downtime becomes several normal batches instead of
# MAGIC   one that runs out of memory.

# COMMAND ----------

# Make src/medallion importable: a notebook runs with its own folder (src/<schema>/) as the working
# directory, so the shared package is one level up.
import os
import sys

sys.path.insert(0, os.path.abspath(".."))

# COMMAND ----------

dbutils.widgets.text("source", "landing_trips")
dbutils.widgets.text("catalog", "medallion")
dbutils.widgets.text("bronze_schema", "00_bronze")
dbutils.widgets.text("max_files_per_trigger", "50")

from medallion.sources import get_source

catalog, bronze_schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("bronze_schema")
source = get_source(dbutils.widgets.get("source"))
assert source.kind == "files", f"{source.name} is a {source.kind} source: use ingest.py"

landing = f"/Volumes/{catalog}/{bronze_schema}/{source.volume}/{source.path}"
checkpoint = f"/Volumes/{catalog}/{bronze_schema}/{source.volume}/_checkpoints/{source.name}"
target_table = f"`{catalog}`.`{bronze_schema}`.{source.name}"
print(f"{source.name}: {landing} -> {target_table} (checkpoint {checkpoint})")

# COMMAND ----------

import json
import uuid

from pyspark.sql import types as T

from medallion.bronze import add_ingestion_metadata
from medallion.contract import raise_if_schema_mismatch

# Tables are created only by DDL migrations (the apply_ddl job), never here.
if not spark.catalog.tableExists(target_table):
    raise RuntimeError(f"{target_table} doesn't exist: run `databricks bundle run apply_ddl` first")

batch_id = str(uuid.uuid4())
# The files hold the source columns; the _ columns are added here, so they are not part of the read schema.
file_schema = T.StructType([field for field in spark.table(target_table).schema.fields if not field.name.startswith("_")])

files = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", source.format)
    .option("cloudFiles.maxFilesPerTrigger", dbutils.widgets.get("max_files_per_trigger"))
    .schema(file_schema)
    .load(landing)
)
bronze = add_ingestion_metadata(files, batch_id, landing)
raise_if_schema_mismatch(bronze.dtypes, spark.table(target_table).dtypes, target_table)

# COMMAND ----------

before = spark.table(target_table).count()

query = (
    bronze.writeStream.option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(target_table)
)
query.awaitTermination()

# The stream reports what each micro-batch read; with availableNow there are as many as the backlog needed.
def batch_report(progress):
    # Spark Connect's progress doesn't carry numInputRows here, so rows come from the table count; what the source
    # reports is how much backlog is left, which is what maxFilesPerTrigger would spread over several batches.
    source_progress = (progress.get("sources") or [{}])[0]
    return {"batch": progress.get("batchId"), "files_outstanding": (source_progress.get("metrics") or {}).get("numFilesOutstanding")}

batches = [batch_report(progress) for progress in query.recentProgress]
after = spark.table(target_table).count()

summary = {
    "source": source.name,
    "table": target_table.replace("`", ""),
    "landing": landing,
    "checkpoint": checkpoint,
    "batch_id": batch_id,
    "rows_before": before,
    "rows_after": after,
    "rows_ingested": after - before,
    "micro_batches": batches,
    "checkpoint_dirs": sorted(f.name for f in dbutils.fs.ls(checkpoint)),
}
print(summary)
dbutils.notebook.exit(json.dumps(summary))
