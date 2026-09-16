# Databricks notebook source
# MAGIC %md
# MAGIC # Clean trips (silver): clean, typed, one row per trip
# MAGIC
# MAGIC Reads `<catalog>.<bronze_schema>.nyctaxi_trips` and merges it into two tables in `<catalog>.<silver_schema>`:
# MAGIC `trips` for the clean trips and `trips_quarantine` for the rejected ones.
# MAGIC
# MAGIC 1. **Key.** The source has no trip ID, so `trip_id` is a SHA-256 hash of the six source columns.
# MAGIC    The same trip loaded in different bronze batches gets the same `trip_id`.
# MAGIC 2. **Deduplicate.** Keep one row per `trip_id`, the first one loaded (earliest `_ingested_at`).
# MAGIC 3. **Validate.** Flag trips that can't be real: a missing value, a distance or fare of 0 or less, or a
# MAGIC    dropoff that isn't after the pickup. Flagged trips go to **quarantine**, as received and with the rules they
# MAGIC    broke, so they can be inspected instead of silently disappearing.
# MAGIC 4. **Type and rename** the valid trips: clear column names, the fare as `DECIMAL(10,2)` (money
# MAGIC    shouldn't be a floating-point double), ZIP codes as 5-character strings (int `7002` is really
# MAGIC    `07002`), plus `pickup_date` and `trip_duration_minutes`.
# MAGIC 5. **`MERGE` on `trip_id`, insert-only**, into both tables. The key hashes every source column, so a
# MAGIC    matching `trip_id` means the trip is already there with identical content, and there is nothing
# MAGIC    to update. Only new trips are inserted, so running the notebook again changes nothing.
# MAGIC
# MAGIC Every run reads all of bronze. At ~22k trips per batch that's cheap, and it keeps the logic simple.

# COMMAND ----------

# Make src/medallion importable: a notebook runs with its own folder (src/<schema>/) as the working
# directory, so the shared package is one level up.
import os
import sys

sys.path.insert(0, os.path.abspath(".."))

# COMMAND ----------

dbutils.widgets.text("catalog", "medallion")
dbutils.widgets.text("bronze_schema", "00_bronze")
dbutils.widgets.text("silver_schema", "01_silver")

catalog = dbutils.widgets.get("catalog")
bronze_table = f"`{catalog}`.`{dbutils.widgets.get('bronze_schema')}`.nyctaxi_trips"
silver_schema = f"`{catalog}`.`{dbutils.widgets.get('silver_schema')}`"
silver_table = f"{silver_schema}.trips"
quarantine_table = f"{silver_schema}.trips_quarantine"

# COMMAND ----------

# MAGIC %md
# MAGIC Both tables are created and changed only by DDL migrations in `src/01_silver/ddl/` (the `apply_ddl` job). This
# MAGIC notebook only merges rows into them, and Delta rejects any row that doesn't match their declared schema.

# COMMAND ----------

# Tables are created only by DDL migrations (the apply_ddl job), never here.
for table in [silver_table, quarantine_table]:
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"{table} doesn't exist: run `databricks bundle run apply_ddl` first")

# COMMAND ----------

from pyspark.sql import functions as F

from medallion.silver import add_trip_id, keep_first_load, split_valid_and_rejected, to_quarantine, to_silver_trips

bronze = spark.table(bronze_table)

# Steps 1-4 live in src/medallion/silver.py, where they are unit-tested: key, deduplicate (first load wins),
# validate into valid / rejected, then type the valid trips and keep the rejected ones as received.
deduplicated = keep_first_load(add_trip_id(bronze))
valid, rejected = split_valid_and_rejected(deduplicated)
silver = to_silver_trips(valid)
quarantine = to_quarantine(rejected)

# COMMAND ----------

from delta.tables import DeltaTable

# 5. Insert-only MERGE: a matched trip_id is the same trip with the same content, so there is no update clause.
from medallion.contract import raise_if_schema_mismatch

def insert_new_trips(table_name, trips):
    """Merges trips into table_name on trip_id, inserting only unseen trips. Returns the rows inserted."""
    # What gets merged must match the DDL exactly (names and types), before anything is written.
    raise_if_schema_mismatch(trips.dtypes, spark.table(table_name).dtypes, table_name)
    (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(trips.alias("source"), "target.trip_id = source.trip_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
    # Delta records what each MERGE did in the table history.
    last_operation = DeltaTable.forName(spark, table_name).history(1).select("operation", "operationMetrics").first()
    assert last_operation["operation"] == "MERGE", f"expected the last operation on {table_name} to be MERGE, got {last_operation['operation']}"
    return int(last_operation["operationMetrics"]["numTargetRowsInserted"])

silver_inserted = insert_new_trips(silver_table, silver)
quarantine_inserted = insert_new_trips(quarantine_table, quarantine)

# COMMAND ----------

import json

# Every distinct trip must land in exactly one of the two tables, once.
silver_ids = spark.table(silver_table).select("trip_id")
quarantine_ids = spark.table(quarantine_table).select("trip_id")
silver_rows, quarantine_rows = silver_ids.count(), quarantine_ids.count()
distinct_trips = deduplicated.count()

assert silver_rows == silver_ids.distinct().count(), f"{silver_table} has duplicate trip_id rows"
assert quarantine_rows == quarantine_ids.distinct().count(), f"{quarantine_table} has duplicate trip_id rows"
assert silver_ids.intersect(quarantine_ids).isEmpty(), "some trip_id values are in both silver and quarantine"
assert silver_rows + quarantine_rows == distinct_trips, (
    f"silver ({silver_rows}) + quarantine ({quarantine_rows}) != distinct bronze trips ({distinct_trips})"
)

rejected_by_rule = (
    spark.table(quarantine_table)
    .select(F.explode("rejection_reasons").alias("rule"))
    .groupBy("rule").count()
    .collect()
)

summary = {
    "bronze_rows": bronze.count(),
    "distinct_trips": distinct_trips,
    "silver": {"table": silver_table.replace("`", ""), "rows_inserted": silver_inserted, "rows": silver_rows},
    "quarantine": {
        "table": quarantine_table.replace("`", ""),
        "rows_inserted": quarantine_inserted,
        "rows": quarantine_rows,
        "rejected_by_rule": {row["rule"]: row["count"] for row in rejected_by_rule},
    },
}
print(summary)
dbutils.notebook.exit(json.dumps(summary))
