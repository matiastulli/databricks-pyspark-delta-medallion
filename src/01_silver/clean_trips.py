# Databricks notebook source
# MAGIC %md
# MAGIC # Clean trips (silver): clean, typed, one row per trip
# MAGIC
# MAGIC Reads `<catalog>.<bronze_schema>.trips` and merges it into two tables in `<catalog>.<silver_schema>`:
# MAGIC `trips` for the clean trips and `trips_quarantine` for the rejected ones.
# MAGIC
# MAGIC 1. **Key.** The source has no trip ID, so `trip_id` is a SHA-256 hash of the six source columns.
# MAGIC    The same trip loaded in different bronze batches gets the same `trip_id`.
# MAGIC 2. **Deduplicate.** Keep one row per `trip_id`, the first one loaded (earliest `_ingested_at`).
# MAGIC 3. **Validate.** Flag trips that can't be real: a distance or fare of 0 or less, or a dropoff that
# MAGIC    isn't after the pickup. Flagged trips go to **quarantine**, as received and with the rules they
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
bronze_table = f"`{catalog}`.`{dbutils.widgets.get('bronze_schema')}`.trips"
silver_schema = f"`{catalog}`.`{dbutils.widgets.get('silver_schema')}`"
silver_table = f"{silver_schema}.trips"
quarantine_table = f"{silver_schema}.trips_quarantine"

# COMMAND ----------

# MAGIC %md
# MAGIC The silver schema is a contract for the layers downstream, so both tables are created with explicit
# MAGIC types and comments instead of being inferred from the first write. The quarantine table keeps the
# MAGIC bronze column names and types, because it holds rows that failed before any typing happened.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_table} (
  trip_id               STRING        NOT NULL COMMENT 'SHA-256 of the six source columns',
  pickup_at             TIMESTAMP     NOT NULL,
  dropoff_at            TIMESTAMP     NOT NULL,
  pickup_date           DATE          NOT NULL,
  trip_duration_minutes DOUBLE        NOT NULL,
  trip_distance_miles   DOUBLE        NOT NULL,
  fare_amount           DECIMAL(10,2) NOT NULL,
  pickup_zip            STRING        NOT NULL COMMENT '5-character ZIP code, zero-padded',
  dropoff_zip           STRING        NOT NULL COMMENT '5-character ZIP code, zero-padded',
  _batch_id             STRING        NOT NULL COMMENT 'Bronze batch the trip was first loaded in',
  _ingested_at          TIMESTAMP     NOT NULL COMMENT 'When that bronze batch was loaded',
  _merged_at            TIMESTAMP     NOT NULL COMMENT 'When silver inserted the row'
)
COMMENT 'NYC taxi trips: deduplicated, cleaned and typed, one row per trip_id'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {quarantine_table} (
  trip_id               STRING        NOT NULL COMMENT 'SHA-256 of the six source columns',
  rejection_reasons     ARRAY<STRING> NOT NULL COMMENT 'Names of the validation rules the trip broke',
  tpep_pickup_datetime  TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  trip_distance         DOUBLE,
  fare_amount           DOUBLE,
  pickup_zip            INT,
  dropoff_zip           INT,
  _batch_id             STRING        NOT NULL COMMENT 'Bronze batch the trip was first loaded in',
  _ingested_at          TIMESTAMP     NOT NULL COMMENT 'When that bronze batch was loaded',
  _merged_at            TIMESTAMP     NOT NULL COMMENT 'When silver quarantined the row'
)
COMMENT 'NYC taxi trips rejected by silver validation, as received from bronze, one row per trip_id'
""")

# COMMAND ----------

from pyspark.sql import functions as F

from medallion.silver import SOURCE_COLUMNS, add_trip_id, keep_first_load, split_valid_and_rejected

bronze = spark.table(bronze_table)

# 1. Key, 2. deduplicate (first load wins), 3. validate into valid / rejected.
# The logic lives in src/medallion/silver.py, where it is unit-tested.
deduplicated = keep_first_load(add_trip_id(bronze))
valid, rejected = split_valid_and_rejected(deduplicated)

# 4. Type and rename the valid trips to match the silver table.
silver = valid.select(
    "trip_id",
    F.col("tpep_pickup_datetime").alias("pickup_at"),
    F.col("tpep_dropoff_datetime").alias("dropoff_at"),
    F.to_date("tpep_pickup_datetime").alias("pickup_date"),
    F.round((F.unix_seconds("tpep_dropoff_datetime") - F.unix_seconds("tpep_pickup_datetime")) / 60, 2).alias("trip_duration_minutes"),
    F.col("trip_distance").alias("trip_distance_miles"),
    F.col("fare_amount").cast("decimal(10,2)").alias("fare_amount"),
    F.lpad(F.col("pickup_zip").cast("string"), 5, "0").alias("pickup_zip"),
    F.lpad(F.col("dropoff_zip").cast("string"), 5, "0").alias("dropoff_zip"),
    "_batch_id",
    "_ingested_at",
    F.current_timestamp().alias("_merged_at"),
)

# Rejected trips keep their bronze columns untouched.
quarantine = rejected.select(
    "trip_id", "rejection_reasons", *SOURCE_COLUMNS, "_batch_id", "_ingested_at", F.current_timestamp().alias("_merged_at")
)

# COMMAND ----------

from delta.tables import DeltaTable

# 5. Insert-only MERGE: a matched trip_id is the same trip with the same content, so there is no update clause.
def insert_new_trips(table_name, trips):
    """Merges trips into table_name on trip_id, inserting only unseen trips. Returns the rows inserted."""
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
