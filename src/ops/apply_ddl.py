# Databricks notebook source
# MAGIC %md
# MAGIC # Apply DDL migrations
# MAGIC
# MAGIC Tables are created and changed only here, never by the jobs that write to them. This notebook applies the
# MAGIC write-once SQL files in `src/<NN_layer>/ddl/` that haven't run yet and records each one in
# MAGIC `<catalog>.ops.schema_migrations`. Every table has its own version sequence (`silver_trips_v001_create.sql`,
# MAGIC `silver_trips_v002_alter.sql`, …).
# MAGIC
# MAGIC **Run order:** `schemas` first, then layer folders (00, 01, 02), tables by name, each table's versions ascending,
# MAGIC and a renamed table always after the table it renames.
# MAGIC
# MAGIC - **Reruns do nothing:** applied versions are skipped.
# MAGIC - **Applied migrations are write-once:** if one was edited, renamed or deleted since it ran, the run fails before
# MAGIC   executing anything. A change is always a new migration.
# MAGIC - **`dry_run = true`** lists what would be applied without executing it.
# MAGIC
# MAGIC What runs (file rules, ordering, drift detection, statement splitting, placeholders) is decided in
# MAGIC `src/medallion/migrations.py`, where it is unit-tested. This notebook only executes and records.

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
dbutils.widgets.text("gold_schema", "02_gold")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"])

from medallion.migrations import HISTORY_SCHEMA, HISTORY_TABLE, PLACEHOLDERS, load_migrations, pending_migrations, render, split_statements

values = {name: dbutils.widgets.get(name) for name in PLACEHOLDERS}
dry_run = dbutils.widgets.get("dry_run") == "true"
catalog = values["catalog"]
history_table = f"`{catalog}`.`{HISTORY_SCHEMA}`.`{HISTORY_TABLE}`"

# COMMAND ----------

# The history table is the runner's own bookkeeping, so the runner creates it (like Flyway's history table):
# it has to exist before any migration can be recorded.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{HISTORY_SCHEMA}` COMMENT 'Pipeline bookkeeping owned by tooling'")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {history_table} (
  migration  STRING    NOT NULL COMMENT 'What the migration versions: schemas or <layer>_<table>',
  version    INT       NOT NULL COMMENT 'Version within that table, v<NNN>',
  file       STRING    NOT NULL COMMENT 'Migration file, relative to src/',
  checksum   STRING    NOT NULL COMMENT 'SHA-256 of the file as applied',
  applied_at TIMESTAMP NOT NULL
)
COMMENT 'DDL migrations applied from src/<NN_layer>/ddl by src/ops/apply_ddl.py'
""")

migrations = load_migrations()
applied = {(row.migration, row.version): (row.file, row.checksum) for row in spark.table(history_table).collect()}
pending = pending_migrations(migrations, applied)
print(f"{len(migrations)} migrations, {len(applied)} already applied, {len(pending)} pending" + (" (dry run)" if dry_run else ""))

# COMMAND ----------

import json

from pyspark.sql import functions as F

for migration in pending:
    statements = split_statements(render(migration.sql, values))
    print(f"{migration.path}: {len(statements)} statement(s)")
    if dry_run:
        continue
    for statement in statements:
        spark.sql(statement)
    # Recorded only after every statement in the file succeeded.
    (
        spark.createDataFrame([(migration.key, migration.version, migration.path, migration.checksum)], "migration string, version int, file string, checksum string")
        .withColumn("applied_at", F.current_timestamp())
        .writeTo(history_table)
        .append()
    )

summary = {"catalog": catalog, "dry_run": dry_run, "already_applied": len(applied), "applied_now": [] if dry_run else [m.path for m in pending], "pending": [m.path for m in pending] if dry_run else []}
print(summary)
dbutils.notebook.exit(json.dumps(summary))
