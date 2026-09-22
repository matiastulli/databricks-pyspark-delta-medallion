# Databricks notebook source
# MAGIC %md
# MAGIC # Maintain tables
# MAGIC
# MAGIC Delta maintenance for every published table, in one weekly workflow:
# MAGIC
# MAGIC | Step | Why |
# MAGIC |---|---|
# MAGIC | `OPTIMIZE` | Compacts small files, and it is **the only operation that re-clusters**. Auto compaction only glues files together, so a heavily merged table can have healthy file sizes and still skip badly. `optimize_full = true` re-clusters a table's existing data, which is what a new `CLUSTER BY` needs once. |
# MAGIC | `REORG TABLE … APPLY (PURGE)` | Materializes deletion vectors. Deletes are cheap to write (a bitmap next to the file) but every read pays until the files are rewritten. Off by default: it rewrites data. |
# MAGIC | `VACUUM` | Deletes files no longer referenced, older than the retention (7 days by default). **Dry run by default**, because `VACUUM` shortens time travel and can break a stream restarting after downtime. |
# MAGIC
# MAGIC **What Databricks already does here:** these tables are Unity Catalog managed tables, and predictive optimization
# MAGIC already runs `OPTIMIZE` on them (`00_bronze.nyctaxi_trips` held 241k rows in one file without us asking). So this
# MAGIC job is mostly a demonstration on this workspace, and measures what it did rather than claiming a fix.
# MAGIC
# MAGIC Which tables to maintain, and the before/after comparison, live in `src/medallion/maintenance.py`, where they
# MAGIC are unit-tested.

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
dbutils.widgets.dropdown("optimize_full", "false", ["true", "false"])
dbutils.widgets.dropdown("purge_deletion_vectors", "false", ["true", "false"])
dbutils.widgets.dropdown("vacuum", "dry_run", ["dry_run", "run", "skip"])

from medallion.maintenance import describe_change, tables_to_maintain

catalog = dbutils.widgets.get("catalog")
schemas = [dbutils.widgets.get(name) for name in ("bronze_schema", "silver_schema", "gold_schema")]
optimize_full = dbutils.widgets.get("optimize_full") == "true"
purge = dbutils.widgets.get("purge_deletion_vectors") == "true"
vacuum_mode = dbutils.widgets.get("vacuum")

# COMMAND ----------

def detail(table):
    """DESCRIBE DETAIL as a dict. It can't be used as a subquery, unlike DESCRIBE HISTORY."""
    return spark.sql(f"DESCRIBE DETAIL {table}").first().asDict()

tables = tables_to_maintain({schema: [t.name for t in spark.catalog.listTables(f"`{catalog}`.`{schema}`")] for schema in schemas})
print(f"{len(tables)} tables to maintain: optimize_full={optimize_full}, purge={purge}, vacuum={vacuum_mode}")

# COMMAND ----------

import json

report = {}
for name in tables:
    schema, table = name.split(".")
    qualified = f"`{catalog}`.`{schema}`.{table}"
    before = detail(qualified)

    spark.sql(f"OPTIMIZE {qualified}" + (" FULL" if optimize_full else ""))
    if purge:
        spark.sql(f"REORG TABLE {qualified} APPLY (PURGE)")

    after = detail(qualified)
    entry = describe_change(before, after)

    if vacuum_mode != "skip":
        # DRY RUN lists the files it would delete; the count is what we report.
        vacuum = spark.sql(f"VACUUM {qualified}" + (" DRY RUN" if vacuum_mode == "dry_run" else ""))
        entry["vacuum"] = {"mode": vacuum_mode, "files": vacuum.count() if vacuum_mode == "dry_run" else "deleted"}

    report[name] = entry
    print(f"  {name}: files {entry['files'][0]} -> {entry['files'][1]}, {entry['size_mb'][1]} MB, clustering {entry['clustering'] or 'none'}")

summary = {"catalog": catalog, "tables": len(report), "optimize_full": optimize_full, "purge": purge, "vacuum": vacuum_mode, "report": report}
print(summary)
dbutils.notebook.exit(json.dumps(summary))
