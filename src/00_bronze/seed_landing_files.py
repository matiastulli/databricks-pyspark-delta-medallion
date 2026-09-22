# Databricks notebook source
# MAGIC %md
# MAGIC # Seed the landing volume
# MAGIC
# MAGIC Writes JSON files into `/Volumes/<catalog>/<bronze_schema>/landing/trips/`, one folder per pickup date, so there
# MAGIC is something for Auto Loader to pick up. In a real setup another system drops these files; here the notebook
# MAGIC stands in for it.
# MAGIC
# MAGIC Run it again with a larger `days` to make **new** files arrive: existing date folders are left alone
# MAGIC (`mode("ignore")`), so only the new dates are written, and Auto Loader then ingests exactly those.

# COMMAND ----------

dbutils.widgets.text("catalog", "medallion")
dbutils.widgets.text("bronze_schema", "00_bronze")
dbutils.widgets.text("days", "3")

catalog, bronze_schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("bronze_schema")
days = int(dbutils.widgets.get("days"))
landing = f"/Volumes/{catalog}/{bronze_schema}/landing/trips"

# COMMAND ----------

import json

from pyspark.sql import functions as F

source = spark.table("samples.nyctaxi.trips")
dates = [row.pickup_date for row in source.select(F.to_date("tpep_pickup_datetime").alias("pickup_date")).distinct().orderBy("pickup_date").limit(days).collect()]

written = []
for pickup_date in dates:
    folder = f"{landing}/pickup_date={pickup_date}"
    day = source.where(F.to_date("tpep_pickup_datetime") == F.lit(pickup_date))
    # ignore: a date already dropped stays as it is, so re-running only adds the dates that are new.
    day.coalesce(1).write.mode("ignore").json(folder)
    written.append({"date": str(pickup_date), "rows": day.count()})

files = [f.path for f in dbutils.fs.ls(landing)]
summary = {"landing": landing, "dates_requested": days, "written": written, "folders": len(files)}
print(summary)
dbutils.notebook.exit(json.dumps(summary))
