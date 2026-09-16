# databricks-pyspark-delta-medallion

A medallion lakehouse on **Databricks**, built with **PySpark** and **Delta Lake**, orchestrated with **Databricks Asset Bundles** and tested with **pytest**.

> 🚧 **Built step by step as a learning path.** Each step lands in its own commits, so the history shows how the lakehouse grows. The steps, their status and the decisions behind them are in [docs/PLAN.md](docs/PLAN.md).

The data is the [NYC taxi trips](https://docs.databricks.com/aws/en/discover/databricks-datasets) sample that ships with every Databricks workspace (`samples.nyctaxi.trips`). It's simple, well-known data, so the focus stays on the engineering rather than on business rules.

## Getting started

```sh
cp .env.example .env             # then set DATABRICKS_HOST to your workspace URL
set -a; source .env; set +a
databricks auth login --host "$DATABRICKS_HOST"
scripts/setup_unity_catalog.sh   # creates the medallion catalog and its 00_bronze / 01_silver / 02_gold schemas
scripts/run_notebook.sh 00_bronze  # uploads the notebook and runs it once on serverless
scripts/run_notebook.sh 01_silver
scripts/run_notebook.sh 02_gold
```

On Databricks Free Edition, `databricks catalogs create` fails because the metastore has no storage root. The script creates the catalog with SQL on the serverless warehouse instead, which puts it on Default Storage.

## Layers

| Layer | Notebook | Table | What it does |
|---|---|---|---|
| Bronze | [`notebooks/00_bronze.py`](notebooks/00_bronze.py) | `medallion.00_bronze.trips` | Appends the raw trips as is, plus `_batch_id`, `_ingested_at`, `_source_table` and `_source_file`. Each run is a new batch, and duplicates are left for silver to remove. |
| Silver | [`notebooks/01_silver.py`](notebooks/01_silver.py) | `medallion.01_silver.trips`<br>`medallion.01_silver.trips_quarantine` | One row per trip. `trip_id` is a hash of the source columns. Valid trips get clean types, with fares as `DECIMAL` and ZIPs as zero-padded strings. Impossible trips go to quarantine as received, with the rules they broke. Insert-only Delta `MERGE`s make reruns change nothing. |
| Gold | [`notebooks/02_gold.py`](notebooks/02_gold.py) | `medallion.02_gold.daily_trips`<br>`medallion.02_gold.busiest_pickup_zones` | Trips, revenue and averages per pickup date, and pickup ZIPs ranked by trips. 13 data quality checks run before anything is written, including gold reconciling exactly with silver. Any failure fails the run and leaves gold at its last good version. |

## Related

- [dbt-terraform-postgres-medallion](https://github.com/matiastulli/dbt-terraform-postgres-medallion): the same medallion idea with dbt, Terraform and PostgreSQL.
