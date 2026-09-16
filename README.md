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
databricks bundle deploy          # deploys the pipeline job (dev target)
databricks bundle run medallion   # runs bronze → silver → gold on serverless
```

Unit tests run locally on PySpark (Python 3.11 and Java 17), and in [GitHub Actions](.github/workflows/ci.yml) on every push:

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
.venv/bin/pytest
```

On Databricks Free Edition, `databricks catalogs create` fails because the metastore has no storage root. The script creates the catalog with SQL on the serverless warehouse instead, which puts it on Default Storage.

## Layers

| Layer | Notebook | Table | What it does |
|---|---|---|---|
| Bronze | [`src/00_bronze.py`](src/00_bronze.py) | `medallion.00_bronze.trips` | Appends the raw trips as is, plus `_batch_id`, `_ingested_at`, `_source_table` and `_source_file`. Each run is a new batch, and duplicates are left for silver to remove. |
| Silver | [`src/01_silver.py`](src/01_silver.py) | `medallion.01_silver.trips`<br>`medallion.01_silver.trips_quarantine` | One row per trip. `trip_id` is a hash of the source columns. Valid trips get clean types, with fares as `DECIMAL` and ZIPs as zero-padded strings. Impossible trips go to quarantine as received, with the rules they broke. Insert-only Delta `MERGE`s make reruns change nothing. |
| Gold | [`src/02_gold.py`](src/02_gold.py) | `medallion.02_gold.daily_trips`<br>`medallion.02_gold.busiest_pickup_zones` | Trips, revenue and averages per pickup date, and pickup ZIPs ranked by trips. 13 data quality checks run before anything is written, including gold reconciling exactly with silver. Any failure fails the run and leaves gold at its last good version. |

## Project layout

```
databricks.yml               Asset Bundle: variables and the dev target
resources/medallion_job.yml  the job: bronze → silver → gold notebook tasks on serverless
src/
  00_bronze.py … 02_gold.py  notebooks: read tables, call the logic, write tables
  medallion/                 logic the notebooks import (trip key, dedup, validation, quality checks)
tests/                       pytest for src/medallion
scripts/                     one-off workspace setup (Unity Catalog)
docs/PLAN.md                 the learning path and its decisions
```

## Related

- [dbt-terraform-postgres-medallion](https://github.com/matiastulli/dbt-terraform-postgres-medallion): the same medallion idea with dbt, Terraform and PostgreSQL.
