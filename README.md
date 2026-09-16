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
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt   # the bundle generates jobs with this venv
databricks bundle deploy                  # deploys every workflow (dev target: schedules and triggers paused)
databricks bundle run ingest_trips        # one ingestion workflow per source in config/sources.toml
databricks bundle run clean_trips         # silver
databricks bundle run build_trip_metrics  # gold scorecard
```

Unit tests run locally on PySpark (Python 3.11 and Java 17), and in [GitHub Actions](.github/workflows/ci.yml) on every push:

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
.venv/bin/pytest
```

On Databricks Free Edition, `databricks catalogs create` fails because the metastore has no storage root. The script creates the catalog with SQL on the serverless warehouse instead, which puts it on Default Storage.

## Workflows

Every process is its own workflow (Databricks job), so each one has its own cadence, retries and failures:

| Workflow | Schema | Runs | What it does |
|---|---|---|---|
| `ingest_<source>`, one per entry in [`config/sources.toml`](config/sources.toml) | `00_bronze` | its own schedule: daily, twice a month, monthly… | [`ingest.py`](src/00_bronze/ingest.py) copies the source as is, plus `_batch_id`, `_ingested_at`, `_source_table` and `_source_file`. `append` sources add a batch per run (`trips`); `overwrite` sources replace a full snapshot (7 TPC-H tables). |
| `clean_trips` | `01_silver` | when `00_bronze.trips` changes (table update trigger) | [`clean_trips.py`](src/01_silver/clean_trips.py): one row per trip in `trips`, keyed by a hash of the source columns, with clean types. Impossible trips go to `trips_quarantine` with the rules they broke. Insert-only `MERGE`s make reruns change nothing. |
| `build_trip_metrics` | `02_gold` | when `01_silver.trips` changes (table update trigger) | [`build_trip_metrics.py`](src/02_gold/build_trip_metrics.py): the scorecard, `daily_trips` and `busiest_pickup_zones`. 13 data quality checks, including reconciliation with silver, run before anything is written. Any failure fails the run and leaves gold at its last good version. |

Ingestion jobs are **generated** from the config ([`resources/__init__.py`](resources/__init__.py)), so adding a bronze table means adding a config entry: no notebook, no job YAML. Silver and gold workflows react to their input tables instead of guessing when bronze finished. A trigger only fires on real data changes, so a silver run that inserts nothing doesn't rebuild gold.

## Project layout

```
databricks.yml                     Asset Bundle: variables, dev target, Python resource loader
config/sources.toml                bronze sources: table, target, load mode, schedule
resources/
  __init__.py                      generates one ingest_<source> job per config entry
  clean_trips_job.yml              silver workflow (table update trigger)
  build_trip_metrics_job.yml       gold scorecard workflow (table update trigger)
src/
  00_bronze/ingest.py              processes, in one folder per schema they write to
  01_silver/clean_trips.py
  02_gold/build_trip_metrics.py
  medallion/                       shared logic: sources config, trip key, dedup, validation, quality checks
tests/                             pytest for src/medallion and config/sources.toml
scripts/                           one-off workspace setup (Unity Catalog)
docs/PLAN.md                       the learning path and its decisions
```

## Related

- [dbt-terraform-postgres-medallion](https://github.com/matiastulli/dbt-terraform-postgres-medallion): the same medallion idea with dbt, Terraform and PostgreSQL.
