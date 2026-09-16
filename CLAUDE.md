# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

A **learning-path portfolio project**: a medallion lakehouse on Databricks built with **PySpark + Delta Lake only**. No dbt and no Terraform: the user chose those tools for a separate repo (`dbt-terraform-postgres-medallion`). The user is a software engineer with Databricks experience from a company-specific framework, and is refreshing the fundamentals in a public repo.

**Build it one step at a time.** Never scaffold later steps ahead of time; each step adds only what it needs, with explanations. The user prefers easy-going example data over business rules.

Learning path (tick them off in `README.md` as they land, and update `docs/PLAN.md` with status, decisions and learnings; plan changes to later steps go into `docs/PLAN.md` before the code):
0. Local environment: PySpark + Delta on the laptop, done first because the Databricks site was down during setup
1. Setup: Databricks Free Edition, Databricks CLI auth, Unity Catalog catalog + `00_bronze`/`01_silver`/`02_gold` schemas
2. Bronze: PySpark ingestion of `samples.nyctaxi.trips` into Delta, with ingestion metadata
3. Silver: cleaning/typing, idempotent Delta `MERGE`
4. Gold: aggregates (daily trips/revenue, busiest zones) + data quality checks that fail the run
5. Orchestration: Databricks Asset Bundle (`databricks.yml`) with a bronze → silver → gold job
6. Tests + CI: transformations as pure functions in `src/`, pytest on local PySpark, GitHub Actions

## Environment

- Databricks **Free Edition**: serverless compute only, with usage limits. If a step doesn't fit, adapt the step rather than suggest a paid plan.
- Databricks CLI (Homebrew). Authenticate with `databricks auth login` (browser OAuth). **Never ask for or handle personal access tokens in chat.** The CLI uses the `DEFAULT` profile in `~/.databrickscfg`. Keep the workspace host out of committed files.
- Workspace-specific settings (`DATABRICKS_HOST`, `DATABRICKS_CONFIG_PROFILE`, `DATABRICKS_WAREHOUSE_ID`) and the catalog/schema names live in `.env`, which is git-ignored. `.env.example` is the committed template. Add new variables to both files, and only when code uses them. Never put tokens in `.env`, because auth is OAuth.
- Unity Catalog: catalog `medallion` with schemas `00_bronze`, `01_silver`, `02_gold`. The user chose these names so the schemas sort in pipeline order. Unquoted names like `medallion.00_bronze.trips` parse fine (checked on the warehouse), and the setup script quotes them with backticks anyway. Free Edition's metastore has no storage root, so `databricks catalogs create` fails. Create catalogs with SQL on the serverless warehouse, which uses Default Storage. The only warehouse is the "Serverless Starter Warehouse". Stop it after ad-hoc SQL to save quota.
- Local: Python 3.11 and Java 17 (Homebrew `openjdk@17`). Versions are pinned in `requirements.txt` (PySpark 4.2.0 + delta-spark 4.4.0) and were verified together locally. They match serverless compute, which was checked in step 2 and runs Spark 4.2.0 on Python 3.11. Re-check if the serverless environment changes.

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

A local Delta-enabled session needs `configure_spark_with_delta_pip(builder)` plus the two Delta configs: `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` and `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`. The first session downloads the Delta jars from Maven into `~/.ivy2.5.2`, which takes about a minute. pip lists the package as `delta_spark`.

Databricks commands:

```sh
cp .env.example .env                                         # once, then fill in DATABRICKS_HOST (never overwrite an existing .env)
set -a; source .env; set +a                                  # load workspace settings into the shell
databricks auth login --host "$DATABRICKS_HOST"              # browser OAuth, saves the DEFAULT profile
databricks current-user me                                   # check the auth works
scripts/setup_unity_catalog.sh                               # idempotent, reads .env: catalog + 00_bronze/01_silver/02_gold schemas
databricks schemas list "$CATALOG"
databricks warehouses stop "$DATABRICKS_WAREHOUSE_ID" --no-wait   # stop after ad-hoc SQL to save quota
scripts/run_notebook.sh 00_bronze                            # upload notebooks/00_bronze.py and run it once on serverless, then print its exit JSON
scripts/run_notebook.sh 01_silver                            # rerunning must report rows_inserted: 0 for silver and quarantine
```

Notebooks live in `notebooks/` as Databricks source files (`# Databricks notebook source`, `# COMMAND ----------` between cells). They read the catalog and schema names from widgets, which `run_notebook.sh` fills in from `.env`. `run_notebook.sh` uses `databricks jobs submit` (a one-time run with no saved job) as a stopgap until the Asset Bundle in step 5.

Source data facts (`samples.nyctaxi.trips`): 21,932 rows, Jan–Feb 2016, and no nulls. The columns are `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `trip_distance`, `fare_amount`, `pickup_zip`, `dropoff_zip`. There is **no trip ID**, and zones are ZIP codes. Bronze (`medallion.00_bronze.trips`) is append-only, so every run adds a full batch under a new `_batch_id`. Silver (`medallion.01_silver.trips`) keys trips by `trip_id` = SHA-256 of the six source columns, with timestamps as `unix_micros` so the key doesn't depend on the time zone. Its `MERGE` is insert-only, because a matching key means identical content. Rejected trips go to `medallion.01_silver.trips_quarantine`, keeping their bronze columns and adding `rejection_reasons`. Every distinct trip lands in exactly one of the two tables, and the notebook asserts that.

## Working agreements

- The repo is **public**. Confirm with the user before pushing, and keep secrets and workspace-specific IDs out of committed files.
- The user's GitHub profile README (`~/Code/matiastulli`) lists this project. Update its entry as the project progresses, and pull before editing because the user also edits it on the web.
- Add commands to this file as each step introduces them. Don't document commands that don't exist yet.
