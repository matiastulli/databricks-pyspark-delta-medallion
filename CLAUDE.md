# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

A **learning-path portfolio project**: a medallion lakehouse on Databricks built with **PySpark + Delta Lake only**. No dbt and no Terraform: the user chose those tools for a separate repo (`dbt-terraform-postgres-medallion`). The user is a software engineer with Databricks experience from a company-specific framework, and is refreshing the fundamentals in a public repo.

**Build it one step at a time.** Never scaffold later steps ahead of time; each step adds only what it needs, with explanations. The user prefers easy-going example data over business rules.

Learning path. `docs/PLAN.md` is the only place that tracks it: update its status, decisions and learnings as steps land, and put plan changes for later steps there before writing the code. The README describes what exists (setup, layers), not the plan:
0. Local environment: PySpark + Delta on the laptop, done first because the Databricks site was down during setup
1. Setup: Databricks Free Edition, Databricks CLI auth, Unity Catalog catalog + `00_bronze`/`01_silver`/`02_gold` schemas
2. Bronze: PySpark ingestion of `samples.nyctaxi.trips` into Delta, with ingestion metadata
3. Silver: cleaning/typing, idempotent Delta `MERGE`
4. Gold: aggregates (daily trips/revenue, busiest zones) + data quality checks that fail the run
5. CI/CD + orchestration, thin and early: high-risk logic as pure functions in `src/` with a few pytest tests, GitHub Actions, and a Databricks Asset Bundle (`databricks.yml`) job running bronze → silver → gold for real
6. Scale out: generic `00_bronze/ingest.py` driven by `config/sources.toml` (nyctaxi + TPC-H), one generated, scheduled job per source, and silver/gold jobs on table update triggers; `src/` folders per schema, notebooks named by process
7. Complete tests: the remaining transformations and edge cases
8. Table DDL as versioned migrations (`src/<NN_layer>/ddl/<verb>_<layer>_<table>_v<NNN>.sql`, versioned per table; renames `rename_<layer>_<old>_to_<new>_v001.sql`; applied by the `apply_ddl` workflow), so jobs stop creating tables. **DDL holds only final (published) tables.** Intermediates inside one transformation (DataFrames, temp views, CTEs, or a `_tmp_` table created and dropped within a run) never go in the `ddl/` folders. In-repo migration runner; schemas created by a migration (not the bundle `schema` resource: dev mode renames it to `dev_<user>_<name>` and `bundle destroy` drops it with its data, tested). Table naming convention in `docs/PLAN.md` step 8: bronze `<source_system>_<source_table>`, silver plural `<entity>` + `<entity>_quarantine`, gold `fct_`/`dim_`/`agg_<subject>_<grain>`, temp `_tmp_<process>_<purpose>`, no layer in table names. Confirmed by the user; in progress

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
databricks bundle validate                                   # also runs resources/__init__.py (needs .venv with databricks-bundles)
databricks bundle validate -o json                           # inspect generated jobs, schedules, triggers and pause status
databricks bundle deploy                                     # dev target: jobs "[dev <user>] <name>", every schedule and trigger PAUSED
databricks bundle run ingest_trips                           # any ingest_<source>; prints the notebook's exit JSON
databricks bundle run clean_trips                            # must report rows_inserted: 0 on reruns
databricks bundle run build_trip_metrics                     # fails with DataQualityError if a check fails
databricks jobs list-runs --job-id <id> --limit 3            # the run's `trigger` field shows TABLE / PERIODIC / ONE_TIME
```

Tests (local PySpark, no Delta and no workspace needed). CI (`.github/workflows/ci.yml`) runs the same on pushes to main and on PRs:

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
.venv/bin/pytest                                             # pyproject.toml sets pythonpath=src, testpaths=tests
.venv/bin/pytest tests/test_silver.py -k time_zone           # single test
```

Layout: notebooks are Databricks source files (`# Databricks notebook source`, `# COMMAND ----------` between cells). `src/` has **one folder per schema** (`00_bronze/`, `01_silver/`, `02_gold/`, as the user chose), holding the processes that write to that schema. Processes are **named after what they do** (`ingest.py`, `clean_trips.py`, `build_trip_metrics.py`), never after the schema. On serverless a notebook's working directory is its own folder, so each notebook starts with a cell doing `sys.path.insert(0, os.path.abspath(".."))` to import the shared `src/medallion/` package (verified with a bundle run). Notebooks read the catalog and schema names from widgets, which the job fills in through job parameters from the bundle variables.

`src/medallion/` holds all the pure, unit-tested logic. Notebooks only read tables, call these functions, write tables and orchestrate:
- `sources.py`: `load_sources`, `get_source`, `parse_sources`, which validates `config/sources.toml`
- `bronze.py`: `add_ingestion_metadata`
- `silver.py`: `add_trip_id`, `keep_first_load`, `split_valid_and_rejected` (rules include `missing_required_value`, because `null <= 0` is null and would pass as valid), `to_silver_trips`, `to_quarantine`
- `gold.py`: `daily_trips`, `busiest_pickup_zones`
- `quality.py`: `gold_checks`, `raise_if_any_failed`

New logic goes into `src/medallion/` with tests. `DeltaTable` `MERGE`s and table writes stay in the notebooks, because the tests use plain local Spark without Delta.

**One workflow per process** (the user's production practice):
- Bronze is config-driven. `config/sources.toml` lists every source (`name`, `table`, `target`, `mode` = `append` | `overwrite`, `schedule` = Quartz cron in UTC).
- `resources/__init__.py` (Python-defined bundle resources, `databricks-bundles` pinned to the CLI version) generates one job `ingest_<name>` per entry. Each job runs `00_bronze/ingest.py` with job parameter `source` on its own schedule.
- `clean_trips` (silver) and `build_trip_metrics` (gold scorecard) are YAML jobs with **table update triggers** on their input table.
- Add a bronze table by adding a config entry. Never add a notebook or job YAML for it, and never put all sources in one job.
- Table update triggers fire only on data changes: a MERGE that inserts 0 rows commits a version but doesn't trigger downstream. This was verified on Free Edition by unpausing temporarily.
- To test a trigger in dev, unpause it with `databricks jobs update` and pause it again afterwards, because development mode deploys it paused.
- `samples.tpch.lineitem` (30M rows) is left out to protect Free Edition quota.

Source data facts (`samples.nyctaxi.trips`): 21,932 rows, Jan–Feb 2016, and no nulls. The columns are `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `trip_distance`, `fare_amount`, `pickup_zip`, `dropoff_zip`. There is **no trip ID**, and zones are ZIP codes. Bronze (`medallion.00_bronze.trips`) is append-only, so every run adds a full batch under a new `_batch_id`. Silver (`medallion.01_silver.trips`) keys trips by `trip_id` = SHA-256 of the six source columns, with timestamps as `unix_micros` so the key doesn't depend on the time zone. Its `MERGE` is insert-only, because a matching key means identical content. Rejected trips go to `medallion.01_silver.trips_quarantine`, keeping their bronze columns and adding `rejection_reasons`. Every distinct trip lands in exactly one of the two tables, and the notebook asserts that. Gold (`medallion.02_gold.daily_trips`, `busiest_pickup_zones`) runs in the order compute → data quality checks → overwrite, so a failing check raises `DataQualityError` before any write. Don't use `.cache()`/`.persist()` in notebooks: Databricks documents DataFrame caching as unsupported on serverless (not tested here).

## Working agreements

- **Get CI/CD working early, and test only the high-risk logic until the end.** (Done for this project: the full suite landed in step 7.) The user wants the delivery path (GitHub Actions, Asset Bundle deploy, a real run) working as early as possible, not built after every layer exists. Write tests as you go but keep them few. Cover the logic where a bug would silently corrupt data: the `trip_id` key, deduplication, the validation that splits silver from quarantine, and gold reconciliation / data quality checks. The complete test suite comes in the last step. (This came in after steps 0–4 were built, which is why orchestration and CI weren't set up earlier.)
- **CI on GitHub, CD from the laptop.** GitHub Actions only runs tests. The user deploys with `databricks bundle deploy` / `run` locally. Don't add Databricks credentials, service principals or deploy steps to CI.
- The repo is **public**. Confirm with the user before pushing, and keep secrets and workspace-specific IDs out of committed files.
- The user's GitHub profile README (`~/Code/matiastulli`) lists this project. Update its entry as the project progresses, and pull before editing because the user also edits it on the web.
- Add commands to this file as each step introduces them. Don't document commands that don't exist yet.
