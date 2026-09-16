# Plan

How this project is built, one step at a time. Each step lands in its own commits, and later steps are only planned here: their code is written when the step starts, so the details below may change as earlier steps teach us something.

**Status:** steps 0–2 done · next up: **step 3, Silver**

| Step | Status | What it delivers |
|---|---|---|
| 0. Local environment | ✅ Done | PySpark + Delta Lake running on the laptop |
| 1. Setup | ✅ Done | Databricks Free Edition workspace, CLI auth, Unity Catalog catalog and schemas |
| 2. Bronze | ✅ Done | Raw trips appended into Delta, with ingestion metadata |
| 3. Silver | ⏳ Next | Cleaned, typed, deduplicated trips via an idempotent `MERGE` |
| 4. Gold | 🔜 Planned | Daily and per-zone aggregates, plus data quality checks that fail the run |
| 5. Orchestration | 🔜 Planned | A Databricks Asset Bundle job running bronze → silver → gold |
| 6. Tests + CI | 🔜 Planned | Transformations as pure functions, pytest, GitHub Actions |

## Constraints that shape every step

- **PySpark + Delta Lake only.** dbt and Terraform are covered in the sibling repo [dbt-terraform-postgres-medallion](https://github.com/matiastulli/dbt-terraform-postgres-medallion).
- **Databricks Free Edition:** serverless compute only, with usage limits. If a step doesn't fit, the step gets adapted. No paid features.
- **Public repo:** no tokens, workspace hosts or IDs in committed files. Workspace settings live in a git-ignored `.env`, with `.env.example` as the committed template. Auth is browser OAuth (`databricks auth login`).
- **Simple data, not business rules:** the [NYC taxi sample](https://docs.databricks.com/aws/en/discover/databricks-datasets) keeps the focus on the engineering.

## The data

`samples.nyctaxi.trips` ships with every workspace. We profiled it on serverless in step 2:

- 21,932 rows, pickups from 2016-01-01 to 2016-02-29
- 6 columns: `tpep_pickup_datetime`, `tpep_dropoff_datetime` (timestamp), `trip_distance`, `fare_amount` (double), `pickup_zip`, `dropoff_zip` (int)
- No nulls, and no fully duplicated rows
- **No trip ID** and **no zone names**: "zones" in this project are ZIP codes

## Unity Catalog layout

```
medallion                  catalog (Default Storage)
├── 00_bronze.trips        raw, append-only                  step 2
├── 01_silver.trips        clean, one row per trip           step 3
└── 02_gold.*              aggregates                        step 4
```

Each schema name starts with its layer number, so the schemas sort in pipeline order. Unquoted names like `medallion.00_bronze.trips` work in Spark SQL, and the scripts quote them with backticks anyway.

---

## 0. Local environment ✅

**Goal:** run PySpark with Delta Lake on the laptop. We did this first because the Databricks site was down during setup, and later it's what the unit tests run on.

- Python 3.11, Java 17 (Homebrew `openjdk@17`), and pinned `pyspark==4.2.0` + `delta-spark==4.4.0` in `requirements.txt`
- A local Delta session needs `configure_spark_with_delta_pip` plus the two Delta configs, which are documented in `CLAUDE.md`
- Checked end to end with a local Delta `MERGE`

## 1. Setup ✅

**Goal:** a workspace we can drive from the terminal, and a place for each layer's tables.

- Free Edition workspace, with `databricks auth login` saving the `DEFAULT` profile
- [`scripts/setup_unity_catalog.sh`](../scripts/setup_unity_catalog.sh) creates the `medallion` catalog and its `00_bronze` / `01_silver` / `02_gold` schemas. The script is idempotent and reads `.env`.
- `.env` / `.env.example` for the workspace host, CLI profile, warehouse ID and catalog/schema names

**Learned:** on Free Edition, `databricks catalogs create` fails because the metastore has no storage root. `CREATE CATALOG` run as SQL on the serverless SQL warehouse works and puts the catalog on Default Storage, so the setup script uses the SQL Statement Execution API.

## 2. Bronze ✅

**Goal:** land the source data in our own Delta table, untouched, and trace where every row came from.

- [`notebooks/00_bronze.py`](../notebooks/00_bronze.py) reads `samples.nyctaxi.trips` and appends to `medallion.00_bronze.trips`, adding:
  - `_batch_id`: one UUID per run
  - `_ingested_at`: when the run wrote the row
  - `_source_table`: where the data came from
  - `_source_file`: the source data file, from Spark's hidden `_metadata` column
- The notebook fails if a batch doesn't contain exactly the source row count, and it returns a JSON summary as the run output
- [`scripts/run_notebook.sh`](../scripts/run_notebook.sh) uploads a notebook and runs it once on serverless with `databricks jobs submit`, passing the catalog and schema names from `.env`

**Decisions:**
- **Append-only.** Rerunning adds the same trips again under a new `_batch_id`. Bronze keeps the full load history, and deduplication belongs to silver. Two runs are loaded so far (43,864 rows, 2 batches), which gives step 3 real duplicates to handle.
- **Versions checked:** serverless compute runs Spark 4.2.0 on Python 3.11, matching the local pins.

## 3. Silver ⏳

**Goal:** one clean, correctly typed row per trip, and reruns that change nothing.

Planned:
- `notebooks/01_silver.py` reads bronze and writes `medallion.01_silver.trips`
- **Key:** the source has no ID, so build `trip_id` as a hash (`sha2`) of the six source columns. Identical trips from different bronze batches get the same key.
- **Deduplicate** within the input on `trip_id`, keeping the most recent `_ingested_at`
- **Clean** with simple, readable rules, for example:
  - drop trips with a non-positive distance or fare
  - drop trips whose dropoff is before the pickup
  - derive `pickup_date` and `trip_duration_minutes`
- **Delta `MERGE`** into silver on `trip_id`: update matched rows, insert new ones. Running it twice must leave the row count unchanged, and the notebook checks that.

Open questions:
- Should silver read all of bronze or only batches it hasn't processed yet? Reading everything is simplest at this size, while tracking processed batches teaches incremental loading.
- Should rejected rows be dropped, or kept in a quarantine table?

## 4. Gold 🔜

**Goal:** tables that answer questions directly, plus a run that fails loudly when the data is wrong.

Planned:
- `notebooks/02_gold.py` with two tables in `medallion.02_gold`:
  - `daily_trips`: trips, revenue (`sum(fare_amount)`) and average distance per `pickup_date`
  - `busiest_pickup_zones`: trips and revenue per `pickup_zip`, ranked
- Rebuilt in full on each run (overwrite), because aggregates this small don't need incremental logic
- **Data quality checks** that raise and fail the run, for example:
  - silver and gold aren't empty
  - `trip_id` is unique and not null in silver
  - no negative revenue
  - total trips in `daily_trips` equal silver's row count

## 5. Orchestration 🔜

**Goal:** deploy and run the whole pipeline as one job, with no hand-run scripts.

Planned:
- `databricks.yml` Asset Bundle with a job of three notebook tasks, bronze → silver → gold, each depending on the previous one
- Serverless compute, and bundle variables for the catalog and schema names (replacing the `.env` → widget handoff for deployed runs)
- A `dev` target, deployed with `databricks bundle deploy` and run with `databricks bundle run`
- Replaces `scripts/run_notebook.sh`
- Keep the job schedule paused or leave it out, to protect Free Edition quota

## 6. Tests + CI 🔜

**Goal:** the transformation logic is tested without a workspace.

Planned:
- Move transformations (bronze metadata, silver key/cleaning/dedup, gold aggregates) into pure functions in `src/`, each taking and returning a DataFrame. The notebooks keep only the reading, writing and orchestration.
- pytest on local PySpark + Delta (the step 0 environment) with small hand-written DataFrames
- GitHub Actions: Python 3.11 + Java 17, install `requirements.txt`, run pytest. CI never talks to the workspace, so it needs no secrets.

---

## How to keep this file current

- When a step lands: change its status in the table and in its heading, move "Planned" to what was built, and note decisions and anything learned
- Tick the same step off in the README's learning path
- When a later step's plan changes, edit it here before writing the code
