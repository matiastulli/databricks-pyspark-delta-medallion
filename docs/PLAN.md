# Plan

How this project is built, one step at a time. Each step lands in its own commits, and later steps are only planned here: their code is written when the step starts, so the details below may change as earlier steps teach us something.

**Status:** steps 0–4 done · next up: **step 5, CI/CD + orchestration**

| Step | Status | What it delivers |
|---|---|---|
| 0. Local environment | ✅ Done | PySpark + Delta Lake running on the laptop |
| 1. Setup | ✅ Done | Databricks Free Edition workspace, CLI auth, Unity Catalog catalog and schemas |
| 2. Bronze | ✅ Done | Raw trips appended into Delta, with ingestion metadata |
| 3. Silver | ✅ Done | Cleaned, typed, deduplicated trips via an idempotent `MERGE` |
| 4. Gold | ✅ Done | Daily and per-zone aggregates, plus data quality checks that fail the run |
| 5. CI/CD + orchestration | ⏳ Next | A thin but working delivery path: high-risk logic tested in GitHub Actions, and an Asset Bundle job running bronze → silver → gold |
| 6. Complete tests | 🔜 Planned | The rest of the transformations as pure functions, with full pytest coverage |

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

## 3. Silver ✅

**Goal:** one clean, correctly typed row per trip, and reruns that change nothing.

- [`notebooks/01_silver.py`](../notebooks/01_silver.py) reads all of bronze and merges into `medallion.01_silver.trips` (valid trips) and `medallion.01_silver.trips_quarantine` (rejected trips)
- Both tables are created up front with explicit types, `NOT NULL` and column comments, because silver's schema is a contract for gold
- **Key:** `trip_id` = SHA-256 of the six source columns. Timestamps go in as `unix_micros`, because their string form depends on the session time zone.
- **Deduplicate** on `trip_id`, keeping the first load (earliest `_ingested_at`). 43,864 bronze rows became exactly the source's 21,932 trips.
- **Validate:** each trip gets `rejection_reasons`, the names of the rules it breaks: a distance of 0 or less (76), a fare of 0 or less (10), a dropoff not after the pickup (1). The 85 rejected trips (2 broke two rules) go to quarantine with their bronze columns and types untouched.
- **Type and rename:**
  - `pickup_at` / `dropoff_at`
  - `pickup_date` and `trip_duration_minutes`, both derived
  - `trip_distance_miles`
  - `fare_amount` as `DECIMAL(10,2)`
  - ZIPs as zero-padded strings: New Jersey's `7002` becomes `07002`
- **Insert-only `MERGE`** on `trip_id` into both tables → 21,847 in silver, 85 in quarantine
- **Reconciliation checks** that fail the run:
  - `trip_id` is unique in each table
  - no `trip_id` appears in both tables
  - silver plus quarantine equals the number of distinct bronze trips

**Decisions:**
- **Insert-only merge, no update clause.** The key hashes every source column, so a matched `trip_id` means identical content. Running it again inserted 0 rows into each table, which the Delta history shows (`MERGE` with 21,847 inserted, then 0).
- **Read all of bronze on every run**, rather than tracking processed batches. At this size it's cheap, and incremental loading can come later if it's worth learning.
- **Quarantine rejected rows** instead of dropping them. Nothing disappears silently, and the quarantine can be queried to see why each trip was rejected. It keeps bronze's raw types, because the rows failed before typing.
- Trips over 3 hours (33) are kept. They're suspicious but not impossible, and gold can decide whether they matter.

## 4. Gold ✅

**Goal:** tables that answer questions directly, plus a run that fails loudly when the data is wrong.

- [`notebooks/02_gold.py`](../notebooks/02_gold.py) builds two tables from `medallion.01_silver.trips`:
  - `medallion.02_gold.daily_trips`: trips, revenue, and average distance, fare and duration per `pickup_date`. That's 60 rows, one per day from 2016-01-01 to 2016-02-29.
  - `medallion.02_gold.busiest_pickup_zones`: pickup ZIPs ranked by trips with `dense_rank`, plus revenue and average fare. That's 120 ZIPs; the top three are 10001 (1,227 trips), 10003 (1,180) and 10011 (1,128).
- **13 data quality checks**, in three groups:
  - silver's contract: not empty, `trip_id` not null and unique
  - each gold table's shape: not empty, one row per key, no negative revenue, 5-digit ZIPs, ranking starts at 1
  - **reconciliation**: trips and revenue in each gold table add up exactly to silver
- `scripts/run_notebook.sh` now submits with `--no-wait` and polls the run, so a failed run prints the notebook's own error and exits non-zero

**Decisions:**
- **Compute → check → write.** The checks run on the new aggregates before anything is written. On failure the notebook raises `DataQualityError` listing every failed check, and gold keeps its last good version. We tested this with a throwaway copy containing an impossible check: the run failed with `1 of 13 data quality checks failed, gold was not written`, and both tables stayed at the same Delta version.
- **Full overwrite each run** instead of incremental logic. The aggregates are tiny, and a Delta overwrite is atomic, so readers never see a half-written table.
- Trips over 3 hours stay in the averages. That's a simple, visible choice to revisit if the averages look off.

**Note:** Databricks' serverless limitations list DataFrame caching (`.cache()` / `.persist()`) as unsupported, so the notebook doesn't cache and the checks recompute from silver. That's cheap at this size. We didn't try caching ourselves.

## 5. CI/CD + orchestration, thin and early ⏳

**Goal:** get the whole delivery path working now, not at the end. Every push is tested in CI, the pipeline deploys as a job, and one real run goes bronze → silver → gold. Later changes then land on a path that already works.

> This step used to come after all the layers, with every test written at the end. The order changed after step 4, following the working preference to **get CI/CD running early and test only the high-risk logic until the final step**.

Planned:
- **Pull only the high-risk logic out into pure functions in `src/`**, each taking and returning a DataFrame or plain values. The notebooks import these functions and keep the reading, writing and orchestration.
  - `trip_id` key: stable across batches and time zones
  - deduplication: the first load wins
  - validation: every trip ends up in exactly one of silver or quarantine, with the right reasons
  - gold data quality checks: reconciliation catches lost or double-counted trips
- **A few pytest tests** for exactly those, on local PySpark (the step 0 environment) with small hand-written DataFrames. No coverage for its own sake.
- **GitHub Actions CI:** Python 3.11 + Java 17, install `requirements.txt`, run pytest on every push and pull request
- **`databricks.yml` Asset Bundle:**
  - one job with three serverless notebook tasks, bronze → silver → gold
  - bundle variables for the catalog and schema names
  - a `dev` target
  - deploy and run it once for real with `databricks bundle deploy` / `databricks bundle run`
  - it replaces `scripts/run_notebook.sh`
  - no schedule, or a paused one, to protect Free Edition quota

Open questions:
- **CD from GitHub Actions, or deploy from the laptop?** Deploying from CI needs a Databricks service principal with OAuth credentials stored as GitHub Actions secrets. They'd be created in the Databricks and GitHub UIs, never pasted into chat or committed. First check that Free Edition allows it. If it doesn't, CI runs tests plus `databricks bundle validate`, and deploys happen from the laptop.

## 6. Complete the test suite 🔜

**Goal:** fill in the tests that step 5 deliberately skipped.

Planned:
- Move the remaining transformations into `src/` (bronze metadata columns, silver typing and renaming, gold aggregates) and test them
- Edge cases for the step 5 functions: nulls in key columns, trips breaking several rules, empty inputs
- Keep CI fast, so the tests keep running on every push

---

## How to keep this file current

- When a step lands: change its status in the table and in its heading, move "Planned" to what was built, and note decisions and anything learned
- This file is the only plan. The README describes what exists (getting started, layers) and links here
- When a later step's plan changes, edit it here before writing the code
