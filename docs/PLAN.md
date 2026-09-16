# Plan

How this project is built, one step at a time. Each step lands in its own commits, and later steps are only planned here: their code is written when the step starts, so the details below may change as earlier steps teach us something.

**Status:** steps 0–6 done · next up: **step 7, complete the test suite**

| Step | Status | What it delivers |
|---|---|---|
| 0. Local environment | ✅ Done | PySpark + Delta Lake running on the laptop |
| 1. Setup | ✅ Done | Databricks Free Edition workspace, CLI auth, Unity Catalog catalog and schemas |
| 2. Bronze | ✅ Done | Raw trips appended into Delta, with ingestion metadata |
| 3. Silver | ✅ Done | Cleaned, typed, deduplicated trips via an idempotent `MERGE` |
| 4. Gold | ✅ Done | Daily and per-zone aggregates, plus data quality checks that fail the run |
| 5. CI/CD + orchestration | ✅ Done | A thin but working delivery path: high-risk logic tested in GitHub Actions, and an Asset Bundle job running bronze → silver → gold |
| 6. Scale out | ✅ Done | Generic ingestion driven by `config/sources.toml`, one scheduled job per source, and silver/gold jobs triggered by table updates |
| 7. Complete tests | ⏳ Next | The rest of the transformations as pure functions, with full pytest coverage |

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
│   00_bronze.tpch_*       7 TPC-H snapshots (overwrite)     step 6
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

- [`src/00_bronze.py`](../src/00_bronze.py) (in `notebooks/` until step 5) reads `samples.nyctaxi.trips` and appends to `medallion.00_bronze.trips`, adding:
  - `_batch_id`: one UUID per run
  - `_ingested_at`: when the run wrote the row
  - `_source_table`: where the data came from
  - `_source_file`: the source data file, from Spark's hidden `_metadata` column
- The notebook fails if a batch doesn't contain exactly the source row count, and it returns a JSON summary as the run output
- `scripts/run_notebook.sh` (replaced by the Asset Bundle in step 5) uploaded a notebook and runs it once on serverless with `databricks jobs submit`, passing the catalog and schema names from `.env`.

**Decisions:**
- **Append-only.** Rerunning adds the same trips again under a new `_batch_id`. Bronze keeps the full load history, and deduplication belongs to silver. Two runs are loaded so far (43,864 rows, 2 batches), which gives step 3 real duplicates to handle.
- **Versions checked:** serverless compute runs Spark 4.2.0 on Python 3.11, matching the local pins.

## 3. Silver ✅

**Goal:** one clean, correctly typed row per trip, and reruns that change nothing.

- [`src/01_silver.py`](../src/01_silver.py) (in `notebooks/` until step 5) reads all of bronze and merges into `medallion.01_silver.trips` (valid trips) and `medallion.01_silver.trips_quarantine` (rejected trips)
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

- [`src/02_gold.py`](../src/02_gold.py) (in `notebooks/` until step 5) builds two tables from `medallion.01_silver.trips`:
  - `medallion.02_gold.daily_trips`: trips, revenue, and average distance, fare and duration per `pickup_date`. That's 60 rows, one per day from 2016-01-01 to 2016-02-29.
  - `medallion.02_gold.busiest_pickup_zones`: pickup ZIPs ranked by trips with `dense_rank`, plus revenue and average fare. That's 120 ZIPs; the top three are 10001 (1,227 trips), 10003 (1,180) and 10011 (1,128).
- **13 data quality checks**, in three groups:
  - silver's contract: not empty, `trip_id` not null and unique
  - each gold table's shape: not empty, one row per key, no negative revenue, 5-digit ZIPs, ranking starts at 1
  - **reconciliation**: trips and revenue in each gold table add up exactly to silver
- `scripts/run_notebook.sh` was changed to submit with `--no-wait` and poll the run, so a failed run printed the notebook's own error and exited non-zero

**Decisions:**
- **Compute → check → write.** The checks run on the new aggregates before anything is written. On failure the notebook raises `DataQualityError` listing every failed check, and gold keeps its last good version. We tested this with a throwaway copy containing an impossible check: the run failed with `1 of 13 data quality checks failed, gold was not written`, and both tables stayed at the same Delta version.
- **Full overwrite each run** instead of incremental logic. The aggregates are tiny, and a Delta overwrite is atomic, so readers never see a half-written table.
- Trips over 3 hours stay in the averages. That's a simple, visible choice to revisit if the averages look off.

**Note:** Databricks' serverless limitations list DataFrame caching (`.cache()` / `.persist()`) as unsupported, so the notebook doesn't cache and the checks recompute from silver. That's cheap at this size. We didn't try caching ourselves.

## 5. CI/CD + orchestration, thin and early ✅

**Goal:** get the whole delivery path working now, not at the end. Every push is tested in CI, the pipeline deploys as a job, and one real run goes bronze → silver → gold. Later changes then land on a path that already works.

> This step used to come after all the layers, with every test written at the end. The order changed after step 4, following the working preference to **get CI/CD running early and test only the high-risk logic until the final step**.

Built, in the order that got the path working soonest:

1. **Asset Bundle first, with the notebooks unchanged**
   - [`databricks.yml`](../databricks.yml): bundle variables for the catalog and schemas, and a `dev` target in development mode. The job name gets a `[dev <user>]` prefix and schedules are paused.
   - [`resources/medallion_job.yml`](../resources/medallion_job.yml): one job with three serverless notebook tasks, bronze → silver → gold. Job parameters reach every task as notebook widgets.
   - No workspace host in the file (the repo is public): the CLI takes it from the `DEFAULT` profile
   - `databricks bundle deploy` + `databricks bundle run medallion` ran end to end in about 1 minute, and silver still inserted 0 rows on rerun
2. **High-risk logic moved into [`src/medallion/`](../src/medallion)**
   - `silver.py`: `add_trip_id`, `keep_first_load`, `split_valid_and_rejected`
   - `quality.py`: `gold_checks`, `raise_if_any_failed`
   - The notebooks import them. Typing, renaming, gold aggregates and table I/O stay in the notebooks until step 6.
3. **6 pytest tests** ([`tests/`](../tests)), which take about 6 seconds on local PySpark with no Delta
   - the same trip gets the same `trip_id` across batches, and a different trip gets a different one
   - `trip_id` doesn't change with the session time zone
   - the first load wins
   - every trip lands in exactly one of valid or rejected, with all the rules it broke
   - gold that matches silver passes every check
   - double-counted trips fail reconciliation and raise
   - **Checked that the tests catch real bugs:** hashing timestamps as strings, or keeping the latest load instead of the first, each made its own test fail
4. **Notebooks moved into `src/`**, next to the package they import. That's the layout of Databricks' default bundle template. On serverless a notebook's own folder is on `sys.path`, so the import needs no path fix (verified with a bundle run).
5. **GitHub Actions CI** ([`.github/workflows/ci.yml`](../.github/workflows/ci.yml)): Python 3.11, Java 17, `pip install -r requirements.txt`, then `pytest` on pushes to `main` and on pull requests. It passed on its first run.
6. `scripts/run_notebook.sh` removed, because the bundle replaces it

**Decisions:**
- **CI on GitHub, CD from the laptop.** GitHub Actions runs the tests, and deploys stay manual with `databricks bundle deploy` / `databricks bundle run` from the laptop. So GitHub holds no Databricks credentials and needs no service principal. The first CI run on GitHub passed (6 tests).
- **The package stays `medallion`, not `tests`.** It's production logic that the job runs. `tests/` holds the pytest tests that check it.
- **Bronze appends a full batch on every job run.** That's by design: silver deduplicates. Five batches are loaded so far.

## 6. Scale out: config-driven ingestion, one workflow per process ✅

**Goal:** a production-shaped structure that grows to many tables without growing the code.
- Each source is ingested by **its own workflow, on its own cadence** (daily, biweekly, monthly…)
- Silver transformations and gold scorecards are **separate workflows that react to their input tables**, instead of one DAG that runs everything at once

**Why:**
- One notebook per layer, named after its schema, mixes up *where data lives* with *what the code does*.
- One job that ingests every source at the same moment doesn't match production: sources arrive on different cadences, and one failure or retry shouldn't block unrelated sources. Silver shouldn't wait for bronze tables it doesn't read.

Built:
- **Folders per schema, processes named by what they do:** `src/00_bronze/ingest.py`, `src/01_silver/clean_trips.py`, `src/02_gold/build_trip_metrics.py`, with the shared logic in `src/medallion/`. A notebook's working directory is its own folder on serverless, so each notebook adds `src/` to `sys.path` in its first code cell.
- **[`config/sources.toml`](../config/sources.toml)** lists every bronze source: `name`, `table`, `target`, `mode`
  - `trips` uses `append`
  - 7 TPC-H tables use `overwrite` (full snapshots)
  - `lineitem` (30M rows) is left out to protect Free Edition quota
- **One generic [`ingest.py`](../src/00_bronze/ingest.py)**, parameterized by `source`
- **Config validation** in [`src/medallion/sources.py`](../src/medallion/sources.py), with 7 tests: required and unknown keys, identifiers, `catalog.schema.table`, known `mode`, unique names and targets, and the committed config is valid

Tried first, then replaced: one job with a `list_sources` task and a `for_each_task` fanning `ingest.py` out over every source. It worked (8/8 iterations, row counts matched, `overwrite` stayed at one batch on rerun). But it runs every source at the same moment, which is the problem described above.

Built on top:
- **`schedule` per source** in [`config/sources.toml`](../config/sources.toml) (Quartz cron, UTC). The cadences are illustrative:
  - `trips`, `tpch_customer`, `tpch_orders`: daily
  - `tpch_supplier`, `tpch_part`, `tpch_partsupp`: twice a month, on the 1st and 15th (Quartz has no "every 14 days")
  - `tpch_region`, `tpch_nation`: monthly
  - Validation rejects a 5-field Unix cron, the usual mistake (1 more test, 8 in total for the config)
- **One ingestion job per source, generated** by [`resources/__init__.py`](../resources/__init__.py) with Python-defined bundle resources (`databricks-bundles==1.16.1`, matching the CLI)
  - It reuses the validated config loader, so a bad config fails `bundle validate`
  - Each job `ingest_<name>` runs `src/00_bronze/ingest.py` with job parameter `source`, on its own schedule
  - `list_sources.py` and the single `medallion` job with its `for_each_task` are removed
- **[`clean_trips`](../resources/clean_trips_job.yml)** (silver) with a table update trigger on `00_bronze.trips`
- **[`build_trip_metrics`](../resources/build_trip_metrics_job.yml)** (gold scorecard) with a table update trigger on `01_silver.trips`
- All 10 jobs are deployed. In `dev`, development mode pauses every schedule and trigger.

Verified on Free Edition (both capabilities work):
- `bundle validate -o json` showed the 8 generated ingestion jobs with their schedules, and the two triggers, all `PAUSED`
- **The silver trigger fired:** with the triggers temporarily unpaused, running only `ingest_trips` started `clean_trips` by itself about 75 seconds later (`trigger: TABLE`, the 60-second settle time plus evaluation). It succeeded and inserted 0 rows.
- **Gold did not start after that**, which is correct. The 0-row `MERGE` committed a Delta version with no files added, and table update triggers only fire on data changes, so the scorecard doesn't rebuild when silver didn't change.
- **The gold trigger fired on a real change,** in a test that repaired itself:
  - deleting one silver trip triggered `build_trip_metrics` (`TABLE`), and its checks passed against the 21,846 remaining rows
  - running `clean_trips` re-inserted exactly that trip (`rows_inserted: 1`, back to 21,847), which triggered gold again
- Both triggers were paused again afterwards

**Decisions:**
- **One workflow per process,** following production practice. Each source is ingested on its own cadence, silver transformations run per entity, and each gold scorecard is its own workflow. Failures, retries and schedules don't couple unrelated data.
- **Generic code, generated workflows.** One `ingest.py` for every source, plus one job per source generated from the config, so 40 tables means 40 config entries and still no per-source code or YAML.
- **Event-driven downstream.** Silver and gold react to their input tables instead of guessing when bronze finished on their own schedules. Downstream processes don't know about upstream jobs.
- **The load mode is per source.** `append` keeps history for data that needs deduplication, and `overwrite` suits reference snapshots and keeps Free Edition storage flat.
- **Folders mirror the schemas, and files name the process** (the user's call).
- **Not built yet:** silver and gold for TPC-H, and generic silver helpers parameterized per entity. They come when a second entity actually needs silver.

## 7. Complete the test suite ⏳

**Goal:** fill in the tests that steps 5 and 6 deliberately skipped.

Planned:
- Move the remaining transformations into `src/medallion/` (bronze metadata columns, silver typing and renaming, gold aggregates) and test them
- Edge cases for the step 5 functions: nulls in key columns, trips breaking several rules, empty inputs
- Keep CI fast, so the tests keep running on every push

---

## How to keep this file current

- When a step lands: change its status in the table and in its heading, move "Planned" to what was built, and note decisions and anything learned
- This file is the only plan. The README describes what exists (getting started, layers) and links here
- When a later step's plan changes, edit it here before writing the code
