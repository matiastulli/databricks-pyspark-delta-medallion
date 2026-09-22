# Plan

How this project is built, one step at a time. Each step lands in its own commits, and later steps are only planned here: their code is written when the step starts, so the details below may change as earlier steps teach us something.

**Status:** steps 0–9 done · next up: **step 10, Auto Loader**

| Step | Status | What it delivers |
|---|---|---|
| 0. Local environment | ✅ Done | PySpark + Delta Lake running on the laptop |
| 1. Setup | ✅ Done | Databricks Free Edition workspace, CLI auth, Unity Catalog catalog and schemas |
| 2. Bronze | ✅ Done | Raw trips appended into Delta, with ingestion metadata |
| 3. Silver | ✅ Done | Cleaned, typed, deduplicated trips via an idempotent `MERGE` |
| 4. Gold | ✅ Done | Daily and per-zone aggregates, plus data quality checks that fail the run |
| 5. CI/CD + orchestration | ✅ Done | A thin but working delivery path: high-risk logic tested in GitHub Actions, and an Asset Bundle job running bronze → silver → gold |
| 6. Scale out | ✅ Done | Generic ingestion driven by `config/sources.toml`, one scheduled job per source, and silver/gold jobs triggered by table updates |
| 7. Complete tests | ✅ Done | The rest of the transformations as pure functions, with full pytest coverage |
| 8. DDL as migrations | ✅ Done | Tables created and changed only by versioned SQL migrations applied by an `apply_ddl` workflow; jobs stop creating tables |
| 9. Layout + maintenance | ✅ Done | Liquid clustering and Delta table properties through migrations, plus a `maintain_tables` workflow (`OPTIMIZE`, `REORG`, `VACUUM`), measured |
| 10. Auto Loader | ⏳ Next | File ingestion from a UC volume with Auto Loader, `availableNow` and a checkpoint |
| 11. Change Data Feed | 🔜 Planned | An incremental gold built from silver's change feed instead of a full rebuild |

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

## 7. Complete the test suite ✅

**Goal:** fill in the tests that steps 5 and 6 deliberately skipped.

Built:
- **The rest of the logic moved into `src/medallion/`**, so notebooks only read, write and orchestrate:
  - `bronze.py`: `add_ingestion_metadata`
  - `silver.py`: `to_silver_trips` (typing and renaming) and `to_quarantine`
  - `gold.py`: `daily_trips`, `busiest_pickup_zones`
- **A real bug found by an edge-case test, written to fail first:** a trip with a missing value passed validation as *valid*. `null <= 0` is null, not true, so no rule flagged it, and silver's `NOT NULL` columns would have failed the `MERGE` on the first null. Fixed with a `missing_required_value` rule. Today's source has no nulls, so the published numbers didn't change.
- **23 tests** (up from 14), about 7 seconds locally:
  - bronze metadata: source columns untouched, the same batch metadata on every row
  - silver edge cases: null key columns give a stable key that differs from real values, a missing value is rejected, empty input gives empty output
  - silver typing: column names and order, zero-padded ZIPs, `DECIMAL` fares, duration, pickup date; quarantine keeps the bronze values as received
  - gold aggregates: daily sums and averages, and tied zones share a `dense_rank` with no gap after
  - quality: an empty silver fails instead of publishing an empty scorecard
- **Verified on the workspace after the refactor:** `ingest_tpch_region`, `clean_trips` and `build_trip_metrics` all succeeded with the same results as before (21,847 trips, 85 quarantined, 13/13 checks, same top zones)

## 8. Table DDL as versioned migrations, separate from the jobs ✅

**Goal:** tables are created and changed only by reviewed, versioned DDL, never by the jobs that write to them. The jobs load data into tables that already exist, and Delta rejects any write that doesn't match the declared schema.

**Why:**
- **It's a common production practice** (and the user's, from work): the schema is a contract with its own review, history and owner.
- **The Databricks docs point the same way,** without naming one required pattern:
  - [Schema enforcement](https://docs.databricks.com/aws/en/tables/schema-enforcement): every write must match the target table's columns and types, so a pre-created table protects its own schema
  - [Schema evolution](https://docs.databricks.com/aws/en/tables/update-schema) should be explicit per write (`mergeSchema`, `MERGE WITH SCHEMA EVOLUTION`), never a session-wide setting
  - [Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/resources) can manage catalogs, schemas and volumes, but have **no table resource**
  - [Terraform's `databricks_sql_table`](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/sql_table) says it doesn't handle complex schema evolution and recommends migration tools like **Flyway or Liquibase**
- **Today's weak spots:**

  | Layer | How the table is created now | Problem |
  |---|---|---|
  | Bronze | `saveAsTable` infers the schema on the first write | Nobody reviews the schema |
  | Silver | `CREATE TABLE IF NOT EXISTS` inside `clean_trips.py` | **Changing that DDL never alters the existing table** (`IF NOT EXISTS` skips it), so the code and the real table can silently drift apart |
  | Gold | `overwrite` + `overwriteSchema=true` | Any run can change the table's schema, which turns off schema enforcement |

  Jobs that create tables also need `CREATE` privileges they otherwise wouldn't.

**Scope rule (the user's decision): DDL holds only the final tables.**
- **In the `ddl/` folders:** every table a process *publishes*, meaning the tables other processes, people or tools read: every bronze source table, silver `trips` and `trips_quarantine`, and the gold tables.
- **Not in the `ddl/` folders:** anything that only exists **during one transformation**, such as intermediate DataFrames, temporary views (`createOrReplaceTempView`) and CTEs. They belong to the process's code and disappear when the run ends.
  - Today every intermediate step (`keyed`, `deduplicated`, `valid`/`rejected`, the aggregates before the checks) is already an in-memory DataFrame, so nothing moves out of the code.
  - If a transformation ever needs a temporary **persisted** table (for example, to break up a very long plan), the job creates it and drops it at the end of the run, and it stays out of the `ddl/` folders. Its name must make that clear, e.g. a `_tmp_` prefix.

**Decisions (the user's):**
- **Migrations live next to their schema's code, versioned per table:** `src/<NN_layer>/ddl/<layer>_<table>_v<NNN>_<verb>.sql`, e.g. `src/01_silver/ddl/silver_trips_v001_create.sql`. Each table has its own version sequence, and the version goes at the end of the file name. They are deployed with the rest of `src/`.
- **Runner: a small in-repo runner,** not Flyway or Liquibase. It keeps the project PySpark-only and is small enough to test fully.
- **Schemas move into a migration** (`src/00_bronze/ddl/schemas_v001_create.sql`), not the bundle's `schema` resource. We tested the bundle resource on Free Edition with a throwaway bundle, and it showed two problems:
  - **development mode renames the schemas:** `zz_schema_probe` was created as `medallion.dev_jmatiastulli_zz_schema_probe`, so in `dev` the bundle would create parallel schemas instead of adopting `00_bronze`, `01_silver` and `02_gold`
  - **`bundle destroy` dropped the schema together with a table holding data**

  With a migration, a schema is only dropped by an explicit, reviewed migration. The **catalog** stays in `scripts/setup_unity_catalog.sh`, because Free Edition can't create it through the API.

**Naming convention (declared for step 8; enforced by validation and tests where possible):**

*Schemas*
- Medallion layers: `NN_<layer>`, e.g. `00_bronze`, `01_silver`, `02_gold`. The number keeps pipeline order when sorted.
- Anything else: a plain name, e.g. `ops` for pipeline bookkeeping such as the migration history.

*Tables*
- `snake_case`, matching `^[a-z][a-z0-9_]*$`, so table names never need quoting
- **No layer in the table name**, because the schema already says it (not `bronze_trips`, not `stg_trips`)

| Where | Pattern | Examples | Why |
|---|---|---|---|
| Bronze | `<source_system>_<source_table>` | `nyctaxi_trips`, `tpch_orders`, `tpch_customer` | Mirrors the source exactly, including its singular/plural. The system prefix prevents collisions when two systems have a table with the same name. |
| Silver | `<entity>` as a plural noun, plus `<entity>_quarantine` for its rejected rows | `trips`, `trips_quarantine` | A cleaned, conformed business entity no longer belongs to one source system |
| Gold | `fct_<event>` (one row per event), `dim_<entity>` (one row per entity), `agg_<subject>_<grain>` (aggregates and scorecards) | `agg_trips_daily`, `agg_trips_by_pickup_zip` | The prefix says how to use the table, the same convention as `fct_orders` / `dim_customers` in [dbt-terraform-postgres-medallion](https://github.com/matiastulli/dbt-terraform-postgres-medallion) |
| Temporary (never in `ddl/`) | `_tmp_<process>_<purpose>`, created and dropped within one run | `_tmp_clean_trips_keyed` | Anything starting with `_tmp_` is safe to drop |
| Bookkeeping | `ops.<purpose>` | `ops.schema_migrations` | Owned by tooling, not by a pipeline |

*Columns*
- Bronze keeps the **source column names untouched** (raw stays raw). Renaming happens in silver, e.g. `tpep_pickup_datetime` → `pickup_at`.
- `_` prefix only for pipeline metadata: `_batch_id`, `_ingested_at`, `_source_table`, `_source_file`, `_merged_at`
- Timestamps `<event>_at` (UTC), dates `<event>_date`
- Measures carry their unit: `trip_distance_miles`, `trip_duration_minutes`
- Money: `<name>_amount` as `DECIMAL`
- Keys: `<entity>_id`, e.g. `trip_id`; booleans: `is_<state>` / `has_<thing>`

*Migrations*
- `src/<NN_layer>/ddl/<layer>_<table>_v<NNN>_<create|alter|rename|drop>.sql`, with versions counted **per table** from `v001`, e.g. `silver_trips_v001_create.sql`, then `silver_trips_v002_alter.sql`. **The subject comes first and the action last** (the user's call), so a table's whole history sorts together in the folder and reads in version order.
- The schemas: `src/00_bronze/ddl/schemas_v001_create.sql`
- A table's history starts with `create` (v001). A rename starts the **new** table's history and names both tables: `<layer>_<old>_to_<new>_v001_rename.sql`, e.g. `bronze_trips_to_nyctaxi_trips_v001_rename.sql`
- The layer in the file name must match its folder (`silver` only in `01_silver/ddl/`)

*Enforcement*
- Bronze targets are **derived** from the source instead of written by hand: `samples.nyctaxi.trips` → `nyctaxi_trips`. The `target` field leaves `config/sources.toml`, which removes one thing to get wrong (tested).
- The runner rejects migration files that don't match the file-name pattern (tested)

**Renames this convention requires**, applied as migrations (`ALTER TABLE … RENAME TO`), after the baseline:

| Today | After |
|---|---|
| `00_bronze.trips` | `00_bronze.nyctaxi_trips` |
| `02_gold.daily_trips` | `02_gold.agg_trips_daily` |
| `02_gold.busiest_pickup_zones` | `02_gold.agg_trips_by_pickup_zip` |

`tpch_*`, silver `trips` and `trips_quarantine` already match. Code that follows the renames: `clean_trips.py` reads `nyctaxi_trips`, the `clean_trips` trigger watches `00_bronze.nyctaxi_trips`, the gold writes, and the docs. Verify on the workspace that the Delta history and data survive the rename, and that the trigger fires on the new name.

Planned:
- **Write-once SQL files in each schema's `ddl/` folder, versioned per table,** for example:
  ```
  src/00_bronze/ddl/schemas_v001_create.sql                         CREATE SCHEMA IF NOT EXISTS 00_bronze / 01_silver / 02_gold
  src/00_bronze/ddl/bronze_trips_v001_create.sql                    baseline: the tables exactly as they exist today
  src/00_bronze/ddl/bronze_tpch_region_v001_create.sql …
  src/01_silver/ddl/silver_trips_v001_create.sql
  src/01_silver/ddl/silver_trips_quarantine_v001_create.sql
  src/02_gold/ddl/gold_daily_trips_v001_create.sql
  src/02_gold/ddl/gold_busiest_pickup_zones_v001_create.sql
  src/00_bronze/ddl/bronze_trips_to_nyctaxi_trips_v001_rename.sql   then the naming convention renames
  src/02_gold/ddl/gold_daily_trips_to_agg_trips_daily_v001_rename.sql
  src/02_gold/ddl/gold_busiest_pickup_zones_to_agg_trips_by_pickup_zip_v001_rename.sql
  ```
  A schema change is always a **new** version of that table (e.g. `silver_trips_v002_alter.sql` with `ALTER TABLE … ADD COLUMNS`). Applied files are never edited.
- **Baseline first:** the tables already exist with data, so the first migrations must describe them **exactly as they are today** (columns, types, `NOT NULL`, comments). They adopt the tables without rewriting or losing data, and applying them is checked against `DESCRIBE TABLE`.
- **A small migration runner** in `src/medallion/migrations.py` plus an `apply_ddl` workflow (serverless notebook, `spark.sql`):
  - reads `src/*/ddl/*.sql` and runs them in this order: `schemas` first, then layer folders (00, 01, 02), tables by name, each table's versions ascending, and **a renamed table always after the table it renames**. Otherwise `nyctaxi_trips` would sort before `trips`, and on a fresh catalog the rename would run before `trips` exists.
  - records applied versions in `medallion.ops.schema_migrations` (`migration` = `schemas` or `<layer>_<table>`, `version`, `file`, `checksum`, `applied_at`). The runner creates that schema and table itself on its first run, like Flyway's history table, because it has to exist before any migration can be recorded.
  - applies only pending migrations, so reruns do nothing
  - **fails if an applied migration's file changed** (checksum mismatch), moved or disappeared, and if a table has duplicate or missing versions, a misplaced or misnamed file, or a rename of a table that has no migrations
- **Jobs stop creating tables:**
  - `clean_trips.py` loses its `CREATE TABLE` cells
  - `build_trip_metrics.py` overwrites data without `overwriteSchema`
  - `ingest.py` writes into pre-created tables
  - A missing table or a mismatched schema fails the run with a clear error
- **Deploy order (CD stays on the laptop):** `databricks bundle deploy` → `databricks bundle run apply_ddl` → pipelines
- **High-risk tests:**
  - the runner's logic: run order (schemas → folders → tables → versions, `v010` after `v009`, renames after their source table), only pending migrations run, and checksum drift, duplicate or missing versions, misplaced or misnamed files all fail
  - bronze target names derived from the source
  - the rest of the runner goes in the full test suite as before
- **Get it working early:**
  1. Run the runner with the baseline migrations against the existing tables and confirm nothing changes (schemas identical, row counts intact)
  2. Remove table creation from one job, run it, and confirm the results are unchanged
  3. Force a mismatched write and confirm Delta rejects it
  4. Apply the renames and confirm data, history and the `clean_trips` trigger survive
  5. Then do the remaining jobs

Confirmed by the user:
- **Bronze DDL:** bronze tables are final tables, so they get migrations, generated per source by `scripts/new_bronze_migration.py <source>`. The script reads the source table's schema, adds the metadata columns, and writes the next numbered migration with the conventional name for review.
- **The naming convention** above, including the gold `fct_` / `dim_` / `agg_` prefixes and the three renames.

Built and verified on the workspace:
- **Migrations and runner:**
  - 17 migrations: the schemas (`schemas_v001_create`, plus `schemas_v002_alter` for the comments), 12 table baselines, and 3 renames
  - [`src/medallion/migrations.py`](../src/medallion/migrations.py) holds the logic (18 tests)
  - [`src/ops/apply_ddl.py`](../src/ops/apply_ddl.py) is the runner, deployed as the [`apply_ddl`](../resources/apply_ddl_job.yml) job with `dry_run`
  - [`scripts/setup_unity_catalog.sh`](../scripts/setup_unity_catalog.sh) now creates only the catalog
- **The baseline matches reality exactly.** All migrations applied to a fresh throwaway catalog produced the same 12 tables as `medallion`: 117 columns with identical order, types, `NOT NULL` and comments, plus identical table comments. The comparison also caught one mismatch: the schema comments in `schemas_v001_create` had been rewritten instead of copied. They were restored in v001 and changed properly in `schemas_v002_alter`.
- **Drift detection works on Databricks.** Rerunning on the throwaway catalog after that edit failed with `schemas_v001_create.sql changed after it was applied`, and nothing ran.
- **Adopted without rewriting data.** On `medallion`: dry run, then 14 migrations applied. Every table's Delta version and row count were unchanged, the schema comments were updated by v002, and a rerun applied nothing.
- **Jobs stopped creating tables.**
  - `ingest.py` and `build_trip_metrics.py` write with `writeTo(...).append()` / `.overwrite(F.lit(True))` instead of `saveAsTable` + `overwriteSchema`
  - `clean_trips.py` lost its `CREATE TABLE` cells
  - all three check that their tables exist, and table comments now live only in DDL
  - After running all four job types, all 117 columns and 12 table comments were identical.
- **Schema enforcement, tested on a `_tmp_` table:** Delta rejected an extra column, a null into `NOT NULL`, and an impossible cast. It **accepted a missing nullable column, filling it with null**. So [`src/medallion/contract.py`](../src/medallion/contract.py) (`raise_if_schema_mismatch`, 3 tests) now checks column names and types exactly before every write. Every gold column is nullable, so this is what stops an aggregation that loses a column from publishing nulls.
- **Naming convention applied:**
  - `config/sources.toml` lost `name` and `target`: bronze names come from `bronze_table_name` (`samples.nyctaxi.trips` → `nyctaxi_trips`), and the job became `ingest_nyctaxi_trips`
  - three `…_to_…_v001_rename` migrations renamed `trips`, `daily_trips` and `busiest_pickup_zones`
  - row counts, Delta history versions (16, 20, 20) and comments moved with the tables, and the old names are gone
  - all jobs succeeded under the new names, with 13/13 gold checks
- **[`scripts/new_bronze_migration.py`](../scripts/new_bronze_migration.py)** reads a source's schema with `DESCRIBE TABLE` on the warehouse. Its output for `tpch_region` was identical to the committed baseline, and it refuses to overwrite an existing migration.
- **46 tests** in total (up from 23)
- **The silver trigger follows the rename.** `clean_trips` now watches `00_bronze.nyctaxi_trips`.
  - With the trigger unpaused 2 minutes before the write, `ingest_nyctaxi_trips` finished at 23:29:22 UTC and `clean_trips` started by itself at 23:30:58 (`trigger: TABLE`). It succeeded with the contract check (241,252 bronze rows, 0 inserted), and the trigger was paused again.
  - **Learned:** a table update trigger needs time after it's unpaused before it notices writes. In the first attempt the write committed about 25 seconds after unpausing, and nothing fired within 9 minutes. When testing triggers, unpause, wait a couple of minutes, then write.

Note: **least privilege** can only be documented here, not demonstrated. Free Edition has a single user, so the jobs and the migration runner run as the same identity.

## 9. Delta table layout and maintenance ✅

**Goal:** put the Delta layout and maintenance levers from [`docs/databricks.md`](databricks.md) into practice, and **measure** them, rather than repeating the theory.

**What the workspace already does for us** (checked 2026-09-22, before planning this step):
- **Deletion vectors are on everywhere** (`delta.enableDeletionVectors = true`), the Databricks default. So merge-on-read is already in play, and materializing the vectors is a maintenance question, not a setting to turn on.
- **Predictive optimization is already compacting.** `00_bronze.nyctaxi_trips` holds 241,252 rows in **1 file** (0.6 MB), and `DESCRIBE HISTORY` shows an `OPTIMIZE` run by a service principal, not by us. The catalog and schema both report `enable_predictive_optimization: INHERIT`.
- **No table is clustered or partitioned** (`clusteringColumns` is empty everywhere).
- Sizes today: `tpch_orders` 7.5M rows / 165 MB / 3 files; everything else is 1–2 files and under 2 MB.

**So the small-file problem doesn't exist here.** Anything this step adds is to show the mechanism and measure it honestly, not to fix a real pain. The plan says so, and the README should too.

Built and measured:
- **Table properties through migrations:** `bronze_nyctaxi_trips_v002_alter.sql`, `silver_trips_v002_alter.sql` and `silver_trips_quarantine_v002_alter.sql` set `delta.autoOptimize.optimizeWrite` and `autoCompact` on the tables that get a write on every run. Each migration says what the property does and when it acts (before the write vs after the commit).
- **Liquid clustering:** `bronze_tpch_orders_v002_alter.sql` sets `CLUSTER BY (o_orderdate)`, and the first `OPTIMIZE FULL` clustered the existing 7.5M rows.
- **[`src/ops/maintain_tables.py`](../src/ops/maintain_tables.py)** + [`maintain_tables`](../resources/maintain_tables_job.yml), weekly (Sunday 07:00 UTC, paused in dev): `OPTIMIZE` (with `optimize_full`), optional `REORG TABLE … APPLY (PURGE)`, and `VACUUM` in **dry run by default**. It discovers the tables itself, skips `_tmp_` ones, and reports files, size and clustering before and after. [`src/medallion/maintenance.py`](../src/medallion/maintenance.py) holds the tested part (3 tests).
- **[`scripts/measure_pruning.py`](../scripts/measure_pruning.py)** runs a query and reports files read vs pruned from query history.

**The measurement, on `00_bronze.tpch_orders` (7.5M rows, 165 MB):**

| Query | Before `CLUSTER BY` | After `CLUSTER BY (o_orderdate)` + `OPTIMIZE FULL` |
|---|---|---|
| `WHERE o_orderdate = '1996-01-02'` | 3 of 3 files read, 0 pruned, 37.1 MB read | **1 of 2 files read, 1 pruned**, 35.7 MB pruned |
| `WHERE o_orderdate BETWEEN … (a month)` | 3 of 3 files read, 0 pruned, 12.1 MB read | **1 of 2 files read, 1 pruned**, 35.7 MB pruned |

So file skipping went from nothing to half the table, on a table where every file previously spanned the full date range. In absolute terms this saves a second at most: the table is 165 MB, and Delta's row-group statistics were already keeping `rows_read` low. The mechanism is what's being shown.

**The maintenance run** (`optimize_full=true`, 12 tables): `tpch_orders` 3 → 2 files and now clustered, silver `trips` 2 → 1, everything else already at 1–2 files. `VACUUM … DRY RUN` found **0 files** to delete anywhere, which is consistent with predictive optimization having already tidied up.

**Learned:**
- **Query history redacts `query_text`**, so a measurement script can't find its own run by a comment tag. It matches on `statement_id`, which equals `query_id` in history.
- **The result cache silently ruins a before/after**: the same query returned in 397 ms reading 0 files. Comments don't defeat it, because Databricks normalizes them away; vary a harmless predicate instead. The script reports `from cache` so a cached run can't be mistaken for a fast one.
- `DESCRIBE DETAIL` can't be used as a subquery, unlike `DESCRIBE HISTORY`.
- Renaming migration files **breaks the history**, which records each file's path. Renaming all 21 files to the new convention needed a one-off `UPDATE` of `ops.schema_migrations`; after it, `apply_ddl` reported 21 applied and 0 pending with no drift.

**Out of scope, with reasons:**
- **Z-order:** left out. It can't coexist with liquid clustering on the same table, and adding a table only to demo the legacy approach is noise. `docs/databricks.md` §3 already explains it, including why liquid clustering replaced it. (The user can pull it back in, on a table of its own.)
- **Partitioning:** the tables are orders of magnitude below the threshold where it helps.

**Verified on Free Edition before building** (2026-09-22, on a throwaway `_tmp_` table that was dropped afterwards):
- `ALTER TABLE … CLUSTER BY (o_orderdate)` works on a UC managed table, and `DESCRIBE DETAIL` reports the clustering columns
- `OPTIMIZE`, `OPTIMIZE … FULL`, `REORG TABLE … APPLY (PURGE)` and `VACUUM … DRY RUN` all work on serverless
- Query history reports pruning: `pruned_files_count`, `read_files_count`, `pruned_bytes`, `rows_read_count`, via **GET** `/api/2.0/sql/history/queries?include_metrics=true` (the POST form doesn't exist in this CLI). So the clustering effect can be measured rather than asserted.
- `DESCRIBE DETAIL` can't be used as a subquery the way `DESCRIBE HISTORY` can; run it on its own and read the columns.

## 10. File ingestion: Auto Loader and checkpoints ⏳

**Goal:** close the biggest gap against [`docs/databricks.md`](databricks.md) §5. Bronze currently reads *tables*, so nothing here uses Auto Loader, Structured Streaming, triggers or checkpoints.

Planned:
- Land sample files in a Unity Catalog volume (export a slice of the trips sample), so there's a real file source
- A bronze process reading them with Auto Loader (`cloudFiles`), `trigger(availableNow=True)` and its own checkpoint, writing the same bronze table shape
- Show the checkpoint's `offsets/` / `commits/` / `sources/` contents, and that a rerun ingests nothing new
- Show what happens when new files land, and what deleting the checkpoint would do (described, not done to the real table)
- `maxFilesPerTrigger` to cap a backlog
- Keep the table-based ingestion for the other sources: the config gets a source *kind*

## 11. Change Data Feed: an incremental gold 🔜

**Goal:** replace gold's full rebuild with incremental processing, the last big idea in [`docs/databricks.md`](databricks.md) §2 that this project doesn't use.

Planned:
- `delta.enableChangeDataFeed` on `01_silver.trips` through a migration
- A gold process reading `table_changes(...)` since the last processed version instead of re-aggregating everything
- Keep the reconciliation checks, which are exactly what catches an incremental aggregation that drifts from its source
- Track the last processed version in an `ops` table, next to the migration history

---

## How to keep this file current

- When a step lands: change its status in the table and in its heading, move "Planned" to what was built, and note decisions and anything learned
- This file is the only plan. The README describes what exists (getting started, layers) and links here
- When a later step's plan changes, edit it here before writing the code
