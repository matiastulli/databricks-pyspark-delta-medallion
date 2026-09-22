# Databricks – Interview Study Guide

## TL;DR

- **Databricks is Spark plus Delta Lake on your object storage. Delta adds a transaction log that turns a folder of Parquet into an ACID table.**
- **The `_delta_log` gives you atomic commits, time travel and file-level min/max stats.** Those stats power file skipping, the same idea as Snowflake micro-partition pruning.
- **For new tables, use liquid clustering (`CLUSTER BY`), not partitioning plus Z-order.** Partition only very large tables on a low-cardinality column.
- **Copy-on-write rewrites whole files so reads stay clean; merge-on-read (deletion vectors) writes a bitmap instead and makes every read pay.** Frequent small writes fragment the table either way — optimized writes prevent it, auto compaction cures it, only `OPTIMIZE` can re-cluster.
- **A checkpoint is the job's bookmark: offsets written before the batch, commit written after.** Delete it and the job reprocesses everything.
- **Cost ordering, not digits:** Jobs compute < SQL warehouse < all-purpose. Serverless costs more per DBU, but VMs are included and there's no idle time. Never run scheduled jobs on all-purpose clusters.
- **Unity Catalog is governance** (catalog.schema.table grants, row filters, column masks, lineage). **Delta Sharing** shares live tables without copying them.

---

## 1. The problem: a data lake without transactions

Your trucks platform lands **2 TB/day** of Parquet in S3. Plain files on object storage break in predictable ways:

- **Half-written jobs.** A job dies after writing 300 of 500 files, and readers see a partial day.
- **No UPDATE or DELETE.** A GDPR delete or a late invoice correction means rewriting whole folders by hand.
- **Concurrent writers.** Two jobs overwrite the same partition and one silently wins.
- **Slow planning.** Listing millions of files just to plan a query takes minutes.

**Delta Lake** fixes this with a transaction log. That gives the lake the **ACID** guarantees a database has:
- **A**tomic commits: all files or none.
- **C**onsistent schema enforcement.
- **I**solated snapshot reads.
- **D**urable commits in the log.

**Databricks** is the managed platform around it: Spark (plus Photon), Delta, Unity Catalog, jobs and SQL warehouses.

| | Postgres-style DBMS | Databricks lakehouse |
|---|---|---|
| Built for | OLTP: point reads and writes, many small transactions | Analytics: big scans, batch/stream ETL, ML |
| Storage | Proprietary pages on local disk | Open Parquet + Delta log in *your* bucket |
| Compute | Coupled to storage | Separate clusters. Scale out, and pay nothing when idle |

**Decision rule:** app transactions → Postgres. Analytics on lake-scale files with Python/Spark ETL or ML → Databricks. SQL-first warehouse team → see §8.

---

## 2. Delta Lake: how the log works

```
loads/
├── part-0000-a1.parquet
├── part-0001-b2.parquet
└── _delta_log/
    ├── 00000000000000000000.json      ← v0: CREATE
    ├── 00000000000000000001.json      ← v1: add part-0000, part-0001 (with min/max stats per column)
    ├── 00000000000000000002.json      ← v2: MERGE → remove part-0001, add part-0002
    └── 00000000000000000010.checkpoint.parquet   ← periodic snapshot of the log
```

**Mechanism.** Each commit is a JSON file of actions: `add` (file plus stats), `remove`, `metaData` (schema), `protocol` and `commitInfo`. A reader loads the latest checkpoint, replays newer JSONs, and gets the exact file list for that version. Writers use **optimistic concurrency**: write files, then try to create the next version number. If another writer got there first and touched the same files, the commit fails and retries. Readers never block.

**Bridge:** the log's per-file min/max stats are to Delta what micro-partition metadata is to Snowflake. A `WHERE truck_id = 'T-42'` skips every file whose range excludes it.

### How a change lands: copy-on-write vs merge-on-read

Parquet files are immutable. There is no "edit row 7" — so every `UPDATE`, `DELETE` and `MERGE`
has to decide between two ways of pretending there is, and that choice is what these two names mean.

**Copy-on-write is telling the table: when I change something, leave it tidy immediately.** Delta
finds the files that contain the matched rows and rewrites them whole, with the change applied,
then commits a `remove` of the old files and an `add` of the new ones. Whoever reads next reads
clean files and never learns that anything happened. This is the default, and it's where the name
comes from: you copy in order to write.

**Merge-on-read is telling it the opposite: when I change something, don't touch the data, just
note it on the side.** Delta writes a **deletion vector** — a small bitmap stored next to the
data file saying "in this file, row positions 7, 40 and 112 are dead" — and commits that alongside
the unchanged file. The write moves almost no bytes. An `UPDATE` becomes a delete plus an insert:
mark the old rows dead, append the new version elsewhere. Now every reader has to load the bitmap
with the file and filter those positions out as it scans.

Nothing got cheaper; the bill moved. Copy-on-write pays at write time and reads clean. Merge-on-read
pays a little on **every** read until you materialize the deletes with `OPTIMIZE` (or
`REORG TABLE … APPLY (PURGE)`, which rewrites the files for real).

**What copy-on-write costs, concretely.** Two things, and it's worth keeping them apart:

- **Write amplification.** To change 10 rows sitting in three 500 MB files, you rewrite 1.5 GB.
  You touched a kilobyte of data and moved a gigabyte and a half.
- **Fragmentation.** Every write leaves new files behind. A `MERGE` running every five minutes from
  a stream leaves its little pile each time, and a file that lost half its rows comes back smaller
  than it was. A month of that is 50,000 files of 2 MB where you wanted 100 of 1 GB.

Fragmentation hurts because **the cost is per file, not per byte**. For every file the engine reads
its entry in the log, opens it, reads the footer statistics and decides whether to skip it. Same
gigabytes either way, but one version is 50,000 errands and the other is 100. The query spends more
time planning than reading.

### The two fixes for small files, which are not the same fix

People say "auto optimize" as if it were one thing. It's two, and they act at different moments:

| | When it acts | What it does |
|---|---|---|
| **Optimized writes** (`delta.autoOptimize.optimizeWrite`) | *Before* writing | Adds a shuffle so the batch lands as a few large files (~128 MB) instead of one per task. Prevention |
| **Auto compaction** (`delta.autoOptimize.autoCompact`) | *After* the commit | Checks whether the write left too many small files and, if so, fires a small `OPTIMIZE` right there, targeting ~128 MB. Cure |

`OPTIMIZE` run manually or on a schedule is a third thing: it targets larger files (~1 GB) and it is
**the only one of the three that can `ZORDER`**. Auto compaction only glues files together, it
doesn't reorder rows, so it never restores clustering — which is why a heavily merged table can have
healthy file sizes and still skip badly. On Unity Catalog managed tables, predictive optimization
can run the scheduled half of this for you (§3).

**Gotcha:** deletion vectors are a table feature, so enabling them bumps the table protocol. Clients
too old to understand the feature can no longer read the table at all — check what else reads it
before turning it on.

**Decision rule:** write rarely and read constantly (a nightly rebuild, a dimension table) →
copy-on-write, the default, and leave it alone. Write constantly in small batches (a streaming
`MERGE` every few minutes) → deletion vectors plus a scheduled `OPTIMIZE` to pay off the debt.
Turn optimized writes on either way; add auto compaction when writes are frequent and small.

### Time travel, history, restore

```sql
SELECT * FROM loads VERSION AS OF 41;
SELECT * FROM loads TIMESTAMP AS OF '2026-09-01 06:00';
DESCRIBE HISTORY loads;              -- who, what operation, rows/files changed per version
RESTORE TABLE loads TO VERSION AS OF 41;   -- a NEW commit that re-adds old files; history is kept
```

### VACUUM

`remove` only hides a file logically. `VACUUM` physically deletes unreferenced files older than the retention period (default **7 days**).

```sql
VACUUM loads DRY RUN;
VACUUM loads;                -- default retention
```
**Gotcha:** VACUUM shortens time travel. Once files are gone, `VERSION AS OF` older versions fails. Retention below 7 days needs a safety check disabled, and it can break long-running readers and streams. Don't do it in prod.

### Change Data Feed

`delta.enableChangeDataFeed = true` records row-level inserts, updates and deletes per version, so downstream jobs read *changes* instead of re-scanning. It's the Delta counterpart of a Snowflake Stream.

**Decision rule:** undo a bad write → `RESTORE`. Audit → `DESCRIBE HISTORY`. Incremental downstream → CDF. Schedule `VACUUM` with a retention of at least your time-travel and stream-lag needs.

---

## 3. Data layout: partition vs Z-order vs liquid clustering

**The problem.** Dispatch queries `WHERE truck_id = ? AND event_date BETWEEN …` on a 20 TB pings table. File skipping only works if files hold *narrow* ranges of the filter columns.

| | Hive-style partitioning | `OPTIMIZE … ZORDER BY` | **Liquid clustering** (`CLUSTER BY`) |
|---|---|---|---|
| Mechanism | One folder per value, exact pruning | Rewrites files so nearby values co-locate, then stats skipping | Incremental clustering on keys, then stats skipping |
| Good columns | Low cardinality (date) | High-cardinality filter columns | Any of your common filter columns |
| Change keys later | Full rewrite | Re-run on everything | `ALTER TABLE … CLUSTER BY (…)`, no rewrite of old data |
| Maintenance | Small-file risk if the column is too fine | Full re-Z-order is expensive | `OPTIMIZE` clusters only what's needed; can be automatic |
| Status | Legacy default | Legacy | **Recommended for new tables** |

```sql
CREATE TABLE pings (...) CLUSTER BY (truck_id, event_date);
OPTIMIZE pings;                       -- incremental clustering + compaction
```

### What Z-ordering actually does

"Co-locates nearby values" is the summary, and it hides the interesting part: how one physical file layout can serve a filter on `truck_id` *or* a filter on `event_date`, when sorting can only ever favour one column.

**Sorting solves one column and abandons the rest.** `ORDER BY truck_id, event_date` writes files where each holds a narrow band of truck ids but spans the *whole* date range. Filter by truck and the engine skips almost everything; filter by date alone and it skips nothing, because every file contains every date. This is the same leftmost-prefix rule that governs composite B-tree indexes in Postgres ([sql-advanced.md §4](sql-advanced.md)): the second column only helps once the first one has been pinned down.

**Z-ordering interleaves the bits of both columns instead.** Take the two values, write them in binary, and build a new number by alternating their bits — one bit from the date, one from the truck id, one from the date, and so on. That number is the row's position on a **Z-order curve** (also called a Morton curve, after the ordering Guy Macdonald Morton published in 1966). Sorting rows by it, then cutting the sorted list into files, gives every file a *moderately* narrow range on **both** columns rather than a perfect range on one.

It's easiest to see on a 4×4 grid. Bucket trucks into 4 groups across the top and dates into 4 groups down the side, then write each cell's Z-value in it:

| | truck 0 | truck 1 | truck 2 | truck 3 |
|---|---|---|---|---|
| **date 0** | 0 | 1 | 4 | 5 |
| **date 1** | 2 | 3 | 6 | 7 |
| **date 2** | 8 | 9 | 12 | 13 |
| **date 3** | 10 | 11 | 14 | 15 |

Follow 0 → 1 → 2 → 3 and you trace a "Z", which is where the name comes from; the pattern then repeats at a larger scale. Now cut that ordering into 4 files of 4 cells each, and every file turns out to be a **quadrant** — file 0 holds cells (0–1, 0–1), file 1 holds (2–3, 0–1), and so on. Each file spans only 2 of the 4 truck buckets *and* only 2 of the 4 date buckets.

That geometry is the whole payoff:

| Query | Sorted by (truck, date) | Z-ordered by (truck, date) |
|---|---|---|
| `WHERE truck_id IN (bucket 2)` | skips 3 of 4 files | skips 2 of 4 files |
| `WHERE event_date IN (bucket 0)` | **skips nothing** | skips 2 of 4 files |

Z-order is worse than sorting for the leading column and far better for every other one. That trade is the reason to reach for it: you accept a mediocre layout for each column in exchange for no column being hopeless. It follows that Z-ordering on a column nobody filters on is pure cost, and that Z-ordering on five columns dilutes all of them — two or three is the practical limit.

The skipping itself is still done by Delta's per-file min/max statistics. Z-order doesn't add a lookup structure; it just arranges the rows so that those min/max ranges are tight enough to be worth consulting. Liquid clustering keeps this idea and swaps the curve: Databricks describes it as using Hilbert curves, which keep neighbours closer together than a Z-curve does, and it applies them incrementally instead of rewriting everything.

### Does it use extra disk?

**No separate structure — it's a rewrite of the data you already have.** This is the sharpest difference from a database index. A non-clustered index in Postgres or SQL Server is an additional copy of the indexed columns kept beside the table, so it permanently adds disk (often 10–30% of the table per index) and makes every write maintain it. Z-ordering adds nothing beside the table: `OPTIMIZE … ZORDER BY` reads the Parquet files, reorders the rows across them, and writes new files. Steady-state size lands close to where it started, usually **slightly smaller**, because co-locating similar values compresses better.

Two temporary costs are real, though, and they're the ones that surprise people:

- **The old files stay until you `VACUUM`.** Delta never deletes in place — the rewritten files are new, and the originals are retained so time travel keeps working. With the default 7-day retention, an `OPTIMIZE` roughly **doubles** the storage of whatever it touched for a week. Z-ordering a 20 TB table means budgeting for 40 TB until `VACUUM` runs.
- **Compute, which is the bigger bill.** The rewrite reads and writes the affected data in full. Because classic Z-ordering isn't incremental, keeping a growing table well-ordered means re-running it over data that was already fine — that's the "full re-Z-order is expensive" row in the table above, and the main thing liquid clustering fixes.

Facts (checked 2026-09, Databricks docs):
- Databricks recommends liquid clustering for new tables.
- It's GA for Delta on DBR 15.4 LTS+.
- It **can't be combined** with partitioning or `ZORDER` on the same table.
- Unity Catalog managed tables can hand clustering and `OPTIMIZE`/`VACUUM` to **predictive optimization**, including automatic key selection.

Databricks' own guidance is to **not partition tables under ~1 TB**, and to keep each partition around ≥1 GB. Partitioning pings by `truck_id` gives 100k folders of tiny files, which is the classic small-file problem.

**Bridge:** liquid clustering ≈ Snowflake automatic clustering ≈ Spark `sortWithinPartitions` before a write. They all narrow min/max ranges per file.

**Decision rule:** new table → `CLUSTER BY` your 1–4 most common filter columns. Existing partitioned table that works → leave it, and migrate when keys need to change or small files hurt. Z-order only on legacy tables you can't migrate yet.

---

## 4. Compute and cost

**The problem.** A team runs its nightly 40-minute ETL on the same all-purpose cluster people use for notebooks, with auto-terminate unset. It burns 24 h/day of the most expensive compute type.

**Price model.** `cost = DBUs consumed × $/DBU (by compute type and tier) + cloud VMs` (VMs are included for serverless).

| Compute | For | Relative $/DBU (approx., AWS US, checked 2026-09) |
|---|---|---|
| **Jobs compute** (classic) | Scheduled pipelines. Starts per run, ends after | Lowest (≈ $0.15) |
| **SQL warehouse**: Classic / Pro / Serverless | BI, dbt-on-Databricks, SQL | ≈ $0.22 / ≈ $0.55 / ≈ $0.70 (serverless includes VMs) |
| **All-purpose** | Interactive notebooks, shared dev | High (≈ $0.40–0.55) + VMs |
| **Serverless jobs / notebooks** | No cluster management, fast start | Higher per DBU, VMs included |

Official pricing pages render prices by region and tier dynamically, so these are third-party 2026 figures. **Remember the ordering, not the numbers.**

**Decision rule:**
- **Scheduled ETL** → jobs compute, or serverless jobs if startup time and ops matter more than $/DBU.
- **Exploration** → all-purpose with auto-terminate at 30–60 min.
- **BI and dashboards** → serverless SQL warehouse for bursty use (it stops quickly), Pro for steady all-day load.
- **Streaming or declarative pipelines** → a pipelines-managed cluster (§5).

**Cost levers, in order of impact:**
1. Jobs compute instead of all-purpose for anything scheduled.
2. Auto-terminate and auto-stop everywhere.
3. Spot instances for workers, with an on-demand driver.
4. Fix layout (§3) and shuffles ([pyspark.md](pyspark.md)) so jobs finish sooner. Photon helps scan-heavy SQL, but its DBU rate is higher, so measure.
5. Incremental processing (CDF, streaming tables) instead of full rebuilds.

---

## 5. Pipelines: Structured Streaming, Auto Loader, Lakeflow Declarative Pipelines

- **Structured Streaming.** Spark's stream engine treats a stream as an unbounded table processed in micro-batches. It gets exactly-once into Delta through checkpoints plus transactional commits. Event time, watermarks and delivery semantics are explained in [streaming-tools.md](../system_design/99-reference/streaming-tools.md).
- **Auto Loader** (`cloudFiles`) incrementally picks up new files landing in S3 and tracks which ones it has seen, so you don't list the bucket every run.
- **Lakeflow Declarative Pipelines**, formerly **Delta Live Tables (DLT)** and renamed in 2025 (checked 2026-09, Databricks docs). You declare streaming tables and materialized views. The framework handles dependencies, retries, infrastructure and data-quality **expectations**. Existing DLT code keeps working. The open-source counterpart is Spark Declarative Pipelines.

```python
from pyspark import pipelines as dp          # older code: import dlt

@dp.table
@dp.expect_or_drop("valid_truck", "truck_id IS NOT NULL")
def pings_clean():
    return spark.readStream.table("pings_raw")
```

### The trigger: how often the job wakes up

**The trigger is telling the job how often to wake up and ask "anything new?"** A stream doesn't
flow continuously. Structured Streaming wakes on a schedule, claims whatever has arrived since last
time, processes it as one ordinary Spark job, commits, and goes back to sleep. A "stream" here is a
fast loop of small batches, which is why the unit is called a **micro-batch**.

```python
.trigger(processingTime="5 minutes")   # wake on a clock
.trigger(availableNow=True)            # take everything new, then stop
# no trigger at all:                     the next batch starts as soon as the last one ends
```

`availableNow` is the one that turns a stream into a batch layer: the same streaming code, run on a
schedule like any job. It keeps its checkpoint, so each run picks up exactly where the last one
stopped — that's what makes a "batch" job incremental instead of a full rescan.

The chain worth being able to recite: **shorter trigger → fresher data → more micro-batches → more
small files → more compaction work and a longer commit log.** A 10-second trigger on a low-volume
topic is 8,640 batches a day, each one a Delta commit and a handful of tiny files, for data almost
nobody is reading that fast.

And the freshness you can promise is never the trigger on its own. It's a **sum**: trigger interval
+ batch processing time + watermark delay + whatever cadence silver and gold run on. Bronze being
30 seconds fresh means nothing if silver runs every 15 minutes.

Finally, size the batch and not only the clock. `maxFilesPerTrigger` and `maxBytesPerTrigger`
(Auto Loader) and `maxOffsetsPerTrigger` (Kafka) cap how much one micro-batch may swallow. Without
them, the first run after a weekend of downtime tries to eat the entire backlog in a single batch
and dies on memory — the caps are what turn a backlog into a few hundred normal batches instead.

### What's inside a checkpoint

**The checkpoint is telling the job where to leave its bookmark**, so that a restart neither starts
over nor skips anything. It's a directory, and knowing its four folders is most of the answer:

```
_checkpoints/bronze_pings/
├── offsets/   ← batch N: the input range we are ABOUT to process
├── commits/   ← batch N: finished successfully
├── sources/   ← source state (Auto Loader: which files it has already seen)
└── state/     ← running aggregations, dedup keys, join buffers
```

The protocol is a write-ahead log. *Before* processing batch N, Spark writes `offsets/N` with the
exact input range. It processes. Only *then* does it write `commits/N`. On restart, an `offsets/N`
with no matching `commits/N` means that batch died halfway, so Spark replays **that same recorded
range** — not "whatever is in the source now". The determinism is the whole trick: the retry reads
the same input and therefore produces the same output files.

That's half of exactly-once. The other half is the sink: the Delta commit carries a
`SetTransaction` action with `(appId, batchId)`, so re-committing batch 47 is a no-op rather than a
duplicate. Neither half works alone — a checkpoint in front of an append-only non-transactional
sink still duplicates.

Five things that bite:

- **Deleting it isn't a reset, it's a full reprocess.** The job restarts from `startingOffsets`, or
  for Auto Loader re-ingests every file in the bucket. With an idempotent `MERGE` downstream that's
  merely expensive; with an append, you've duplicated the table.
- **One checkpoint per query, never shared.** Two streams pointed at the same directory corrupt each
  other's state. Writing one source to two sinks means two checkpoints.
- **Not every code change survives a restart.** Adding a filter or a projected column is fine.
  Changing the source, adding or removing a stateful operator, changing the output mode or the keys
  of a streaming aggregation is not — the restart fails and you start from scratch.
- **On Kafka, the offsets that count are Spark's, not the consumer group's.** Structured Streaming
  keeps them in its own checkpoint and doesn't commit them back to the group, so Kafka-side lag
  tooling won't show where your job actually is. Easy trap when debugging.
- **Without a watermark, `state/` grows forever.** The watermark is what authorizes dropping old
  state, so a streaming aggregation or a `dropDuplicates` with no watermark leaks until the job
  dies. See [streaming-tools.md §6](../system_design/99-reference/streaming-tools.md).

One operational consequence: `VACUUM` retention must exceed your longest stream downtime, or a
restarting job fails looking for files its checkpoint still expects.

**Decision rule:** a few custom streaming jobs → Structured Streaming on jobs compute. Many bronze→silver→gold tables with quality rules → Declarative Pipelines. Files arriving in S3 → Auto Loader as the source in both cases. Trigger on the clock for a live table, `availableNow` on a schedule for a batch layer, and treat the checkpoint directory as part of the table, not as scratch.

---

## 6. Governance: Unity Catalog and Delta Sharing

### Unity Catalog

**The problem.** Five workspaces, each with its own Hive metastore and its own grants. Nobody can answer "who can read carrier bank details?"

Unity Catalog is one metastore per region, attached to workspaces:
- **A three-level namespace:** `catalog.schema.table`.
- **Ownership plus privileges.** Object owners and principals with **`MANAGE`** can grant on an object. Admin roles are account admin, metastore admin and workspace admin. There is no separate "data steward" construct.
- **Fine-grained access** through SQL UDFs.
- **Lineage** (table and column level), captured automatically for queries run through UC and queryable in system tables such as `system.access.table_lineage`. Reads of raw paths that bypass the catalog aren't captured.

```sql
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.billing TO `analysts`;

-- Row filter: carriers see only their own invoices
CREATE FUNCTION prod.billing.carrier_filter(cid INT)
  RETURN is_account_group_member('finance') OR cid = current_carrier_id();  -- current_carrier_id(): your own UDF
ALTER TABLE prod.billing.invoices SET ROW FILTER prod.billing.carrier_filter ON (carrier_id);

-- Column mask: hide bank account except for finance
CREATE FUNCTION prod.billing.mask_iban(v STRING)
  RETURN CASE WHEN is_account_group_member('finance') THEN v ELSE '****' END;
ALTER TABLE prod.billing.carriers ALTER COLUMN iban SET MASK prod.billing.mask_iban;
```

### Delta Sharing

An **open protocol** for sharing live tables. The provider grants a *share* to a *recipient*. The recipient reads the provider's files through short-lived pre-signed URLs, with **no copy and no ETL**. Recipients can be another Databricks workspace (the share shows up as a catalog) or open clients: pandas, Spark, Power BI.

```sql
CREATE SHARE carrier_scorecards;
ALTER SHARE carrier_scorecards ADD TABLE prod.gold.carrier_monthly;
CREATE RECIPIENT acme_logistics;      -- non-Databricks recipients get an activation link
GRANT SELECT ON SHARE carrier_scorecards TO RECIPIENT acme_logistics;
```

**Decision rule:** a partner needs always-fresh tables, revocable and audited → Delta Sharing, not nightly CSV exports to S3. Internal access control → UC grants, with row filters and masks instead of per-audience copied views.

**Also worth naming:** **Photon** is a C++ vectorized engine behind the same APIs. It's on by default in SQL warehouses and speeds scan and aggregation-heavy SQL. **MLflow** handles experiment tracking, model registry and serving.

---

## 7. Failure modes

| Failure | Shows up as | Mitigate |
|---|---|---|
| Small files | Slow planning, thousands of tiny files per table | Optimized writes, auto compaction, `OPTIMIZE`, predictive optimization; never partition by high cardinality |
| Deletion vectors never materialized | Reads get slower week by week on a table whose writes got faster | Schedule `OPTIMIZE`; `REORG TABLE … APPLY (PURGE)` to rewrite files for real |
| Checkpoint deleted "to clean up" | The next run reprocesses the whole source; duplicates if any layer appends | Treat the checkpoint as part of the table; idempotent `MERGE` downstream so a replay is only expensive |
| Micro-batch too big after downtime | First run post-outage OOMs or never finishes | `maxFilesPerTrigger` / `maxBytesPerTrigger` / `maxOffsetsPerTrigger` |
| Concurrent write conflict | `ConcurrentAppendException` on MERGE | Narrow the MERGE condition to the target partition or cluster range; serialize writers per table |
| VACUUM too aggressive | Time travel or a restarted stream fails with missing files | Retention ≥ longest time-travel and stream-downtime need |
| Idle all-purpose cluster | Monthly bill spike with no job growth | Cluster policies enforcing auto-terminate; jobs compute for schedules |
| Lineage gaps | A table missing from impact analysis | Read and write through UC table names, not raw `s3://` paths |

---

## 8. Databricks vs Snowflake (a fair comparison)

Both have converged a lot. Interviewers want to hear *why you'd pick one for this team*, not a feature war.

| | Databricks | Snowflake |
|---|---|---|
| Heritage | Spark and ETL/ML first, grew into SQL warehousing | SQL warehouse first, grew into Python (Snowpark) and ML |
| Storage | Delta (open) in your bucket; Iceberg readers via UniForm | Managed native format, or **Iceberg tables** in your bucket |
| Streaming | Structured Streaming, Declarative Pipelines: arbitrary stateful logic | **Snowpipe Streaming** (row-level ingest, seconds), Streams + Tasks, **Dynamic Tables** (declarative, target lag). Less suited to custom stateful stream processing |
| Knobs | More (clusters, runtimes, layout), reduced by serverless and predictive optimization | Fewer (warehouse size, multi-cluster, clustering key) |
| Governance | Unity Catalog | Horizon |
| dbt | Supported | Most common pairing |

**Decision rule:**
- **Snowflake** → a SQL-and-dbt team with data already in a warehouse, wanting the fewest knobs.
- **Databricks** → heavy Python/Spark transformations, ML on the same data, lake files as the system of record, or custom streaming.
- **Either** → an open-format requirement (Delta or Iceberg). Choose on team skills and existing platform.

---

## Common wrong answers

- **"Z-order the table and partition by `truck_id`."** High-cardinality partitioning creates the small-file problem. New tables use liquid clustering, which can't be combined with partitions or Z-order anyway.
- **"Snowflake is proprietary and can't stream."** It has Iceberg tables, Snowpipe Streaming and Dynamic Tables. The honest gap is custom stateful stream processing, not ingestion.
- **"Run VACUUM with 0 hours to save storage."** That destroys time travel and breaks running readers and streams. The savings are rarely worth it.
- **"Serverless is always more expensive."** Per DBU, yes. For bursty workloads with VMs included and no idle time, the total is often lower.
- **"DLT is deprecated."** It was renamed to Lakeflow Declarative Pipelines, and existing code still runs.

---

## Self-check

<details><summary><b>Q1.</b> A Spark job writing to plain Parquet dies halfway. What do readers see, and what does Delta change mechanically?</summary>

With plain Parquet, readers see whatever files landed: a partial, inconsistent day. With Delta, new files are invisible until the job atomically writes the next `_delta_log` JSON. No commit means readers still see the previous version, and the orphan files are cleaned later by `VACUUM`.

</details>

<details><summary><b>Q2.</b> New 20 TB pings table, queried by <code>truck_id</code> and date range. Partition, Z-order, or liquid clustering? Why?</summary>

Liquid clustering: `CLUSTER BY (truck_id, event_date)`. It narrows min/max ranges per file so stats-based skipping works on both columns, and keys can change later without a rewrite. Partitioning by `truck_id` would create 100k small-file folders. Z-order is the legacy approach and can't be mixed with liquid clustering.

</details>

<details><summary><b>Q3.</b> Someone ran a bad MERGE on <code>invoices</code> an hour ago. How do you recover, and what could make recovery impossible?</summary>

Use `DESCRIBE HISTORY invoices` to find the version before the MERGE, then `RESTORE TABLE invoices TO VERSION AS OF n`. That creates a new commit and keeps history. It fails if `VACUUM` already deleted the old files, which happens when retention was set shorter than the gap.

</details>

<details><summary><b>Q4.</b> The Databricks bill doubled with no new pipelines. Where do you look first?</summary>

1. Compute usage by type: scheduled jobs running on all-purpose clusters, or clusters without auto-terminate.
2. SQL warehouses without auto-stop.
3. Jobs that got slower from small files or skew, so the same work burns more DBUs.
4. Photon or serverless switched on for workloads that don't benefit.

Enforce auto-terminate and jobs compute through cluster policies.

</details>

<details><summary><b>Q5.</b> A carrier partner wants daily-fresh scorecards for only its own loads. Design the access.</summary>

Build a gold table per carrier scope, or one table with a row filter. Share it through Delta Sharing to that recipient: live, no copies, revocable and audited. Internally, UC grants plus a row filter on `carrier_id`, with a column mask on bank details.

</details>

<details><summary><b>Q6.</b> Why does Z-ordering on two columns beat sorting by them, and does it cost extra storage?</summary>

Sorting by `(a, b)` gives each file a narrow range of `a` but the full range of `b`, so a filter on `b` alone skips nothing — the leftmost-prefix rule, same as a composite B-tree index. Z-ordering interleaves the bits of both columns (a Morton curve), so each file ends up with a moderately narrow range on *both*: worse than sorting for `a`, far better for `b`. Delta still skips files using per-file min/max stats; Z-order only makes those ranges tight enough to be useful.

Storage: no separate structure, unlike an index. `OPTIMIZE … ZORDER BY` rewrites the Parquet files in place-ish, and the result is usually slightly smaller because co-located values compress better. But the pre-rewrite files are retained for time travel, so the table roughly doubles on disk until `VACUUM` runs (7-day default retention). The real cost is the compute to rewrite, and classic Z-order isn't incremental, which is what liquid clustering fixes.

</details>

---

<details><summary><b>Q7.</b> A silver table takes a streaming <code>MERGE</code> every 5 minutes. Writes are getting slower and queries are getting slower too. What's happening and what do you change?</summary>

**A.** Copy-on-write. Every `MERGE` rewrites whole files to apply a handful of row changes, so writes pay amplification, and each run leaves new small files behind, so reads pay per-file overhead on a table that is now fragmented.

Two independent changes. For the write cost, switch to **deletion vectors** (merge-on-read): the delete half of the merge writes a bitmap instead of rewriting files, and the debt is paid later by `OPTIMIZE`. For the file count, turn on **optimized writes** so each batch lands as a few large files, and **auto compaction** so the small ones get glued together right after the commit. Keep a scheduled `OPTIMIZE` regardless, because auto compaction doesn't reorder rows and therefore never restores clustering.
</details>

<details><summary><b>Q8.</b> What is actually stored in a Structured Streaming checkpoint, and what happens if you delete it?</summary>

**A.** Four things: `offsets/` (the input range each batch was about to process), `commits/` (which batches finished), `sources/` (source state — for Auto Loader, the files already seen), and `state/` (aggregations, dedup keys, join buffers).

The offsets file is written *before* the batch runs and the commit *after*, so a restart that finds an offset with no commit replays that exact recorded range and produces the same output. Delete the directory and it's not a reset, it's a full reprocess: the job restarts from `startingOffsets`, or re-ingests every file in the bucket. With an idempotent `MERGE` downstream that's just expensive; with an append-only sink you've duplicated the table.
</details>

<details><summary><b>Q9.</b> You shorten the trigger interval from 5 minutes to 10 seconds to make a dashboard fresher. What did you actually buy?</summary>

**A.** Probably nothing, and some new problems. Freshness is a sum — trigger interval + batch processing time + watermark delay + the cadence of the layers downstream — so a 10-second bronze is irrelevant if silver still runs every 15 minutes. Meanwhile you went from 288 micro-batches a day to 8,640, each one a Delta commit and a pile of small files, which means more compaction work and a longer log to replay.

Shorten the whole chain or none of it, and if the dashboard genuinely needs seconds, that's an argument for serving it from a streaming sink rather than from the lakehouse tables.
</details>

## Related

- [pyspark.md](pyspark.md): shuffles, skew, broadcast joins, reading plans
- [snowflake-performance.md](snowflake-performance.md): the Snowflake equivalents of layout, pruning and cost
- [sql-advanced.md](sql-advanced.md): CDC extraction from Postgres into the lakehouse
- [streaming-tools.md](../system_design/99-reference/streaming-tools.md): delivery semantics, watermarks
- [cloud/architecture-comparison.md](../cloud/architecture-comparison.md): core toolkit and scale ladder
