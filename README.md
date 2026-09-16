# databricks-pyspark-delta-medallion

A medallion lakehouse on **Databricks**, built with **PySpark** and **Delta Lake**, orchestrated with **Databricks Asset Bundles** and tested with **pytest**.

> 🚧 **Built step by step as a learning path.** Each step below is added in its own commits, so the history shows how the lakehouse grows.

The data is the [NYC taxi trips](https://docs.databricks.com/aws/en/discover/databricks-datasets) sample that ships with every Databricks workspace (`samples.nyctaxi.trips`). It's simple, well-known data, so the focus stays on the engineering rather than on business rules.

## Learning path

- [x] **0. Local environment:** PySpark + Delta Lake on a laptop (Java 17), with a local Delta `MERGE` checked end to end
- [x] **1. Setup:** Databricks Free Edition, CLI authentication, Unity Catalog catalog with `00_bronze` / `01_silver` / `02_gold` schemas
- [ ] **2. Bronze:** ingest raw trips into Delta with PySpark, adding ingestion metadata
- [ ] **3. Silver:** clean and type the data, with an idempotent Delta `MERGE`
- [ ] **4. Gold:** daily trips and revenue, busiest pickup zones, and data quality checks that fail the run
- [ ] **5. Orchestration:** a Databricks Asset Bundle job running bronze → silver → gold
- [ ] **6. Tests + CI:** transformations as pure functions, unit-tested with pytest, run by GitHub Actions

## Getting started

```sh
cp .env.example .env             # then set DATABRICKS_HOST to your workspace URL
set -a; source .env; set +a
databricks auth login --host "$DATABRICKS_HOST"
scripts/setup_unity_catalog.sh   # creates the medallion catalog and its 00_bronze / 01_silver / 02_gold schemas
```

On Databricks Free Edition, `databricks catalogs create` fails because the metastore has no storage root. The script creates the catalog with SQL on the serverless warehouse instead, which puts it on Default Storage.

## Related

- [dbt-terraform-postgres-medallion](https://github.com/matiastulli/dbt-terraform-postgres-medallion): the same medallion idea with dbt, Terraform and PostgreSQL.
