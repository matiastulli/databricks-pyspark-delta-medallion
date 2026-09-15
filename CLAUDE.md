# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

A **learning-path portfolio project**: a medallion lakehouse on Databricks built with **PySpark + Delta Lake only**. No dbt and no Terraform: the user chose those tools for a separate repo (`dbt-terraform-postgres-medallion`). The user is a software engineer with Databricks experience from a company-specific framework, and is refreshing the fundamentals in a public repo.

**Build it one step at a time.** Never scaffold later steps ahead of time; each step adds only what it needs, with explanations. The user prefers easy-going example data over business rules.

Learning path (tick them off in `README.md` as they land):
0. Local environment: PySpark + Delta on the laptop, done first because the Databricks site was down during setup
1. Setup: Databricks Free Edition, Databricks CLI auth, Unity Catalog catalog + `bronze`/`silver`/`gold` schemas
2. Bronze: PySpark ingestion of `samples.nyctaxi.trips` into Delta, with ingestion metadata
3. Silver: cleaning/typing, idempotent Delta `MERGE`
4. Gold: aggregates (daily trips/revenue, busiest zones) + data quality checks that fail the run
5. Orchestration: Databricks Asset Bundle (`databricks.yml`) with a bronze → silver → gold job
6. Tests + CI: transformations as pure functions in `src/`, pytest on local PySpark, GitHub Actions

## Environment

- Databricks **Free Edition**: serverless compute only, with usage limits. If a step doesn't fit, adapt the step rather than suggest a paid plan.
- Databricks CLI (Homebrew). Authenticate with `databricks auth login` (browser OAuth). **Never ask for or handle personal access tokens in chat.**
- Local: Python 3.11 and Java 17 (Homebrew `openjdk@17`). Versions are pinned in `requirements.txt` (PySpark 4.2.0 + delta-spark 4.4.0) and were verified together locally. The Databricks serverless runtime may differ, so align the pins once the workspace exists.

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

A local Delta-enabled session needs `configure_spark_with_delta_pip(builder)` plus the two Delta configs: `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` and `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`. The first session downloads the Delta jars from Maven into `~/.ivy2.5.2`, which takes about a minute. pip lists the package as `delta_spark`.

## Working agreements

- The repo is **public**. Confirm with the user before pushing, and keep secrets and workspace-specific IDs out of committed files.
- The user's GitHub profile README (`~/Code/matiastulli`) lists this project. Update its entry as the project progresses, and pull before editing because the user also edits it on the web.
- Add commands to this file as each step introduces them. Don't document commands that don't exist yet.
