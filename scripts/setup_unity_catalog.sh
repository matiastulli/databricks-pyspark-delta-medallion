#!/usr/bin/env bash
# Creates the Unity Catalog objects for the lakehouse: one catalog, one schema per medallion layer.
# Idempotent (IF NOT EXISTS), so it is safe to run again.
#
# Runs SQL on the workspace's serverless SQL warehouse through the Statement Execution API, because
# on Free Edition `databricks catalogs create` fails: the metastore has no storage root, and only
# SQL (or the UI) can create a catalog on Default Storage.
#
# The schema names are prefixed with their layer order (00_bronze, 01_silver, 02_gold) so they sort
# in pipeline order. They start with a digit, so the SQL quotes them with backticks.
#
# Usage: scripts/setup_unity_catalog.sh   (after `databricks auth login`)
# Settings come from .env when it exists (see .env.example); the defaults below apply otherwise.
set -euo pipefail

ENV_FILE="$(dirname "$0")/../.env"
if [[ -f "$ENV_FILE" ]]; then
  set -a; source "$ENV_FILE"; set +a
fi

CATALOG="${CATALOG:-medallion}"
BRONZE_SCHEMA="${BRONZE_SCHEMA:-00_bronze}"
SILVER_SCHEMA="${SILVER_SCHEMA:-01_silver}"
GOLD_SCHEMA="${GOLD_SCHEMA:-02_gold}"

# Free Edition ships a single "Serverless Starter Warehouse", so fall back to the first one listed.
WAREHOUSE_ID="${DATABRICKS_WAREHOUSE_ID:-}"
if [[ -z "$WAREHOUSE_ID" ]]; then
  WAREHOUSE_ID=$(databricks warehouses list -o json | python3 -c 'import json, sys; print(json.load(sys.stdin)[0]["id"])')
fi

run_sql() {
  echo "-> $1"
  payload=$(python3 -c 'import json, sys; print(json.dumps({"warehouse_id": sys.argv[1], "statement": sys.argv[2], "wait_timeout": "50s"}))' "$WAREHOUSE_ID" "$1")
  databricks api post /api/2.0/sql/statements --json "$payload" \
    | python3 -c 'import json, sys; s = json.load(sys.stdin)["status"]; print("   ", s["state"], s.get("error", {}).get("message", "")); sys.exit(s["state"] != "SUCCEEDED")'
}

# Escaped backticks (\`) quote the SQL identifiers without bash treating them as command substitution.
run_sql "CREATE CATALOG IF NOT EXISTS \`${CATALOG}\` COMMENT 'Medallion lakehouse on NYC taxi trips (PySpark + Delta)'"
run_sql "CREATE SCHEMA IF NOT EXISTS \`${CATALOG}\`.\`${BRONZE_SCHEMA}\` COMMENT 'Raw trips as ingested, plus ingestion metadata'"
run_sql "CREATE SCHEMA IF NOT EXISTS \`${CATALOG}\`.\`${SILVER_SCHEMA}\` COMMENT 'Cleaned, typed, deduplicated trips'"
run_sql "CREATE SCHEMA IF NOT EXISTS \`${CATALOG}\`.\`${GOLD_SCHEMA}\` COMMENT 'Aggregates ready for analysis'"
