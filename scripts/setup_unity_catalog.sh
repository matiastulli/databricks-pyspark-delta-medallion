#!/usr/bin/env bash
# Creates the Unity Catalog catalog for the lakehouse. Idempotent (IF NOT EXISTS), so it is safe to run again.
# Everything inside the catalog (schemas, tables) is created by DDL migrations: databricks bundle run apply_ddl.
#
# Runs SQL on the workspace's serverless SQL warehouse through the Statement Execution API, because
# on Free Edition `databricks catalogs create` fails: the metastore has no storage root, and only
# SQL (or the UI) can create a catalog on Default Storage.
#
# Usage: scripts/setup_unity_catalog.sh   (after `databricks auth login`)
#        CATALOG=other_catalog scripts/setup_unity_catalog.sh
# Settings come from .env when it exists (see .env.example); the defaults below apply otherwise.
set -euo pipefail

ENV_FILE="$(dirname "$0")/../.env"
if [[ -f "$ENV_FILE" ]]; then
  # Keep a CATALOG given on the command line over the one in .env.
  CATALOG_OVERRIDE="${CATALOG:-}"
  set -a; source "$ENV_FILE"; set +a
  CATALOG="${CATALOG_OVERRIDE:-${CATALOG:-}}"
fi

CATALOG="${CATALOG:-medallion}"

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

# Escaped backticks (\`) quote the SQL identifier without bash treating them as command substitution.
run_sql "CREATE CATALOG IF NOT EXISTS \`${CATALOG}\` COMMENT 'Medallion lakehouse on NYC taxi trips and TPC-H (PySpark + Delta)'"
