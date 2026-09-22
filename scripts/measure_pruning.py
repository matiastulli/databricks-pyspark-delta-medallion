#!/usr/bin/env python3
"""Runs a query on the SQL warehouse and reports how many files it read and pruned.

File skipping is the whole point of a data layout (clustering, Z-order, partitioning), so claims about it should be
measured. Databricks reports the numbers per query in query history.

Usage (after `databricks auth login`, from the repo root):
    .venv/bin/python scripts/measure_pruning.py "SELECT count(*) FROM medallion.\\`00_bronze\\`.tpch_orders WHERE o_orderdate = DATE'1996-01-02'"
"""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_env() -> None:
    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


def databricks(*args: str) -> dict:
    return json.loads(subprocess.run(["databricks", *args], capture_output=True, text=True, check=True).stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("query", help="the SQL to run and measure")
    args = parser.parse_args()

    load_env()
    warehouse = os.environ.get("DATABRICKS_WAREHOUSE_ID") or databricks("warehouses", "list", "-o", "json")[0]["id"]

    # Comments don't defeat the result cache (Databricks normalizes them away), so a repeated identical query comes
    # back cached, reading 0 files and proving nothing. The script reports "from cache"; when comparing before and
    # after a layout change, vary a harmless predicate, e.g. `AND o_orderkey <> <a number not in the data>`.
    statement = f"-- run {uuid.uuid4().hex}\n{args.query}"
    response = databricks("api", "post", "/api/2.0/sql/statements", "--json", json.dumps({"warehouse_id": warehouse, "statement": statement, "wait_timeout": "50s"}))
    if response["status"]["state"] != "SUCCEEDED":
        sys.exit(f"query failed: {response['status'].get('error', {}).get('message')}")
    print("result:", (response.get("result") or {}).get("data_array"))

    # Query history redacts query_text, so the run is found by its id: statement_id == query_id. History lags a little.
    statement_id = response["statement_id"]
    for _ in range(10):
        history = databricks("api", "get", "/api/2.0/sql/history/queries?max_results=20&include_metrics=true")
        match = next((q for q in history.get("res", []) if q.get("query_id") == statement_id), None)
        if match and match.get("metrics", {}).get("read_files_count") is not None:
            metrics = match["metrics"]
            read, pruned = metrics.get("read_files_count", 0), metrics.get("pruned_files_count", 0)
            total = read + pruned
            print(f"files read {read} of {total} ({pruned} pruned)")
            print(f"bytes read {metrics.get('read_bytes', 0) / 1e6:.1f} MB, pruned {metrics.get('pruned_bytes', 0) / 1e6:.1f} MB")
            print(f"rows read {metrics.get('rows_read_count', 0):,}, produced {metrics.get('rows_produced_count', 0):,}")
            print(f"duration {match.get('duration')} ms (execution {metrics.get('execution_time_ms')} ms), from cache: {metrics.get('result_from_cache')}")
            return
        time.sleep(3)
    sys.exit("query ran but its metrics never appeared in query history")


if __name__ == "__main__":
    main()
