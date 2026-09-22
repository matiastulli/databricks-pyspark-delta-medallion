#!/usr/bin/env python3
"""Generates the v001 migration for a bronze table from its source table's real schema.

Adding a bronze source is two steps: add its entry to config/sources.toml, then run this script. It reads the source
table's columns on the SQL warehouse (DESCRIBE TABLE), adds the ingestion metadata columns, and writes
src/00_bronze/ddl/bronze_<name>_v001_create.sql for review.

Usage (after `databricks auth login`, from the repo root):
    .venv/bin/python scripts/new_bronze_migration.py tpch_lineitem            # writes the migration
    .venv/bin/python scripts/new_bronze_migration.py tpch_region --dry-run    # prints it instead
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from medallion.migrations import SRC_DIR, render_bronze_migration  # noqa: E402
from medallion.sources import get_source  # noqa: E402


def load_env() -> None:
    """Reads .env (DATABRICKS_WAREHOUSE_ID etc.) without overriding variables already set in the shell."""
    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


def warehouse_id() -> str:
    if os.environ.get("DATABRICKS_WAREHOUSE_ID"):
        return os.environ["DATABRICKS_WAREHOUSE_ID"]
    warehouses = json.loads(subprocess.run(["databricks", "warehouses", "list", "-o", "json"], capture_output=True, text=True, check=True).stdout)
    return warehouses[0]["id"]


def source_columns(table: str) -> list[tuple[str, str]]:
    """Columns of `table`, in order, from DESCRIBE TABLE on the SQL warehouse."""
    payload = json.dumps({"warehouse_id": warehouse_id(), "statement": f"DESCRIBE TABLE {table}", "wait_timeout": "50s"})
    response = json.loads(subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--json", payload], capture_output=True, text=True, check=True).stdout)
    if response["status"]["state"] != "SUCCEEDED":
        sys.exit(f"DESCRIBE TABLE {table} failed: {response['status'].get('error', {}).get('message')}")
    columns = []
    for name, data_type, _comment in response["result"]["data_array"]:
        if not name or name.startswith("#"):  # partition and metadata sections follow the columns
            break
        columns.append((name, data_type))
    return columns


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("source", help="bronze table name of a config/sources.toml entry, e.g. tpch_lineitem")
    parser.add_argument("--dry-run", action="store_true", help="print the migration instead of writing it")
    args = parser.parse_args()

    load_env()
    source = get_source(args.source)
    path, sql = render_bronze_migration(source.name, source.table, source.mode, source_columns(source.table))

    if args.dry_run:
        print(f"-- {path}\n{sql}", end="")
        return
    target = SRC_DIR / path
    if target.exists():
        sys.exit(f"{target.relative_to(REPO_ROOT)} already exists; change an existing table with a new alter_..._v<NNN>.sql instead")
    target.write_text(sql)
    print(f"wrote {target.relative_to(REPO_ROOT)}; review it, then `databricks bundle run apply_ddl`")


if __name__ == "__main__":
    main()
