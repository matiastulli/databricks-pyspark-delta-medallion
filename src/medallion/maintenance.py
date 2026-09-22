"""Table maintenance: which tables to maintain, and what a maintenance run changed.

The SQL itself (OPTIMIZE, REORG, VACUUM) lives in src/ops/maintain_tables.py. What's here is the part worth testing:
picking the tables, and turning two DESCRIBE DETAIL snapshots into a readable before/after.
"""

TEMPORARY_PREFIX = "_tmp_"


def tables_to_maintain(tables_by_schema: dict[str, list[str]]) -> list[str]:
    """Qualified `schema.table` names to maintain, in schema then table order.

    Skips `_tmp_` tables: a process creates and drops those within one run, so maintaining them is pointless and, if
    the run is still going, disruptive.
    """
    return [
        f"{schema}.{table}"
        for schema in sorted(tables_by_schema)
        for table in sorted(tables_by_schema[schema])
        if not table.startswith(TEMPORARY_PREFIX)
    ]


def describe_change(before: dict, after: dict) -> dict:
    """Compares two DESCRIBE DETAIL results (numFiles, sizeInBytes, clusteringColumns) for one table."""
    return {
        "files": [before["numFiles"], after["numFiles"]],
        "size_mb": [round(before["sizeInBytes"] / 1e6, 2), round(after["sizeInBytes"] / 1e6, 2)],
        "files_removed": before["numFiles"] - after["numFiles"],
        "clustering": after.get("clusteringColumns") or [],
    }
