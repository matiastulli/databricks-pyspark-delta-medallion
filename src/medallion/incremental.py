"""Incremental processing from a Delta change feed: what to read, and which keys a run has to rebuild."""

from pyspark.sql import DataFrame

# Rows the change feed reports. A deleted row (and the "before" image of an update) matters as much as an inserted
# one: its group has to be recomputed, or the aggregate keeps counting rows that are gone.
CHANGE_TYPES = ("insert", "update_preimage", "update_postimage", "delete")


def plan_run(last_processed_version: int | None, current_version: int) -> dict:
    """Decides what a run should read from a table's change feed.

    - no watermark yet -> a full rebuild, because the change feed only records versions after it was enabled
    - watermark == current version -> nothing changed, so there is nothing to do
    - otherwise -> the versions after the watermark, up to the current one
    """
    if last_processed_version is None:
        return {"mode": "full", "start_version": None, "end_version": current_version}
    if last_processed_version >= current_version:
        return {"mode": "none", "start_version": None, "end_version": current_version}
    return {"mode": "incremental", "start_version": last_processed_version + 1, "end_version": current_version}


def changed_keys(changes: DataFrame, key_column: str) -> list:
    """The distinct values of `key_column` touched by the change feed, whatever kind of change it was."""
    return sorted(row[key_column] for row in changes.select(key_column).distinct().collect() if row[key_column] is not None)
