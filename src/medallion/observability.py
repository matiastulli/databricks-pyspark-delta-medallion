"""Health checks that monitor_pipeline runs, and the rows every check writes to ops.check_results.

A check is a dict like the gold checks in quality.py: {"check", "passed", "actual", "expected"}, plus "table" when it is
about one table. Keeping one shape means one results table, one dashboard and one way to fail a run
(quality.raise_if_any_failed) for every kind of check.
"""

from datetime import datetime

# Column order of ops.check_results without checked_at, which the notebook stamps with current_timestamp().
CHECK_RESULT_SCHEMA = "job string, run_id string, check_name string, table_name string, passed boolean, actual string, expected string"


def freshness_check(table: str, latest_ingested_at: datetime | None, now: datetime, max_age_hours: float) -> dict:
    """Passes when the table's newest row was ingested at most max_age_hours before now.

    An empty table fails: a source that has never arrived is the stalest of all. `now` must come from the same clock and
    time zone as `latest_ingested_at`, so the notebook reads both in one query.
    """
    expected = f"<= {max_age_hours:g}h"
    if latest_ingested_at is None:
        return {"check": f"{table} is fresh", "table": table, "passed": False, "actual": "never ingested", "expected": expected}
    age_hours = (now - latest_ingested_at).total_seconds() / 3600
    return {"check": f"{table} is fresh", "table": table, "passed": age_hours <= max_age_hours, "actual": f"{age_hours:.1f}h", "expected": expected}


def quarantine_rate_check(table: str, valid: int, quarantined: int, max_rate: float) -> dict:
    """Passes when quarantined trips are at most max_rate of all distinct trips (valid + quarantined).

    A few rejected rows are normal; a jump means a source changed, or a validation rule rejects what it shouldn't. No
    trips at all fails, because a rate of 0 out of 0 would otherwise look perfect.
    """
    total = valid + quarantined
    expected = f"<= {max_rate:.2%}"
    if total == 0:
        return {"check": f"{table} quarantine rate", "table": table, "passed": False, "actual": "no trips", "expected": expected}
    rate = quarantined / total
    return {"check": f"{table} quarantine rate", "table": table, "passed": rate <= max_rate, "actual": f"{rate:.2%} ({quarantined} of {total})", "expected": expected}


def check_result_rows(results: list[dict], job: str, run_id: str | None) -> list[tuple]:
    """One row per check, in CHECK_RESULT_SCHEMA order. `run_id` is None outside a job (an interactive run)."""
    return [(job, run_id or None, r["check"], r.get("table"), bool(r["passed"]), r["actual"], r["expected"]) for r in results]
