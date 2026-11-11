from datetime import datetime, timedelta

from medallion.observability import CHECK_RESULT_SCHEMA, check_result_rows, freshness_check, quarantine_rate_check

NOW = datetime(2026, 11, 10, 12, 0)


def test_a_table_ingested_within_its_max_age_is_fresh():
    result = freshness_check("nyctaxi_trips", NOW - timedelta(hours=25), NOW, max_age_hours=26)

    assert result["passed"]
    assert result["actual"] == "25.0h"
    assert result["expected"] == "<= 26h"


def test_a_table_older_than_its_max_age_is_stale():
    assert not freshness_check("nyctaxi_trips", NOW - timedelta(hours=26, minutes=6), NOW, max_age_hours=26)["passed"]


def test_an_empty_table_is_stale_not_skipped():
    # max(_ingested_at) of an empty table is null: a source that never arrived must fail, not pass by default.
    result = freshness_check("tpch_orders", None, NOW, max_age_hours=26)

    assert not result["passed"]
    assert result["actual"] == "never ingested"


def test_the_quarantine_rate_is_quarantined_over_all_trips():
    result = quarantine_rate_check("trips", valid=21847, quarantined=85, max_rate=0.01)

    assert result["passed"]
    assert result["actual"] == "0.39% (85 of 21932)"
    assert not quarantine_rate_check("trips", valid=98, quarantined=2, max_rate=0.01)["passed"]


def test_no_trips_at_all_fails_the_quarantine_check():
    # 0 of 0 would otherwise read as a perfect rate.
    assert not quarantine_rate_check("trips", valid=0, quarantined=0, max_rate=0.01)["passed"]


def test_result_rows_follow_the_table_schema():
    gold = {"check": "silver is not empty", "passed": True, "actual": "True", "expected": "True"}  # no table: spans several
    fresh = freshness_check("nyctaxi_trips", None, NOW, max_age_hours=26)

    rows = check_result_rows([gold, fresh], job="monitor_pipeline", run_id="")

    assert len(rows[0]) == len(CHECK_RESULT_SCHEMA.split(","))
    assert rows[0] == ("monitor_pipeline", None, "silver is not empty", None, True, "True", "True")
    assert rows[1][3] == "nyctaxi_trips" and rows[1][4] is False
