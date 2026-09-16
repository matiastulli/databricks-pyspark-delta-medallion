from datetime import date
from decimal import Decimal

import pytest

from medallion.quality import DataQualityError, gold_checks, raise_if_any_failed

SILVER_SCHEMA = "trip_id string, pickup_date date, fare_amount decimal(10,2), pickup_zip string"
DAILY_SCHEMA = "pickup_date date, trips bigint, revenue decimal(20,2)"
ZONES_SCHEMA = "rank int, pickup_zip string, trips bigint, revenue decimal(20,2)"


@pytest.fixture
def silver(spark):
    return spark.createDataFrame(
        [
            ("a", date(2016, 1, 1), Decimal("10.00"), "10001"),
            ("b", date(2016, 1, 1), Decimal("5.50"), "07002"),
            ("c", date(2016, 1, 2), Decimal("7.25"), "10001"),
        ],
        SILVER_SCHEMA,
    )


@pytest.fixture
def busiest_pickup_zones(spark):
    return spark.createDataFrame([(1, "10001", 2, Decimal("17.25")), (2, "07002", 1, Decimal("5.50"))], ZONES_SCHEMA)


def test_gold_that_matches_silver_passes_every_check(spark, silver, busiest_pickup_zones):
    daily_trips = spark.createDataFrame([(date(2016, 1, 1), 2, Decimal("15.50")), (date(2016, 1, 2), 1, Decimal("7.25"))], DAILY_SCHEMA)

    results = gold_checks(silver, daily_trips, busiest_pickup_zones)

    assert [r["check"] for r in results if not r["passed"]] == []
    raise_if_any_failed(results)


def test_double_counted_trips_fail_reconciliation_and_stop_the_run(spark, silver, busiest_pickup_zones):
    # A bad join or a duplicated day would count trips twice; the totals must no longer match silver.
    daily_trips = spark.createDataFrame(
        [(date(2016, 1, 1), 2, Decimal("15.50")), (date(2016, 1, 1), 2, Decimal("15.50")), (date(2016, 1, 2), 1, Decimal("7.25"))],
        DAILY_SCHEMA,
    )

    results = gold_checks(silver, daily_trips, busiest_pickup_zones)

    assert sorted(r["check"] for r in results if not r["passed"]) == [
        "daily_trips has one row per pickup_date",
        "daily_trips revenue adds up to silver revenue",
        "daily_trips trips add up to silver rows",
    ]
    with pytest.raises(DataQualityError, match="3 of 13 data quality checks failed"):
        raise_if_any_failed(results)
