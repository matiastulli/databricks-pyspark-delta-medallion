from datetime import date
from decimal import Decimal

from medallion.gold import busiest_pickup_zones, daily_trips

SILVER_SCHEMA = "pickup_date date, trip_distance_miles double, fare_amount decimal(10,2), trip_duration_minutes double, pickup_zip string"


def test_daily_trips_sums_and_averages_per_pickup_date(spark):
    silver = spark.createDataFrame(
        [
            (date(2016, 1, 1), 1.0, Decimal("10.00"), 10.0, "10001"),
            (date(2016, 1, 1), 2.0, Decimal("5.50"), 20.0, "07002"),
            (date(2016, 1, 2), 3.0, Decimal("7.25"), 30.0, "10001"),
        ],
        SILVER_SCHEMA,
    )

    rows = [row.asDict() for row in daily_trips(silver).collect()]

    assert rows == [
        {"pickup_date": date(2016, 1, 1), "trips": 2, "revenue": Decimal("15.50"), "avg_distance_miles": 1.5, "avg_fare": Decimal("7.75"), "avg_duration_minutes": 15.0},
        {"pickup_date": date(2016, 1, 2), "trips": 1, "revenue": Decimal("7.25"), "avg_distance_miles": 3.0, "avg_fare": Decimal("7.25"), "avg_duration_minutes": 30.0},
    ]


def test_zones_with_the_same_trip_count_share_a_rank_and_the_next_rank_has_no_gap(spark):
    silver = spark.createDataFrame(
        [(date(2016, 1, 1), 1.0, Decimal("10.00"), 5.0, zip_code) for zip_code in ["10001", "10001", "10003", "10003", "07002"]],
        SILVER_SCHEMA,
    )

    ranking = [(row["rank"], row.pickup_zip, row.trips) for row in busiest_pickup_zones(silver).collect()]

    assert ranking == [(1, "10001", 2), (1, "10003", 2), (2, "07002", 1)]
