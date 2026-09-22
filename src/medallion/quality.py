"""Gold data quality checks: silver's contract, the shape of each gold table, and reconciliation with silver."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    pass


def gold_checks(silver: DataFrame, daily_trips: DataFrame, busiest_pickup_zones: DataFrame) -> list[dict]:
    """Runs every check and returns one result per check; it never raises, so all failures get reported together."""
    silver_stats = silver.agg(
        F.count("*").alias("rows"),
        F.countDistinct("trip_id").alias("distinct_trip_ids"),
        F.count_if(F.col("trip_id").isNull()).alias("null_trip_ids"),
        F.sum("fare_amount").alias("revenue"),
    ).first()

    daily_stats = daily_trips.agg(
        F.count("*").alias("rows"),
        F.countDistinct("pickup_date").alias("distinct_dates"),
        F.sum("trips").alias("trips"),
        F.sum("revenue").alias("revenue"),
        F.count_if(F.col("revenue") < 0).alias("negative_revenue_days"),
    ).first()

    zone_stats = busiest_pickup_zones.agg(
        F.count("*").alias("rows"),
        F.countDistinct("pickup_zip").alias("distinct_zips"),
        F.sum("trips").alias("trips"),
        F.sum("revenue").alias("revenue"),
        F.count_if(~F.col("pickup_zip").rlike("^[0-9]{5}$")).alias("malformed_zips"),
        F.min("rank").alias("top_rank"),
    ).first()

    # (check name, actual, expected)
    checks = [
        ("silver is not empty", silver_stats["rows"] > 0, True),
        ("silver trip_id is never null", silver_stats["null_trip_ids"], 0),
        ("silver trip_id is unique", silver_stats["distinct_trip_ids"], silver_stats["rows"]),
        ("daily_trips is not empty", daily_stats["rows"] > 0, True),
        ("daily_trips has one row per pickup_date", daily_stats["distinct_dates"], daily_stats["rows"]),
        ("daily_trips has no negative revenue", daily_stats["negative_revenue_days"], 0),
        ("daily_trips trips add up to silver rows", daily_stats["trips"], silver_stats["rows"]),
        ("daily_trips revenue adds up to silver revenue", daily_stats["revenue"], silver_stats["revenue"]),
        ("busiest_pickup_zones has one row per pickup_zip", zone_stats["distinct_zips"], zone_stats["rows"]),
        ("busiest_pickup_zones ZIPs are 5 digits", zone_stats["malformed_zips"], 0),
        ("busiest_pickup_zones ranking starts at 1", zone_stats["top_rank"], 1),
        ("busiest_pickup_zones trips add up to silver rows", zone_stats["trips"], silver_stats["rows"]),
        ("busiest_pickup_zones revenue adds up to silver revenue", zone_stats["revenue"], silver_stats["revenue"]),
    ]
    return [{"check": name, "passed": actual == expected, "actual": str(actual), "expected": str(expected)} for name, actual, expected in checks]


def raise_if_any_failed(results: list[dict]) -> None:
    """Raises DataQualityError listing every failed check."""
    failed = [result for result in results if not result["passed"]]
    if failed:
        raise DataQualityError(
            f"{len(failed)} of {len(results)} data quality checks failed: "
            + "; ".join(f"{r['check']} (got {r['actual']}, expected {r['expected']})" for r in failed)
        )
