"""Gold logic: the trip scorecard aggregates, built from silver trips."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def daily_trips(silver: DataFrame) -> DataFrame:
    """One row per pickup date: trips, revenue, and average distance, fare and duration."""
    return (
        silver.groupBy("pickup_date")
        .agg(
            F.count("*").alias("trips"),
            F.sum("fare_amount").alias("revenue"),
            F.round(F.avg("trip_distance_miles"), 2).alias("avg_distance_miles"),
            F.avg("fare_amount").cast("decimal(10,2)").alias("avg_fare"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        )
        .orderBy("pickup_date")
    )


def busiest_pickup_zones(silver: DataFrame) -> DataFrame:
    """One row per pickup ZIP, ranked by trips. dense_rank: ZIPs with the same number of trips share a rank."""
    zone_totals = silver.groupBy("pickup_zip").agg(
        F.count("*").alias("trips"),
        F.sum("fare_amount").alias("revenue"),
        F.avg("fare_amount").cast("decimal(10,2)").alias("avg_fare"),
    )
    return (
        zone_totals.withColumn("rank", F.dense_rank().over(Window.orderBy(F.desc("trips"))))
        .select("rank", "pickup_zip", "trips", "revenue", "avg_fare")
        .orderBy("rank", "pickup_zip")
    )
