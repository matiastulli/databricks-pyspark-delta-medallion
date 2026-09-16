"""Silver logic: the trip key, deduplication, and validation that splits valid trips from quarantined ones."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

SOURCE_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip"]
TIMESTAMP_COLUMNS = {"tpep_pickup_datetime", "tpep_dropoff_datetime"}


def add_trip_id(trips: DataFrame) -> DataFrame:
    """Adds `trip_id`, a SHA-256 of the six source columns.

    The source has no ID, so the content is the identity: the same trip loaded in different batches gets the
    same key. Timestamps are hashed as epoch microseconds, not strings, because a timestamp's string form
    depends on the session time zone and the key must not.
    """

    def key_part(column_name: str) -> Column:
        column = F.col(column_name)
        if column_name in TIMESTAMP_COLUMNS:
            column = F.unix_micros(column)
        return F.coalesce(column.cast("string"), F.lit("null"))

    return trips.withColumn("trip_id", F.sha2(F.concat_ws("|", *[key_part(c) for c in SOURCE_COLUMNS]), 256))


def keep_first_load(trips: DataFrame) -> DataFrame:
    """Keeps one row per `trip_id`: the earliest `_ingested_at`."""
    first_load = Window.partitionBy("trip_id").orderBy("_ingested_at")
    return trips.withColumn("_load_order", F.row_number().over(first_load)).where("_load_order = 1").drop("_load_order")


def validation_rules() -> dict[str, Column]:
    """Rule name -> condition that is true when a trip breaks the rule."""
    return {
        "non_positive_distance": F.col("trip_distance") <= 0,
        "non_positive_fare": F.col("fare_amount") <= 0,
        "dropoff_not_after_pickup": F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"),
    }


def split_valid_and_rejected(trips: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Adds `rejection_reasons` (the names of the rules a trip breaks) and splits on it.

    Returns (valid, rejected): a trip with no reasons is valid, any other trip is rejected, so every trip lands
    in exactly one of the two.
    """
    reasons = F.array(*[F.when(condition, F.lit(name)) for name, condition in validation_rules().items()])
    flagged = trips.withColumn("rejection_reasons", F.filter(reasons, lambda reason: reason.isNotNull()))
    return flagged.where(F.size("rejection_reasons") == 0), flagged.where(F.size("rejection_reasons") > 0)
