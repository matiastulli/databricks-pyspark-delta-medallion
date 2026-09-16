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
    """Rule name -> condition that is true when a trip breaks the rule.

    The comparisons below are null when a value is missing (`null <= 0` is null, not true), so missing values need
    their own rule; otherwise a trip with a null would pass as valid and break silver's NOT NULL columns.
    """
    return {
        "missing_required_value": F.greatest(*[F.col(c).isNull() for c in SOURCE_COLUMNS]),
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


def to_silver_trips(valid: DataFrame) -> DataFrame:
    """Types and renames valid trips to the silver schema.

    Fares become DECIMAL(10,2) because money shouldn't be a floating-point double, and ZIP codes become 5-character
    strings because they are identifiers, not numbers (int 7002 is really 07002).
    """
    return valid.select(
        "trip_id",
        F.col("tpep_pickup_datetime").alias("pickup_at"),
        F.col("tpep_dropoff_datetime").alias("dropoff_at"),
        F.to_date("tpep_pickup_datetime").alias("pickup_date"),
        F.round((F.unix_seconds("tpep_dropoff_datetime") - F.unix_seconds("tpep_pickup_datetime")) / 60, 2).alias("trip_duration_minutes"),
        F.col("trip_distance").alias("trip_distance_miles"),
        F.col("fare_amount").cast("decimal(10,2)").alias("fare_amount"),
        F.lpad(F.col("pickup_zip").cast("string"), 5, "0").alias("pickup_zip"),
        F.lpad(F.col("dropoff_zip").cast("string"), 5, "0").alias("dropoff_zip"),
        "_batch_id",
        "_ingested_at",
        F.current_timestamp().alias("_merged_at"),
    )


def to_quarantine(rejected: DataFrame) -> DataFrame:
    """Keeps rejected trips as received from bronze, plus the reasons they were rejected."""
    return rejected.select(
        "trip_id", "rejection_reasons", *SOURCE_COLUMNS, "_batch_id", "_ingested_at", F.current_timestamp().alias("_merged_at")
    )
