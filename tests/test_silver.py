from datetime import date, datetime, timezone
from decimal import Decimal

from medallion.silver import add_trip_id, keep_first_load, split_valid_and_rejected, to_quarantine, to_silver_trips

BRONZE_SCHEMA = (
    "tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, trip_distance double, fare_amount double, "
    "pickup_zip int, dropoff_zip int, _batch_id string, _ingested_at timestamp"
)


def at(hour, minute=0):
    return datetime(2016, 1, 15, hour, minute, tzinfo=timezone.utc)


def trip(pickup=at(10), dropoff=at(10, 15), distance=2.5, fare=11.0, pickup_zip=10001, dropoff_zip=7002, batch="b1", ingested=at(20)):
    return (pickup, dropoff, distance, fare, pickup_zip, dropoff_zip, batch, ingested)


def test_same_trip_gets_the_same_trip_id_across_batches_and_a_different_trip_does_not(spark):
    trips = spark.createDataFrame([trip(batch="b1"), trip(batch="b2", ingested=at(21)), trip(fare=12.0)], BRONZE_SCHEMA)

    first_batch, second_batch, other_trip = [row.trip_id for row in add_trip_id(trips).collect()]

    assert first_batch == second_batch
    assert other_trip != first_batch


def test_trip_id_does_not_depend_on_the_session_time_zone(spark):
    # If the key were built from timestamp strings, the same instant would hash differently per time zone
    # and every trip would come back as a "new" trip.
    trips = spark.createDataFrame([trip()], BRONZE_SCHEMA)

    utc_id = add_trip_id(trips).first().trip_id
    spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
    try:
        tokyo_id = add_trip_id(trips).first().trip_id
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")

    assert tokyo_id == utc_id


def test_keep_first_load_keeps_one_row_per_trip_from_the_earliest_batch(spark):
    trips = spark.createDataFrame(
        [trip(batch="late", ingested=at(22)), trip(batch="early", ingested=at(20)), trip(fare=12.0, batch="other")],
        BRONZE_SCHEMA,
    )

    kept = keep_first_load(add_trip_id(trips)).collect()

    assert sorted(row._batch_id for row in kept) == ["early", "other"]


def test_every_trip_lands_in_exactly_one_side_with_all_the_rules_it_breaks(spark):
    trips = add_trip_id(
        spark.createDataFrame(
            [
                trip(),  # valid
                trip(distance=0.0),
                trip(fare=-8.0, distance=-1.0),  # breaks two rules
                trip(dropoff=at(10)),  # dropoff equal to pickup
            ],
            BRONZE_SCHEMA,
        )
    )

    valid, rejected = split_valid_and_rejected(trips)
    valid_ids = {row.trip_id for row in valid.collect()}
    reasons_by_fare_and_distance = {(row.fare_amount, row.trip_distance): sorted(row.rejection_reasons) for row in rejected.collect()}

    assert valid_ids.isdisjoint(row.trip_id for row in rejected.collect())
    assert valid.count() + rejected.count() == trips.count()
    assert valid.count() == 1
    assert reasons_by_fare_and_distance == {
        (11.0, 0.0): ["non_positive_distance"],
        (-8.0, -1.0): ["non_positive_distance", "non_positive_fare"],
        (11.0, 2.5): ["dropoff_not_after_pickup"],
    }


def test_a_trip_with_a_missing_value_is_rejected_not_passed_as_valid(spark):
    # `null <= 0` is null, not true, so a rule written as a comparison alone lets a null through as "valid",
    # and silver's NOT NULL columns would then fail the MERGE.
    trips = add_trip_id(spark.createDataFrame([trip(distance=None), trip(pickup_zip=None, fare=9.0)], BRONZE_SCHEMA))

    valid, rejected = split_valid_and_rejected(trips)

    assert valid.count() == 0
    assert all("missing_required_value" in row.rejection_reasons for row in rejected.collect())


def test_null_key_columns_still_give_a_stable_key_that_differs_from_a_real_value(spark):
    trips = spark.createDataFrame([trip(pickup_zip=None), trip(pickup_zip=None, batch="b2"), trip(pickup_zip=0)], BRONZE_SCHEMA)

    null_first, null_second, zero = [row.trip_id for row in add_trip_id(trips).collect()]

    assert null_first == null_second
    assert null_first != zero


def test_empty_bronze_gives_empty_outputs_instead_of_failing(spark):
    empty = add_trip_id(spark.createDataFrame([], BRONZE_SCHEMA))

    valid, rejected = split_valid_and_rejected(keep_first_load(empty))

    assert (valid.count(), rejected.count()) == (0, 0)
    assert to_silver_trips(valid).count() == 0


def test_valid_trips_are_typed_and_renamed_for_silver(spark):
    trips = add_trip_id(spark.createDataFrame([trip(pickup=at(23, 50), dropoff=at(23, 59), distance=1.3, fare=7.5, pickup_zip=7002, dropoff_zip=10001)], BRONZE_SCHEMA))

    row = to_silver_trips(trips).first()

    assert to_silver_trips(trips).columns == [
        "trip_id", "pickup_at", "dropoff_at", "pickup_date", "trip_duration_minutes", "trip_distance_miles",
        "fare_amount", "pickup_zip", "dropoff_zip", "_batch_id", "_ingested_at", "_merged_at",
    ]
    assert (row.pickup_zip, row.dropoff_zip) == ("07002", "10001")
    assert row.fare_amount == Decimal("7.50")
    assert row.trip_duration_minutes == 9.0
    assert row.pickup_date == date(2016, 1, 15)
    assert row.trip_distance_miles == 1.3


def test_quarantine_keeps_the_bronze_columns_as_received(spark):
    trips = add_trip_id(spark.createDataFrame([trip(fare=-8.0, pickup_zip=7002)], BRONZE_SCHEMA))
    _, rejected = split_valid_and_rejected(trips)

    row = to_quarantine(rejected).first()

    assert (row.fare_amount, row.pickup_zip, row.rejection_reasons) == (-8.0, 7002, ["non_positive_fare"])
