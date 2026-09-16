from datetime import datetime, timezone

from medallion.silver import add_trip_id, keep_first_load, split_valid_and_rejected

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
