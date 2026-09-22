from datetime import date

from medallion.incremental import changed_keys, plan_run

CHANGES_SCHEMA = "pickup_date date, _change_type string, _commit_version bigint"


def test_a_process_without_a_watermark_rebuilds_everything():
    # The change feed only records versions after it was enabled, so there is nothing to read back.
    assert plan_run(None, 42) == {"mode": "full", "start_version": None, "end_version": 42}


def test_a_process_up_to_date_has_nothing_to_do():
    assert plan_run(42, 42)["mode"] == "none"
    assert plan_run(43, 42)["mode"] == "none"  # a restored table can leave the watermark ahead


def test_otherwise_it_reads_the_versions_after_the_watermark():
    assert plan_run(40, 42) == {"mode": "incremental", "start_version": 41, "end_version": 42}


def test_deleted_and_updated_rows_count_as_changed_keys_not_just_inserts(spark):
    # A date whose rows were deleted still has to be recomputed, or the aggregate keeps counting rows that are gone.
    changes = spark.createDataFrame(
        [
            (date(2016, 1, 1), "insert", 11),
            (date(2016, 1, 2), "delete", 12),
            (date(2016, 1, 3), "update_preimage", 13),
            (date(2016, 1, 3), "update_postimage", 13),
        ],
        CHANGES_SCHEMA,
    )

    assert changed_keys(changes, "pickup_date") == [date(2016, 1, 1), date(2016, 1, 2), date(2016, 1, 3)]


def test_an_empty_change_feed_gives_no_keys(spark):
    assert changed_keys(spark.createDataFrame([], CHANGES_SCHEMA), "pickup_date") == []
