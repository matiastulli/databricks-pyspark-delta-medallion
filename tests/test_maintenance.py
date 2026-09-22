from medallion.maintenance import describe_change, tables_to_maintain


def test_temporary_tables_are_skipped_and_the_rest_come_in_a_stable_order():
    tables = {
        "01_silver": ["trips_quarantine", "trips", "_tmp_clean_trips_keyed"],
        "00_bronze": ["nyctaxi_trips", "tpch_orders"],
    }

    assert tables_to_maintain(tables) == [
        "00_bronze.nyctaxi_trips",
        "00_bronze.tpch_orders",
        "01_silver.trips",
        "01_silver.trips_quarantine",
    ]


def test_a_compaction_is_reported_as_files_removed_with_clustering_kept():
    before = {"numFiles": 12, "sizeInBytes": 40_000_000, "clusteringColumns": ["o_orderdate"]}
    after = {"numFiles": 3, "sizeInBytes": 38_500_000, "clusteringColumns": ["o_orderdate"]}

    assert describe_change(before, after) == {
        "files": [12, 3],
        "size_mb": [40.0, 38.5],
        "files_removed": 9,
        "clustering": ["o_orderdate"],
    }


def test_a_table_that_did_not_change_reports_zero_files_removed():
    same = {"numFiles": 1, "sizeInBytes": 1_000, "clusteringColumns": None}

    assert describe_change(same, same) | {"files": [1, 1]} == describe_change(same, same)
    assert describe_change(same, same)["files_removed"] == 0
    assert describe_change(same, same)["clustering"] == []
