import pytest

from medallion.contract import raise_if_schema_mismatch, schema_mismatches

TABLE = [("pickup_date", "date"), ("trips", "bigint"), ("avg_fare", "decimal(10,2)")]


def test_a_write_matching_the_ddl_passes_regardless_of_column_order():
    written = [("trips", "bigint"), ("avg_fare", "decimal(10,2)"), ("pickup_date", "date")]

    assert schema_mismatches(written, TABLE) == []
    raise_if_schema_mismatch(written, TABLE, "gold.agg_trips_daily")


def test_the_two_mistakes_delta_silently_accepts_are_caught():
    # Verified on Databricks: Delta fills a missing nullable column with null and casts compatible types.
    written = [("pickup_date", "date"), ("trips", "bigint"), ("avg_fare", "double")]
    missing = [("pickup_date", "date"), ("trips", "bigint")]

    assert schema_mismatches(written, TABLE) == ["column avg_fare is double but the table has decimal(10,2)"]
    assert schema_mismatches(missing, TABLE) == ["missing column avg_fare (decimal(10,2))"]
    with pytest.raises(ValueError, match="doesn't match the DDL of gold.agg_trips_daily: missing column avg_fare"):
        raise_if_schema_mismatch(missing, TABLE, "gold.agg_trips_daily")


def test_an_extra_column_is_reported():
    assert schema_mismatches(TABLE + [("surprise", "string")], TABLE) == ["extra column surprise (string)"]
