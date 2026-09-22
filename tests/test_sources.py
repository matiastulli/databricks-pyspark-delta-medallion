import pytest

from medallion.sources import bronze_table_name, load_sources, parse_sources


def table_entry(**overrides):
    return {"kind": "table", "table": "samples.tpch.orders", "mode": "overwrite", "schedule": "0 0 6 1 * ?", **overrides}


def files_entry(**overrides):
    return {"kind": "files", "volume": "landing", "path": "trips", "format": "json", "mode": "append", "schedule": "0 15 5 * * ?", **overrides}


def test_the_committed_config_is_valid():
    # Runs in CI, so a broken config/sources.toml fails before anyone deploys it.
    sources = {source.name: source for source in load_sources()}

    assert sources["nyctaxi_trips"].kind == "table"
    assert sources["landing_trips"].kind == "files"


def test_bronze_table_names_follow_the_convention_for_both_kinds():
    assert bronze_table_name("samples.nyctaxi.trips") == "nyctaxi_trips"
    assert parse_sources({"sources": [table_entry()]})[0].name == "tpch_orders"
    # For files the "system" is the volume they land in.
    assert parse_sources({"sources": [files_entry()]})[0].name == "landing_trips"


def test_a_source_says_where_its_data_comes_from():
    assert parse_sources({"sources": [table_entry()]})[0].location == "samples.tpch.orders"
    assert parse_sources({"sources": [files_entry()]})[0].location == "/Volumes/{catalog}/{bronze_schema}/landing/trips"


def test_two_sources_landing_in_the_same_bronze_table_are_rejected():
    # Different catalogs, same system and table: both would derive tpch_orders and mix their rows.
    config = {"sources": [table_entry(), table_entry(table="other_catalog.tpch.orders")]}

    with pytest.raises(ValueError, match=r"duplicate bronze tables: \['tpch_orders'\]"):
        parse_sources(config)


@pytest.mark.parametrize(
    "bad_entry, message",
    [
        (table_entry(mode="upsert"), "mode 'upsert' must be one of"),
        ({**table_entry(), "mdoe": "append"}, r"unknown \['mdoe'\]"),
        ({key: value for key, value in table_entry().items() if key != "mode"}, r"missing \['mode'\]"),
        (table_entry(table="orders"), "table 'orders' must be catalog.schema.table"),
        (table_entry(table="samples.TPCH.orders"), "bronze table name 'TPCH_orders' must be lowercase"),
        (table_entry(schedule="0 6 1 * *"), "must be a Quartz cron with 6 or 7 fields"),
        ({**table_entry(), "target": "orders"}, r"unknown \['target'\]"),  # names are derived, not configured
        ({key: value for key, value in table_entry().items() if key != "kind"}, "kind None must be one of"),
        (table_entry(kind="stream"), "kind 'stream' must be one of"),
        # A files source needs its own settings, not a table.
        (files_entry(format="avro"), "format 'avro' must be one of"),
        (files_entry(mode="overwrite"), "files are ingested incrementally, so mode must be 'append'"),
        ({**files_entry(), "table": "samples.tpch.orders"}, r"unknown \['table'\]"),
        ({key: value for key, value in files_entry().items() if key != "volume"}, r"missing \['volume'\]"),
    ],
)
def test_entries_that_would_load_the_wrong_way_are_rejected(bad_entry, message):
    with pytest.raises(ValueError, match=message):
        parse_sources({"sources": [bad_entry]})
