import pytest

from medallion.sources import bronze_table_name, load_sources, parse_sources


def entry(**overrides):
    return {"table": "samples.tpch.orders", "mode": "overwrite", "schedule": "0 0 6 1 * ?", **overrides}


def test_the_committed_config_is_valid():
    # Runs in CI, so a broken config/sources.toml fails before anyone deploys it.
    sources = load_sources()

    assert "nyctaxi_trips" in {source.name for source in sources}


def test_bronze_table_names_follow_the_convention_source_system_then_table():
    assert bronze_table_name("samples.nyctaxi.trips") == "nyctaxi_trips"
    assert bronze_table_name("samples.tpch.orders") == "tpch_orders"


def test_two_sources_landing_in_the_same_bronze_table_are_rejected():
    # Different catalogs, same system and table: both would derive tpch_orders and mix their rows.
    config = {"sources": [entry(), entry(table="other_catalog.tpch.orders")]}

    with pytest.raises(ValueError, match=r"duplicate bronze tables: \['tpch_orders'\]"):
        parse_sources(config)


@pytest.mark.parametrize(
    "bad_entry, message",
    [
        (entry(mode="upsert"), "mode 'upsert' must be one of"),
        ({**entry(), "mdoe": "append"}, r"unknown \['mdoe'\]"),
        ({key: value for key, value in entry().items() if key != "mode"}, r"missing \['mode'\]"),
        (entry(table="orders"), "table 'orders' must be catalog.schema.table"),
        (entry(table="samples.TPCH.orders"), "bronze table name 'TPCH_orders' must be lowercase"),
        (entry(schedule="0 6 1 * *"), "must be a Quartz cron with 6 or 7 fields"),
        ({**entry(), "target": "orders"}, r"unknown \['target'\]"),  # names are derived, not configured
    ],
)
def test_entries_that_would_load_the_wrong_way_are_rejected(bad_entry, message):
    with pytest.raises(ValueError, match=message):
        parse_sources({"sources": [bad_entry]})
