import pytest

from medallion.sources import load_sources, parse_sources


def entry(**overrides):
    return {"name": "orders", "table": "samples.tpch.orders", "target": "tpch_orders", "mode": "overwrite", "schedule": "0 0 6 1 * ?", **overrides}


def test_the_committed_config_is_valid():
    # Runs in CI, so a broken config/sources.toml fails before anyone deploys it.
    sources = load_sources()

    assert "trips" in {source.name for source in sources}


def test_two_sources_writing_the_same_target_are_rejected():
    config = {"sources": [entry(), entry(name="orders_copy", table="samples.tpch.lineitem")]}

    with pytest.raises(ValueError, match=r"duplicate target: \['tpch_orders'\]"):
        parse_sources(config)


@pytest.mark.parametrize(
    "bad_entry, message",
    [
        (entry(mode="upsert"), "mode 'upsert' must be one of"),
        ({**entry(), "mdoe": "append"}, r"unknown \['mdoe'\]"),
        ({key: value for key, value in entry().items() if key != "mode"}, r"missing \['mode'\]"),
        (entry(target="tpch-orders"), "target 'tpch-orders' must be lowercase"),
        (entry(table="orders"), "table 'orders' must be catalog.schema.table"),
        (entry(schedule="0 6 1 * *"), "must be a Quartz cron with 6 or 7 fields"),
    ],
)
def test_entries_that_would_load_the_wrong_way_are_rejected(bad_entry, message):
    with pytest.raises(ValueError, match=message):
        parse_sources({"sources": [bad_entry]})
