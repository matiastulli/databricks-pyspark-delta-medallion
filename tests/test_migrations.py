import pytest

from medallion.migrations import (
    FILE_NAME,
    load_migrations,
    parse_migrations,
    pending_migrations,
    render,
    render_bronze_migration,
    split_statements,
)


def files(*names):
    return {name: f"-- {name}\nSELECT 1;" for name in names}


def test_the_committed_migrations_are_valid():
    # Runs in CI, so a badly named, duplicated or missing migration fails before anyone applies it.
    migrations = load_migrations()

    assert [m.version for m in migrations] == list(range(1, len(migrations) + 1))
    assert migrations[0].file == "V001__create_schemas.sql"


def test_versions_sort_by_number_not_by_text():
    names = [f"V{v:03d}__create_silver_t{v}.sql" for v in (10, 2, 1, 3, 4, 5, 6, 7, 8, 9)]

    assert [m.version for m in parse_migrations(files(*names))] == list(range(1, 11))


@pytest.mark.parametrize(
    "names, message",
    [
        (["V001__create_schemas.sql", "V001__create_silver_trips.sql"], "V001 is used by"),
        (["V001__create_schemas.sql", "V003__create_silver_trips.sql"], r"missing versions: \['V002'\]"),
        (["V1__create_schemas.sql"], "must be V<NNN>__"),
        (["V001__create_trips.sql"], "must be V<NNN>__"),  # no layer in the name
        (["V001__update_silver_trips.sql"], "must be V<NNN>__"),  # unknown verb
    ],
)
def test_badly_named_duplicated_or_missing_migrations_are_rejected(names, message):
    with pytest.raises(ValueError, match=message):
        parse_migrations(files(*names))


def test_only_migrations_not_yet_applied_are_pending():
    migrations = parse_migrations(files("V001__create_schemas.sql", "V002__create_silver_trips.sql", "V003__create_gold_agg_trips_daily.sql"))
    applied = {m.version: (m.file, m.checksum) for m in migrations[:2]}

    assert [m.file for m in pending_migrations(migrations, applied)] == ["V003__create_gold_agg_trips_daily.sql"]
    assert pending_migrations(migrations, {m.version: (m.file, m.checksum) for m in migrations}) == []


def test_editing_an_applied_migration_is_refused():
    original = parse_migrations(files("V001__create_schemas.sql"))
    applied = {1: (original[0].file, original[0].checksum)}
    edited = parse_migrations({"V001__create_schemas.sql": "CREATE SCHEMA IF NOT EXISTS x;"})

    with pytest.raises(ValueError, match="changed after it was applied"):
        pending_migrations(edited, applied)


def test_an_applied_migration_whose_file_is_gone_is_refused():
    migrations = parse_migrations(files("V001__create_schemas.sql"))

    with pytest.raises(ValueError, match="was applied but its file is gone"):
        pending_migrations(migrations, {1: (migrations[0].file, migrations[0].checksum), 2: ("V002__create_silver_trips.sql", "abc")})


def test_semicolons_inside_quotes_and_comments_do_not_split_statements():
    sql = """
    -- a comment; with a semicolon
    CREATE SCHEMA IF NOT EXISTS `a;b` COMMENT 'raw; untouched';
    ALTER TABLE t SET TBLPROPERTIES ('k' = 'v;w');
    """

    assert split_statements(sql) == [
        "CREATE SCHEMA IF NOT EXISTS `a;b` COMMENT 'raw; untouched'",
        "ALTER TABLE t SET TBLPROPERTIES ('k' = 'v;w')",
    ]


def test_placeholders_are_replaced_and_a_missing_value_is_refused():
    assert render("`${catalog}`.`${gold_schema}`.t", {"catalog": "medallion", "gold_schema": "02_gold"}) == "`medallion`.`02_gold`.t"

    with pytest.raises(ValueError, match=r"no value for placeholders \['silver_schema'\]"):
        render("`${catalog}`.`${silver_schema}`.t", {"catalog": "medallion"})


def test_generated_bronze_migration_keeps_source_columns_and_adds_metadata():
    name, sql = render_bronze_migration(9, "tpch_region", "samples.tpch.region", "overwrite", [("r_regionkey", "bigint"), ("r_name", "string")])

    assert name == "V009__create_bronze_tpch_region.sql"
    assert FILE_NAME.match(name)
    assert split_statements(sql) == [
        "CREATE TABLE IF NOT EXISTS `${catalog}`.`${bronze_schema}`.tpch_region (\n"
        "  r_regionkey BIGINT,\n  r_name STRING,\n"
        "  _batch_id STRING,\n  _ingested_at TIMESTAMP,\n  _source_table STRING,\n  _source_file STRING\n)\n"
        "COMMENT 'Raw samples.tpch.region, loaded in overwrite mode, one _batch_id per load'"
    ]
