import pytest

from medallion.migrations import (
    load_migrations,
    moved_migrations,
    parse_migrations,
    pending_migrations,
    render,
    render_bronze_migration,
    split_statements,
)


def files(*paths):
    return {path: f"-- {path}\nSELECT 1;" for path in paths}


def run_order(*paths):
    return [m.path for m in parse_migrations(files(*paths))]


def applied_from(migrations):
    return {m.checksum: (m.key, m.version, m.path) for m in migrations}


def test_the_committed_migrations_are_valid_and_start_with_the_schemas():
    # Runs in CI, so a misplaced, badly named, duplicated or missing migration fails before anyone applies it.
    migrations = load_migrations()

    assert migrations[0].path == "catalog/ddl/schemas/v001_create.sql"
    # A renamed table keeps one history under its current name.
    assert [m.path for m in migrations if m.key == "bronze_nyctaxi_trips"] == [
        "00_bronze/ddl/nyctaxi_trips/v001_create.sql",
        "00_bronze/ddl/nyctaxi_trips/v002_rename.sql",
        "00_bronze/ddl/nyctaxi_trips/v003_alter.sql",
    ]


def test_run_order_is_catalog_then_layer_folders_then_table_then_version():
    assert run_order(
        "ops/ddl/processed_versions/v001_create.sql",
        "02_gold/ddl/agg_trips_daily/v001_create.sql",
        "01_silver/ddl/trips/v010_alter.sql",
        "01_silver/ddl/trips/v001_create.sql",
        "01_silver/ddl/trips/v002_alter.sql",
        "00_bronze/ddl/tpch_orders/v001_create.sql",
        *[f"01_silver/ddl/trips/v{v:03d}_alter.sql" for v in range(3, 10)],
        "catalog/ddl/schemas/v001_create.sql",
    ) == [
        "catalog/ddl/schemas/v001_create.sql",
        "00_bronze/ddl/tpch_orders/v001_create.sql",
        "01_silver/ddl/trips/v001_create.sql",
        *[f"01_silver/ddl/trips/v{v:03d}_alter.sql" for v in range(2, 11)],  # v010 after v009, not after v001
        "02_gold/ddl/agg_trips_daily/v001_create.sql",
        "ops/ddl/processed_versions/v001_create.sql",  # ops runs after the layers it supports
    ]


@pytest.mark.parametrize(
    "paths, message",
    [
        (["01_silver/ddl/trips_v001_create.sql"], "must be src/<NN_layer, catalog or ops>/ddl/<table>/<file>"),
        (["01_silver/trips/v001_create.sql"], "must be src/<NN_layer, catalog or ops>/ddl/<table>/<file>"),
        (["01_silver/ddl/Trips/v001_create.sql"], "must be the table name in lowercase"),
        (["01_silver/ddl/trips/create_v001.sql"], "must be named v<NNN>_"),
        (["01_silver/ddl/trips/v001_update.sql"], "must be named v<NNN>_"),  # unknown verb
        (["01_silver/ddl/trips/v001_alter.sql"], "v001 must create the table"),
        (["01_silver/ddl/trips/v001_create.sql", "01_silver/ddl/trips/v002_create.sql"], "create can only be v001"),
        (["01_silver/ddl/trips/v001_create.sql", "01_silver/ddl/trips/v003_alter.sql"], r"missing versions \['v002'\]"),
        (["00_bronze/ddl/schemas/v001_create.sql"], "the schemas live in catalog/ddl/schemas/"),  # not a bronze table
        (["catalog/ddl/landing/v001_create.sql"], "catalog/ holds nothing else"),
    ],
)
def test_misplaced_misnamed_or_inconsistent_migrations_are_rejected(paths, message):
    with pytest.raises(ValueError, match=message):
        parse_migrations(files(*paths))


def test_only_migrations_not_yet_applied_are_pending():
    migrations = parse_migrations(files("catalog/ddl/schemas/v001_create.sql", "01_silver/ddl/trips/v001_create.sql", "01_silver/ddl/trips/v002_alter.sql"))

    assert [m.path for m in pending_migrations(migrations, applied_from(migrations[:2]))] == ["01_silver/ddl/trips/v002_alter.sql"]
    assert pending_migrations(migrations, applied_from(migrations)) == []


def test_a_file_that_moved_is_still_the_same_migration():
    # Renaming a table moves and renumbers its files: same content, new path, so nothing is re-applied.
    before = parse_migrations(files("00_bronze/ddl/trips/v001_create.sql"))
    applied = applied_from(before)
    after = parse_migrations({"00_bronze/ddl/nyctaxi_trips/v001_create.sql": before[0].sql})

    assert pending_migrations(after, applied) == []
    assert [m.path for m in moved_migrations(after, applied)] == ["00_bronze/ddl/nyctaxi_trips/v001_create.sql"]
    assert moved_migrations(before, applied) == []


def test_editing_an_applied_migration_is_refused():
    original = parse_migrations(files("01_silver/ddl/trips/v001_create.sql"))
    edited = parse_migrations({"01_silver/ddl/trips/v001_create.sql": "CREATE TABLE IF NOT EXISTS x (id INT);"})

    with pytest.raises(ValueError, match="changed after it was applied"):
        pending_migrations(edited, applied_from(original))


def test_an_applied_migration_whose_file_is_gone_is_refused():
    migrations = parse_migrations(files("01_silver/ddl/trips/v001_create.sql"))
    applied = applied_from(migrations) | {"deadbeef": ("silver_trips", 2, "01_silver/ddl/trips/v002_alter.sql")}

    with pytest.raises(ValueError, match="was applied but is gone"):
        pending_migrations(migrations, applied)


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
    path, sql = render_bronze_migration("tpch_region", "samples.tpch.region", "overwrite", [("r_regionkey", "bigint"), ("r_name", "string")])

    assert path == "00_bronze/ddl/tpch_region/v001_create.sql"
    assert parse_migrations({path: sql})[0].key == "bronze_tpch_region"
    assert split_statements(sql) == [
        "CREATE TABLE IF NOT EXISTS `${catalog}`.`${bronze_schema}`.tpch_region (\n"
        "  r_regionkey BIGINT,\n  r_name STRING,\n"
        "  _batch_id STRING,\n  _ingested_at TIMESTAMP,\n  _source_table STRING,\n  _source_file STRING\n)\n"
        "COMMENT 'Raw samples.tpch.region, loaded in overwrite mode, one _batch_id per load'"
    ]
