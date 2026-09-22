import pytest

from medallion.migrations import (
    load_migrations,
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


def test_the_committed_migrations_are_valid_and_start_with_the_schemas():
    # Runs in CI, so a misplaced, badly named, duplicated or missing migration fails before anyone applies it.
    migrations = load_migrations()

    assert migrations[0].path == "00_bronze/ddl/schemas_v001_create.sql"
    assert {m.key for m in migrations} >= {"bronze_tpch_orders", "silver_trips", "silver_trips_quarantine"}


def test_run_order_is_schemas_then_layer_folders_then_table_then_version():
    assert run_order(
        "02_gold/ddl/gold_agg_trips_daily_v001_create.sql",
        "01_silver/ddl/silver_trips_v010_alter.sql",
        "01_silver/ddl/silver_trips_v001_create.sql",
        "01_silver/ddl/silver_trips_v002_alter.sql",
        "00_bronze/ddl/bronze_tpch_orders_v001_create.sql",
        *[f"01_silver/ddl/silver_trips_v{v:03d}_alter.sql" for v in range(3, 10)],
        "00_bronze/ddl/schemas_v001_create.sql",
    ) == [
        "00_bronze/ddl/schemas_v001_create.sql",
        "00_bronze/ddl/bronze_tpch_orders_v001_create.sql",
        "01_silver/ddl/silver_trips_v001_create.sql",
        *[f"01_silver/ddl/silver_trips_v{v:03d}_alter.sql" for v in range(2, 11)],  # v010 after v009, not after v001
        "02_gold/ddl/gold_agg_trips_daily_v001_create.sql",
    ]


def test_ops_migrations_run_after_the_layers_they_support():
    assert run_order(
        "ops/ddl/ops_processed_versions_v001_create.sql",
        "02_gold/ddl/gold_agg_trips_daily_v001_create.sql",
        "00_bronze/ddl/schemas_v001_create.sql",
    ) == [
        "00_bronze/ddl/schemas_v001_create.sql",
        "02_gold/ddl/gold_agg_trips_daily_v001_create.sql",
        "ops/ddl/ops_processed_versions_v001_create.sql",
    ]


def test_a_renamed_table_runs_after_the_table_it_renames_even_if_its_name_sorts_first():
    # "nyctaxi_trips" sorts before "trips"; on a fresh catalog the rename must still wait for trips to be created.
    assert run_order(
        "00_bronze/ddl/bronze_trips_to_nyctaxi_trips_v001_rename.sql",
        "00_bronze/ddl/bronze_trips_v001_create.sql",
        "00_bronze/ddl/bronze_trips_v002_alter.sql",
        "00_bronze/ddl/bronze_nyctaxi_trips_v002_alter.sql",
    ) == [
        "00_bronze/ddl/bronze_trips_v001_create.sql",
        "00_bronze/ddl/bronze_trips_v002_alter.sql",
        "00_bronze/ddl/bronze_trips_to_nyctaxi_trips_v001_rename.sql",
        "00_bronze/ddl/bronze_nyctaxi_trips_v002_alter.sql",
    ]


@pytest.mark.parametrize(
    "paths, message",
    [
        (["01_silver/silver_trips_v001_create.sql"], r"must be in src/<NN_layer>/ddl/"),
        (["00_bronze/ddl/bronze_trips_create_v001.sql"], "must be named"),
        (["00_bronze/ddl/bronze_trips_v001_update.sql"], "must be named"),  # unknown verb
        (["00_bronze/ddl/silver_trips_v001_create.sql"], "a silver migration can't live in 00_bronze/"),
        (["00_bronze/ddl/bronze_trips_v001_create.sql", "00_bronze/ddl/bronze_trips_v001_alter.sql"], "v001 must create the table"),
        (["00_bronze/ddl/bronze_trips_v001_create.sql", "00_bronze/ddl/bronze_trips_v002_create.sql"], "create can only be v001"),
        (["00_bronze/ddl/bronze_trips_v001_create.sql", "00_bronze/ddl/bronze_trips_v003_alter.sql"], r"missing versions \['v002'\]"),
        (["00_bronze/ddl/bronze_trips_to_nyctaxi_trips_v001_rename.sql"], "renames bronze_trips, which has no migrations"),
        (["00_bronze/ddl/bronze_trips_v001_rename.sql"], "a rename must be named <layer>_<old>_to_<new>_v001_rename"),
    ],
)
def test_misplaced_misnamed_or_inconsistent_migrations_are_rejected(paths, message):
    with pytest.raises(ValueError, match=message):
        parse_migrations(files(*paths))


def test_only_migrations_not_yet_applied_are_pending():
    migrations = parse_migrations(files("00_bronze/ddl/schemas_v001_create.sql", "01_silver/ddl/silver_trips_v001_create.sql", "01_silver/ddl/silver_trips_v002_alter.sql"))
    applied = {(m.key, m.version): (m.path, m.checksum) for m in migrations[:2]}

    assert [m.path for m in pending_migrations(migrations, applied)] == ["01_silver/ddl/silver_trips_v002_alter.sql"]
    assert pending_migrations(migrations, {(m.key, m.version): (m.path, m.checksum) for m in migrations}) == []


def test_editing_an_applied_migration_is_refused():
    original = parse_migrations(files("01_silver/ddl/silver_trips_v001_create.sql"))[0]
    edited = parse_migrations({original.path: "CREATE TABLE IF NOT EXISTS x (id INT);"})

    with pytest.raises(ValueError, match="changed after it was applied"):
        pending_migrations(edited, {(original.key, 1): (original.path, original.checksum)})


def test_an_applied_migration_whose_file_is_gone_is_refused():
    migrations = parse_migrations(files("01_silver/ddl/silver_trips_v001_create.sql"))
    applied = {("silver_trips", 1): (migrations[0].path, migrations[0].checksum), ("silver_trips", 2): ("01_silver/ddl/silver_trips_v002_alter.sql", "abc")}

    with pytest.raises(ValueError, match="was applied but its file is gone"):
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

    assert path == "00_bronze/ddl/bronze_tpch_region_v001_create.sql"
    assert parse_migrations({path: sql})[0].key == "bronze_tpch_region"
    assert split_statements(sql) == [
        "CREATE TABLE IF NOT EXISTS `${catalog}`.`${bronze_schema}`.tpch_region (\n"
        "  r_regionkey BIGINT,\n  r_name STRING,\n"
        "  _batch_id STRING,\n  _ingested_at TIMESTAMP,\n  _source_table STRING,\n  _source_file STRING\n)\n"
        "COMMENT 'Raw samples.tpch.region, loaded in overwrite mode, one _batch_id per load'"
    ]
