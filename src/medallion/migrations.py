"""Table DDL as write-once SQL migrations, one folder per table, applied by a small in-repo runner.

Migrations live next to the code of the schema they belong to, one folder per table. Two folders aren't layers:
`src/catalog/ddl/schemas/` creates the layer schemas and runs first, and `src/ops/ddl/` holds tooling tables and runs last:

    src/catalog/ddl/schemas/v001_create.sql
    src/00_bronze/ddl/nyctaxi_trips/v001_create.sql
                                   /v002_rename.sql   ALTER TABLE trips RENAME TO nyctaxi_trips
                                   /v003_alter.sql
    src/01_silver/ddl/trips/v001_create.sql

The folder is the table as it is called **now**, so its whole history is in one place and reads in version order. A
rename is just another version of that table, not a new identity.

**A migration is identified by the SHA-256 of its content, not by its path.** Renaming a table moves and renumbers its
files, and that must not look like a different migration. Editing an applied migration still fails, and so does
deleting one: those are what write-once protects.

The runner itself (src/ops/notebooks/apply_ddl.py) only executes SQL and records history. Everything that decides *what* runs
and in which order lives here, free of Spark, so it can be unit-tested.
"""

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

# src/medallion/migrations.py -> src/. The bundle deploys src/ as a whole, so this works on Databricks too.
SRC_DIR = Path(__file__).resolve().parents[1]

# A layer folder (00_bronze …), `catalog`, which creates the layer schemas and runs first, or `ops`, which holds tooling
# tables and runs last, after the layers it supports.
LAYER_FOLDER = re.compile(r"^(?:(?P<order>\d{2})_(?P<layer>bronze|silver|gold)|(?P<catalog>catalog)|(?P<ops>ops))$")
TABLE_FOLDER = re.compile(r"^[a-z][a-z0-9_]*$")
FILE_NAME = re.compile(r"^v(?P<version>\d{3})_(?P<verb>create|alter|rename|drop)\.sql$")
PLACEHOLDER = re.compile(r"\$\{([a-z_]+)\}")
PLACEHOLDERS = ("catalog", "bronze_schema", "silver_schema", "gold_schema")

SCHEMAS = "schemas"
CATALOG_ORDER = -1
OPS_ORDER = 99
HISTORY_SCHEMA = "ops"
HISTORY_TABLE = "schema_migrations"


@dataclass(frozen=True)
class Migration:
    key: str  # what the migration versions: "schemas" or "<layer>_<table>", e.g. "bronze_nyctaxi_trips"
    version: int
    verb: str
    path: str  # relative to src/, e.g. "00_bronze/ddl/nyctaxi_trips/v002_rename.sql"
    sql: str
    folder_order: int

    @property
    def checksum(self) -> str:
        """SHA-256 of the file as written: the migration's identity, so moving or renaming the file is free."""
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


def parse_migrations(files: dict[str, str]) -> list[Migration]:
    """Validates migration files (path relative to src/ -> content) and returns them in the order they must run.

    Raises ValueError listing every problem found. Order: folders (catalog, 00, 01, 02, then ops), tables by name,
    and each table's versions ascending.
    """
    problems, migrations = [], []
    for path, sql in sorted(files.items()):
        parts = Path(path).parts
        folder = LAYER_FOLDER.match(parts[0]) if len(parts) == 4 and parts[1] == "ddl" else None
        if not folder:
            problems.append(f"{path}: must be src/<NN_layer, catalog or ops>/ddl/<table>/<file>, e.g. 00_bronze/ddl/nyctaxi_trips/v001_create.sql")
            continue
        table = parts[2]
        if not TABLE_FOLDER.match(table):
            problems.append(f"{path}: the folder {table!r} must be the table name in lowercase letters, digits and underscores")
            continue
        name = FILE_NAME.match(parts[3])
        if not name:
            problems.append(f"{path}: must be named v<NNN>_<create|alter|rename|drop>.sql")
            continue

        # The schemas are the catalog's only DDL, and the only migration whose key isn't <layer>_<table>.
        if (table == SCHEMAS) != bool(folder["catalog"]):
            problems.append(f"{path}: the schemas live in catalog/ddl/schemas/, and catalog/ holds nothing else")
            continue

        version, verb = int(name["version"]), name["verb"]
        layer = folder["layer"] or folder["ops"]
        key = SCHEMAS if folder["catalog"] else f"{layer}_{table}"
        # A table's history starts by creating it; everything after that changes what is already there.
        if verb == "create" and version != 1:
            problems.append(f"{path}: create can only be v001; change an existing table with alter, rename or drop")
        if verb != "create" and version == 1:
            problems.append(f"{path}: v001 must create the table")
        order = CATALOG_ORDER if folder["catalog"] else int(folder["order"]) if folder["order"] else OPS_ORDER
        migrations.append(Migration(key, version, verb, path, sql, order))

    by_key: dict[str, list[Migration]] = {}
    for migration in migrations:
        by_key.setdefault(migration.key, []).append(migration)
    for key, versions in sorted(by_key.items()):
        numbers = [m.version for m in versions]
        if duplicates := sorted({n for n in numbers if numbers.count(n) > 1}):
            problems.append(f"{key}: versions {['v%03d' % n for n in duplicates]} are used by more than one file")
        if missing := sorted(set(range(1, max(numbers) + 1)) - set(numbers)):
            problems.append(f"{key}: missing versions {['v%03d' % n for n in missing]}")

    if problems:
        raise ValueError("invalid migrations:\n  " + "\n  ".join(problems))
    return sorted(migrations, key=lambda m: (m.folder_order, m.key, m.version))


def load_migrations(src_dir: Path = SRC_DIR) -> list[Migration]:
    return parse_migrations({path.relative_to(src_dir).as_posix(): path.read_text(encoding="utf-8") for path in sorted(src_dir.glob("*/ddl/*/*.sql"))})


def pending_migrations(migrations: list[Migration], applied: dict[str, tuple[str, int, str]]) -> list[Migration]:
    """Returns the migrations not applied yet, in run order.

    `applied` maps checksum -> (key, version, path) from the history table. A file that moved keeps its checksum, so it
    is recognised wherever it now lives. What still fails is an applied migration that was **edited** (its key and
    version are still there, with different content) or **deleted** (neither its content nor its key and version).
    """
    by_checksum = {m.checksum: m for m in migrations}
    by_id = {(m.key, m.version): m for m in migrations}
    problems = []
    for checksum, (key, version, path) in sorted(applied.items(), key=lambda item: item[1]):
        if checksum in by_checksum:
            continue
        if (key, version) in by_id:
            problems.append(f"{by_id[(key, version)].path} changed after it was applied; write a new version instead of editing it")
        else:
            problems.append(f"{path} was applied but is gone: no file has its content, and {key} v{version:03d} no longer exists")
    if problems:
        raise ValueError("migration history doesn't match the ddl/ folders:\n  " + "\n  ".join(problems))
    return [m for m in migrations if m.checksum not in applied]


def moved_migrations(migrations: list[Migration], applied: dict[str, tuple[str, int, str]]) -> list[Migration]:
    """Applied migrations whose file has moved or been renumbered, so the history can be refreshed."""
    return [m for m in migrations if m.checksum in applied and applied[m.checksum] != (m.key, m.version, m.path)]


def render(sql: str, values: dict[str, str]) -> str:
    """Replaces ${name} placeholders. An unknown or missing placeholder raises instead of reaching the database."""
    names = set(PLACEHOLDER.findall(sql))
    if unknown := names - values.keys():
        raise ValueError(f"no value for placeholders {sorted(unknown)}")
    return PLACEHOLDER.sub(lambda match: values[match[1]], sql)


def split_statements(sql: str) -> list[str]:
    """Splits a migration into statements on `;`, ignoring semicolons inside quotes, backticks and `--` comments."""
    statements, current, quote, index = [], [], None, 0
    while index < len(sql):
        char = sql[index]
        if quote:
            current.append(char)
            if char == quote:
                quote = None
        elif char in ("'", '"', "`"):
            quote = char
            current.append(char)
        elif sql.startswith("--", index):
            end = sql.find("\n", index)
            index = len(sql) if end == -1 else end
            continue
        elif char == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(char)
        index += 1
    statements.append("".join(current))
    return [statement.strip() for statement in statements if statement.strip()]


def render_bronze_migration(target: str, source_table: str, mode: str, columns: list[tuple[str, str]]) -> tuple[str, str]:
    """Builds the v001 migration (path relative to src/, content) for a bronze table from its source table's columns.

    Bronze keeps the source columns untouched (raw stays raw) and adds the ingestion metadata columns that
    src/medallion/bronze.py writes.
    """
    column_lines = [f"  {name} {data_type.upper()}" for name, data_type in columns]
    column_lines += ["  _batch_id STRING", "  _ingested_at TIMESTAMP", "  _source_table STRING", "  _source_file STRING"]
    sql = (
        f"-- Bronze table for {source_table} ({mode} mode).\n"
        "-- Generated by scripts/new_bronze_migration.py from the source table's schema: source columns keep their\n"
        "-- names and types, and the _ columns are ingestion metadata. Review before committing.\n"
        f"CREATE TABLE IF NOT EXISTS `${{catalog}}`.`${{bronze_schema}}`.{target} (\n"
        + ",\n".join(column_lines)
        + "\n)\n"
        f"COMMENT 'Raw {source_table}, loaded in {mode} mode, one _batch_id per load';\n"
    )
    return f"00_bronze/ddl/{target}/v001_create.sql", sql
