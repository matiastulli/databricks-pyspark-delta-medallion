"""Table DDL as write-once SQL migrations, versioned per table, applied by a small in-repo runner.

Migrations live next to the code of the schema they belong to, in `src/<NN_layer>/ddl/`:

    src/00_bronze/ddl/create_schemas_v001.sql
    src/00_bronze/ddl/create_bronze_trips_v001.sql
    src/00_bronze/ddl/rename_bronze_trips_to_nyctaxi_trips_v001.sql
    src/01_silver/ddl/alter_silver_trips_v002.sql

Each table (and `schemas`) has its own version sequence, starting at v001. A table's history starts with `create` or
with a `rename_<layer>_<old>_to_<new>` that carries it over from another table.

The runner itself (src/ops/apply_ddl.py) only executes SQL and records history. Everything that decides *what* runs
and in which order lives here, free of Spark, so it can be unit-tested.
"""

import hashlib
import heapq
import re
from dataclasses import dataclass
from pathlib import Path

# src/medallion/migrations.py -> src/. The bundle deploys src/ as a whole, so this works on Databricks too.
SRC_DIR = Path(__file__).resolve().parents[1]

LAYER_FOLDER = re.compile(r"^(?P<order>\d{2})_(?P<layer>bronze|silver|gold)$")
# <verb>_<layer>_<table>_v<NNN>.sql or <verb>_schemas_v<NNN>.sql (see the naming convention in docs/PLAN.md).
FILE_NAME = re.compile(
    r"^(?P<verb>create|alter|rename|drop)_(?:(?P<schemas>schemas)|(?P<layer>bronze|silver|gold)_(?P<table>[a-z][a-z0-9_]*?))_v(?P<version>\d{3})\.sql$"
)
PLACEHOLDER = re.compile(r"\$\{([a-z_]+)\}")
PLACEHOLDERS = ("catalog", "bronze_schema", "silver_schema", "gold_schema")

SCHEMAS = "schemas"
HISTORY_SCHEMA = "ops"
HISTORY_TABLE = "schema_migrations"


@dataclass(frozen=True)
class Migration:
    key: str  # what the migration versions: "schemas" or "<layer>_<table>", e.g. "bronze_nyctaxi_trips"
    version: int
    verb: str
    path: str  # relative to src/, e.g. "00_bronze/ddl/create_bronze_trips_v001.sql"
    sql: str
    folder_order: int
    renamed_from: str | None = None  # for a rename: the key of the table it carries over

    @property
    def checksum(self) -> str:
        """SHA-256 of the file as written, so any edit to an applied migration is detected."""
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


def parse_migrations(files: dict[str, str]) -> list[Migration]:
    """Validates migration files (path relative to src/ -> content) and returns them in the order they must run.

    Raises ValueError listing every problem found. Order: `schemas` first, then layer folders in order (00, 01, 02),
    tables by name, each table's versions ascending, and a renamed table always after the table it renames.
    """
    problems, migrations = [], []
    for path, sql in sorted(files.items()):
        parts = Path(path).parts
        folder = LAYER_FOLDER.match(parts[0]) if len(parts) == 3 and parts[1] == "ddl" else None
        if not folder:
            problems.append(f"{path}: must be in src/<NN_layer>/ddl/, e.g. 00_bronze/ddl/")
            continue
        name = FILE_NAME.match(parts[2])
        if not name:
            problems.append(f"{path}: must be named <create|alter|rename|drop>_<layer>_<table>_v<NNN>.sql or <verb>_schemas_v<NNN>.sql")
            continue
        version, verb = int(name["version"]), name["verb"]
        if name["schemas"]:
            key, renamed_from = SCHEMAS, None
        else:
            if name["layer"] != folder["layer"]:
                problems.append(f"{path}: a {name['layer']} migration can't live in {parts[0]}/")
                continue
            key, renamed_from = f"{name['layer']}_{name['table']}", None
            if verb == "rename":
                old_new = name["table"].split("_to_")
                if len(old_new) != 2 or not all(old_new) or old_new[0] == old_new[1]:
                    problems.append(f"{path}: a rename must be named rename_<layer>_<old>_to_<new>_v001.sql")
                    continue
                key, renamed_from = f"{name['layer']}_{old_new[1]}", f"{name['layer']}_{old_new[0]}"
        # A history starts with create (or a rename carrying another table over), so neither can come later.
        if verb in ("create", "rename") and version != 1:
            problems.append(f"{path}: {verb} can only be v001; change an existing table with alter")
        if verb not in ("create", "rename") and version == 1:
            problems.append(f"{path}: v001 must create the table (or rename another table into it)")
        migrations.append(Migration(key, version, verb, path, sql, int(folder["order"]), renamed_from))

    by_key: dict[str, list[Migration]] = {}
    for migration in migrations:
        by_key.setdefault(migration.key, []).append(migration)
    for key, versions in sorted(by_key.items()):
        numbers = [m.version for m in versions]
        if duplicates := sorted({n for n in numbers if numbers.count(n) > 1}):
            problems.append(f"{key}: versions {['v%03d' % n for n in duplicates]} are used by more than one file")
        if missing := sorted(set(range(1, max(numbers) + 1)) - set(numbers)):
            problems.append(f"{key}: missing versions {['v%03d' % n for n in missing]}")
        for migration in versions:
            if migration.renamed_from and migration.renamed_from not in by_key:
                problems.append(f"{migration.path}: renames {migration.renamed_from}, which has no migrations")

    if problems:
        raise ValueError("invalid migrations:\n  " + "\n  ".join(problems))
    return [m for key in _key_order(by_key) for m in sorted(by_key[key], key=lambda m: m.version)]


def _key_order(by_key: dict[str, list[Migration]]) -> list[str]:
    """Orders keys: schemas, then folder order and name, except a renamed table waits for the table it renames."""

    def priority(key: str) -> tuple:
        return (key != SCHEMAS, min(m.folder_order for m in by_key[key]), key)

    waits_for = {key: {m.renamed_from for m in migrations if m.renamed_from} for key, migrations in by_key.items()}
    ready = [priority(key) for key, deps in waits_for.items() if not deps]
    heapq.heapify(ready)
    order = []
    while ready:
        key = heapq.heappop(ready)[-1]
        order.append(key)
        for other, deps in waits_for.items():
            if key in deps:
                deps.discard(key)
                if not deps:
                    heapq.heappush(ready, priority(other))
    if len(order) != len(by_key):
        raise ValueError(f"invalid migrations:\n  renames form a cycle: {sorted(set(by_key) - set(order))}")
    return order


def load_migrations(src_dir: Path = SRC_DIR) -> list[Migration]:
    return parse_migrations({path.relative_to(src_dir).as_posix(): path.read_text(encoding="utf-8") for path in sorted(src_dir.glob("*/ddl/*.sql"))})


def pending_migrations(migrations: list[Migration], applied: dict[tuple[str, int], tuple[str, str]]) -> list[Migration]:
    """Returns the migrations not applied yet, in run order.

    `applied` maps (key, version) -> (path, checksum) from the history table. Applied migrations are write-once: if
    one was edited, moved or deleted since it ran, the database and the files no longer tell the same story, so this
    raises instead of carrying on.
    """
    by_id = {(m.key, m.version): m for m in migrations}
    problems = []
    for (key, version), (path, checksum) in sorted(applied.items()):
        migration = by_id.get((key, version))
        if migration is None:
            problems.append(f"{path} was applied but its file is gone")
        elif migration.path != path:
            problems.append(f"{key} v{version:03d} was applied as {path} but the file is now {migration.path}")
        elif migration.checksum != checksum:
            problems.append(f"{path} changed after it was applied; write a new version instead of editing it")
    if problems:
        raise ValueError("migration history doesn't match the ddl/ folders:\n  " + "\n  ".join(problems))
    return [m for m in migrations if (m.key, m.version) not in applied]


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
    return f"00_bronze/ddl/create_bronze_{target}_v001.sql", sql
