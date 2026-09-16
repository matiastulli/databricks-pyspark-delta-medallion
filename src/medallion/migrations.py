"""Table DDL as write-once SQL migrations in src/ddl/migrations/, applied in version order by a small in-repo runner.

The runner itself (src/ops/apply_ddl.py) only executes SQL and records history. Everything that decides *what* runs
lives here, free of Spark, so it can be unit-tested: file-name rules, version order, pending migrations, drift
detection, statement splitting, placeholders, and the generated bronze migrations.
"""

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

# src/medallion/migrations.py -> src/ddl/migrations. The bundle deploys src/ as a whole, so this works on Databricks too.
DEFAULT_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "ddl" / "migrations"

# V<NNN>__<verb>_<layer>_<table>.sql, or V<NNN>__<verb>_schemas.sql (see the naming convention in docs/PLAN.md).
FILE_NAME = re.compile(r"^V(?P<version>\d{3})__(?P<verb>create|alter|rename|drop)_(?P<subject>schemas|(?:bronze|silver|gold)_[a-z][a-z0-9_]*)\.sql$")
PLACEHOLDER = re.compile(r"\$\{([a-z_]+)\}")
PLACEHOLDERS = ("catalog", "bronze_schema", "silver_schema", "gold_schema")

HISTORY_SCHEMA = "ops"
HISTORY_TABLE = "schema_migrations"


@dataclass(frozen=True)
class Migration:
    version: int
    file: str
    sql: str

    @property
    def checksum(self) -> str:
        """SHA-256 of the file as written, so any edit to an applied migration is detected."""
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


def parse_migrations(files: dict[str, str]) -> list[Migration]:
    """Validates migration files (name -> content) and returns them in version order.

    Raises ValueError listing every problem: a bad file name, two files with one version, or a gap in the versions
    (a missing file would otherwise be skipped silently).
    """
    problems = []
    by_version: dict[int, list[str]] = {}
    for name in files:
        match = FILE_NAME.match(name)
        if not match:
            problems.append(f"{name}: must be V<NNN>__<create|alter|rename|drop>_<schemas|layer_table>.sql")
            continue
        by_version.setdefault(int(match["version"]), []).append(name)

    for version, names in sorted(by_version.items()):
        if len(names) > 1:
            problems.append(f"V{version:03d} is used by {sorted(names)}")
    if by_version and (missing := sorted(set(range(1, max(by_version) + 1)) - by_version.keys())):
        problems.append(f"missing versions: {['V%03d' % v for v in missing]}")

    if problems:
        raise ValueError("invalid migrations:\n  " + "\n  ".join(problems))
    return [Migration(version, names[0], files[names[0]]) for version, names in sorted(by_version.items())]


def load_migrations(directory: Path = DEFAULT_MIGRATIONS_DIR) -> list[Migration]:
    return parse_migrations({path.name: path.read_text(encoding="utf-8") for path in sorted(directory.glob("*.sql"))})


def pending_migrations(migrations: list[Migration], applied: dict[int, tuple[str, str]]) -> list[Migration]:
    """Returns the migrations not applied yet, in order.

    `applied` maps version -> (file, checksum) from the history table. Applied migrations are write-once: if one was
    edited, renamed or deleted since it ran, the database and the files no longer tell the same story, so this raises
    instead of carrying on.
    """
    by_version = {migration.version: migration for migration in migrations}
    problems = []
    for version, (file, checksum) in sorted(applied.items()):
        migration = by_version.get(version)
        if migration is None:
            problems.append(f"V{version:03d} ({file}) was applied but its file is gone")
        elif migration.file != file:
            problems.append(f"V{version:03d} was applied as {file} but the file is now {migration.file}")
        elif migration.checksum != checksum:
            problems.append(f"{file} changed after it was applied; write a new migration instead of editing it")
    if problems:
        raise ValueError("migration history doesn't match src/ddl/migrations:\n  " + "\n  ".join(problems))
    return [migration for migration in migrations if migration.version not in applied]


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


def render_bronze_migration(version: int, target: str, source_table: str, mode: str, columns: list[tuple[str, str]]) -> tuple[str, str]:
    """Builds the migration file (name, content) for a bronze table from its source table's columns.

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
    return f"V{version:03d}__create_bronze_{target}.sql", sql
