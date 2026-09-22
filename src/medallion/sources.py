"""Bronze source config: loads config/sources.toml and rejects anything that could silently load the wrong data."""

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

# src/medallion/sources.py -> repo root. The bundle deploys config/ next to src/, so this works on Databricks too.
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "sources.toml"

MODES = {"append", "overwrite"}
# A source is either a Unity Catalog table or files landing in a volume; each kind needs different settings.
FIELDS_BY_KIND = {
    "table": {"kind", "table", "mode", "schedule"},
    "files": {"kind", "volume", "path", "format", "mode", "schedule"},
}
FILE_FORMATS = {"json", "csv", "parquet"}
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
QUALIFIED_TABLE = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


def bronze_table_name(source_table: str) -> str:
    """Naming convention for bronze: <source_system>_<source_table>, e.g. samples.nyctaxi.trips -> nyctaxi_trips.

    Derived instead of written by hand, so it can't drift from the convention. The system prefix keeps two systems with
    a table of the same name from colliding.
    """
    _, system, table = source_table.split(".")
    return f"{system}_{table}"


@dataclass(frozen=True)
class Source:
    kind: str
    mode: str
    schedule: str
    table: str | None = None  # kind = "table"
    volume: str | None = None  # kind = "files"
    path: str | None = None
    format: str | None = None

    @property
    def name(self) -> str:
        """The bronze table name, which also names the ingestion job (ingest_<name>).

        Both kinds follow the same convention, <source_system>_<source_table>: for a table the system is its schema
        (samples.tpch.orders -> tpch_orders), for files it's the volume they land in (landing/trips -> landing_trips).
        """
        return bronze_table_name(self.table) if self.kind == "table" else f"{self.volume}_{self.path}"

    @property
    def location(self) -> str:
        """Where the data comes from: a table name, or the volume path Auto Loader watches."""
        return self.table if self.kind == "table" else f"/Volumes/{{catalog}}/{{bronze_schema}}/{self.volume}/{self.path}"


def parse_sources(config: dict) -> list[Source]:
    """Validates a parsed config and returns its sources. Raises ValueError listing every problem found."""
    entries = config.get("sources")
    if not entries:
        raise ValueError("config has no [[sources]] entries")

    problems, names = [], []
    for position, entry in enumerate(entries, start=1):
        label = f"source #{position} ({entry.get('table') or entry.get('path') or 'unnamed'})"
        kind = entry.get("kind")
        if kind not in FIELDS_BY_KIND:
            problems.append(f"{label}: kind {kind!r} must be one of {sorted(FIELDS_BY_KIND)}")
            continue
        fields = FIELDS_BY_KIND[kind]
        if missing := fields - entry.keys():
            problems.append(f"{label}: missing {sorted(missing)}")
        # An unknown key is almost always a typo (`mdoe`), and ignoring it would fall back to the wrong behavior.
        if unknown := entry.keys() - fields:
            problems.append(f"{label}: unknown {sorted(unknown)}")
        if kind == "table" and "table" in entry:
            if not QUALIFIED_TABLE.match(str(entry["table"])):
                problems.append(f"{label}: table {entry['table']!r} must be catalog.schema.table")
            elif not IDENTIFIER.match(name := bronze_table_name(entry["table"])):
                problems.append(f"{label}: bronze table name {name!r} must be lowercase letters, digits and underscores")
            else:
                names.append(name)
        if kind == "files":
            if entry.get("format") not in FILE_FORMATS:
                problems.append(f"{label}: format {entry.get('format')!r} must be one of {sorted(FILE_FORMATS)}")
            if "volume" in entry and "path" in entry:
                if not IDENTIFIER.match(name := f"{entry['volume']}_{entry['path']}"):
                    problems.append(f"{label}: bronze table name {name!r} must be lowercase letters, digits and underscores")
                else:
                    names.append(name)
            if entry.get("mode") == "overwrite":
                problems.append(f"{label}: files are ingested incrementally, so mode must be 'append'")
        if "mode" in entry and entry["mode"] not in MODES:
            problems.append(f"{label}: mode {entry['mode']!r} must be one of {sorted(MODES)}")
        # Jobs use Quartz cron (6-7 fields, seconds first). A 5-field Unix cron is the usual mistake.
        if "schedule" in entry and len(str(entry["schedule"]).split()) not in (6, 7):
            problems.append(f"{label}: schedule {entry['schedule']!r} must be a Quartz cron with 6 or 7 fields")

    # Two sources landing in the same bronze table would silently mix unrelated data.
    if duplicates := sorted({name for name in names if names.count(name) > 1}):
        problems.append(f"duplicate bronze tables: {duplicates}")

    if problems:
        raise ValueError("invalid sources config:\n  " + "\n  ".join(problems))
    return [Source(**entry) for entry in entries]


def load_sources(path: Path = DEFAULT_CONFIG_PATH) -> list[Source]:
    with open(path, "rb") as file:
        return parse_sources(tomllib.load(file))


def get_source(name: str, path: Path = DEFAULT_CONFIG_PATH) -> Source:
    sources = {source.name: source for source in load_sources(path)}
    if name not in sources:
        raise ValueError(f"unknown source {name!r}, expected one of {sorted(sources)}")
    return sources[name]
