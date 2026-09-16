"""Bronze source config: loads config/sources.toml and rejects anything that could silently load the wrong data."""

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

# src/medallion/sources.py -> repo root. The bundle deploys config/ next to src/, so this works on Databricks too.
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "sources.toml"

MODES = {"append", "overwrite"}
FIELDS = {"name", "table", "target", "mode", "schedule"}
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
QUALIFIED_TABLE = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


@dataclass(frozen=True)
class Source:
    name: str
    table: str
    target: str
    mode: str
    schedule: str


def parse_sources(config: dict) -> list[Source]:
    """Validates a parsed config and returns its sources. Raises ValueError listing every problem found."""
    entries = config.get("sources")
    if not entries:
        raise ValueError("config has no [[sources]] entries")

    problems = []
    for position, entry in enumerate(entries, start=1):
        label = f"source #{position} ({entry.get('name', 'unnamed')})"
        if missing := FIELDS - entry.keys():
            problems.append(f"{label}: missing {sorted(missing)}")
        # An unknown key is almost always a typo (`mdoe`), and ignoring it would fall back to the wrong behavior.
        if unknown := entry.keys() - FIELDS:
            problems.append(f"{label}: unknown {sorted(unknown)}")
        for field in ("name", "target"):
            if field in entry and not IDENTIFIER.match(str(entry[field])):
                problems.append(f"{label}: {field} {entry[field]!r} must be lowercase letters, digits and underscores")
        if "table" in entry and not QUALIFIED_TABLE.match(str(entry["table"])):
            problems.append(f"{label}: table {entry['table']!r} must be catalog.schema.table")
        if "mode" in entry and entry["mode"] not in MODES:
            problems.append(f"{label}: mode {entry['mode']!r} must be one of {sorted(MODES)}")
        # Jobs use Quartz cron (6-7 fields, seconds first). A 5-field Unix cron is the usual mistake.
        if "schedule" in entry and len(str(entry["schedule"]).split()) not in (6, 7):
            problems.append(f"{label}: schedule {entry['schedule']!r} must be a Quartz cron with 6 or 7 fields")

    # Two sources writing the same target would silently mix unrelated data in one table.
    for field in ("name", "target"):
        values = [entry.get(field) for entry in entries if field in entry]
        if duplicates := sorted({value for value in values if values.count(value) > 1}):
            problems.append(f"duplicate {field}: {duplicates}")

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
