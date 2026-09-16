"""Python-defined bundle resources: one ingestion job per source in config/sources.toml.

`databricks bundle validate/deploy` calls load_resources (see `python:` in databricks.yml). Generating the jobs keeps
the config file the single source of truth: adding a source adds a job, with no per-source YAML to write.
"""

import sys
from pathlib import Path

from databricks.bundles.core import Bundle, Resources
from databricks.bundles.jobs import Job

# Reuse the same validated loader as the notebooks, so a bad config fails the deploy.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from medallion.sources import load_sources  # noqa: E402


def load_resources(bundle: Bundle) -> Resources:
    resources = Resources()
    for source in load_sources():
        resources.add_job(
            f"ingest_{source.name}",
            Job.from_dict(
                {
                    "name": f"ingest_{source.name}",
                    "description": f"Bronze: {source.table} -> {source.name} ({source.mode}). Generated from config/sources.toml",
                    "tags": {"layer": "bronze", "source": source.name},
                    "schedule": {"quartz_cron_expression": source.schedule, "timezone_id": "UTC"},
                    "max_concurrent_runs": 1,
                    # Job parameters become notebook widgets. `source` is a parameter too, so a run can be started
                    # by hand from the UI with the same settings the schedule uses.
                    "parameters": [
                        {"name": "source", "default": source.name},
                        {"name": "catalog", "default": "${var.catalog}"},
                        {"name": "bronze_schema", "default": "${var.bronze_schema}"},
                    ],
                    # No cluster settings: serverless, the only option on Free Edition.
                    "tasks": [{"task_key": "ingest", "notebook_task": {"notebook_path": "src/00_bronze/ingest.py"}}],
                }
            ),
        )
    return resources
