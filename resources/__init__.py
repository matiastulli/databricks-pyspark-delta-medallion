"""Python-defined bundle resources: one ingestion job per source in config/sources.toml, and alerting on every job.

`databricks bundle validate/deploy` calls load_resources and add_alerting (see `python:` in databricks.yml). Generating
the jobs keeps the config file the single source of truth: adding a source adds a job, with no per-source YAML to write.
"""

import sys
from dataclasses import replace
from pathlib import Path

from databricks.bundles.core import Bundle, Resources, job_mutator
from databricks.bundles.jobs import Job, JobEmailNotifications, JobNotificationSettings, JobsHealthRules

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
                    "description": f"Bronze: {source.location} -> {source.name} ({source.kind}, {source.mode}). Generated from config/sources.toml",
                    "tags": {"layer": "bronze", "source": source.name, "kind": source.kind},
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
                    # A table source is copied by notebook_ingest.py; files are picked up incrementally by Auto Loader.
                    "tasks": [{"task_key": "ingest", "notebook_task": {"notebook_path": f"src/00_bronze/_ingestion/notebook_ingest{'_files' if source.kind == 'files' else ''}.py"}}],
                }
            ),
        )
    return resources


# Every job gets these unless it sets its own. Runs take a few minutes on this data, so 20 minutes means something is
# wrong (a backlog, a stuck stream) and an hour is a run that will never finish.
DURATION_WARNING_SECONDS = 20 * 60
TIMEOUT_SECONDS = 60 * 60


@job_mutator
def add_alerting(bundle: Bundle, job: Job) -> Job:
    """Emails ${var.alert_email} when any job fails or runs long, and stops runs that hang.

    A mutator runs on every job, YAML-defined and generated alike, so a new job can't forget its alerts. The address
    is a bundle variable set from .env (BUNDLE_VAR_alert_email), because this repo is public.

    Each setting is all or nothing: a job that sets its own email_notifications keeps exactly those, without the
    defaults merged in.
    """
    alert_to = ["${var.alert_email}"]
    return replace(
        job,
        email_notifications=job.email_notifications
        or JobEmailNotifications(on_failure=alert_to, on_duration_warning_threshold_exceeded=alert_to),
        # A run skipped because the previous one is still running (max_concurrent_runs: 1) isn't a failure.
        notification_settings=job.notification_settings or JobNotificationSettings(no_alert_for_skipped_runs=True),
        health=job.health
        or JobsHealthRules.from_dict({"rules": [{"metric": "RUN_DURATION_SECONDS", "op": "GREATER_THAN", "value": DURATION_WARNING_SECONDS}]}),
        timeout_seconds=job.timeout_seconds or TIMEOUT_SECONDS,
    )
