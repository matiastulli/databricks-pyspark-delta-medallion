-- Every health check's result, one row per check per run: the gold quality checks (build_trip_metrics) and the
-- freshness and quarantine checks (monitor_pipeline). Kept because a check that only raises is forgotten once the
-- run is gone; this is what a dashboard or an alert reads.
-- run_id joins to ops.job_runs (system.lakeflow). A run that serverless retries writes one set of rows per attempt.
--
-- delta.appendOnly makes Delta itself reject UPDATE and DELETE: results are history, and history isn't edited.
CREATE TABLE IF NOT EXISTS `${catalog}`.ops.check_results (
  job          STRING    NOT NULL COMMENT 'The job that ran the check, e.g. monitor_pipeline',
  run_id       STRING             COMMENT 'Job run id ({{job.run_id}}); null when run outside a job',
  check_name   STRING    NOT NULL COMMENT 'What was checked, e.g. nyctaxi_trips is fresh',
  table_name   STRING             COMMENT 'The table the check is about; null when it spans several',
  passed       BOOLEAN   NOT NULL,
  actual       STRING             COMMENT 'The value found, as text',
  expected     STRING             COMMENT 'The value or bound it had to meet, as text',
  checked_at   TIMESTAMP NOT NULL
)
COMMENT 'Health check results from every job, append-only'
TBLPROPERTIES ('delta.appendOnly' = 'true');
