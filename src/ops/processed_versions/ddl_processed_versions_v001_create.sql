-- Bookkeeping for incremental processing: the last source-table version each process has consumed.
-- Gold reads silver's change feed from here, so a run only sees what changed since the previous run.
-- Owned by tooling, like ops.schema_migrations, and written by the process that reads it.
CREATE TABLE IF NOT EXISTS `${catalog}`.ops.processed_versions (
  process        STRING    NOT NULL COMMENT 'The process that consumed the changes, e.g. build_trip_metrics',
  source_table   STRING    NOT NULL COMMENT 'The table whose change feed was read',
  last_version   BIGINT    NOT NULL COMMENT 'Highest source version already processed',
  updated_at     TIMESTAMP NOT NULL
)
COMMENT 'Last processed source version per incremental process';
