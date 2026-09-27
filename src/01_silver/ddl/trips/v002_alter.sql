-- Silver takes a MERGE on every run, which is the classic small-file producer: see alter_bronze_nyctaxi_trips_v002.sql
-- for what each property does and when it acts.
ALTER TABLE `${catalog}`.`${silver_schema}`.trips SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
