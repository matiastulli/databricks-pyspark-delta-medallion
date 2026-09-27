-- Same as silver trips: merged into on every run.
ALTER TABLE `${catalog}`.`${silver_schema}`.trips_quarantine SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
