-- Small-file prevention and cure for the one bronze table that gets a new batch on every run.
--   optimizeWrite  acts BEFORE the write: adds a shuffle so a batch lands as a few large files instead of one per task
--   autoCompact    acts AFTER the commit: if the write left many small files, it runs a small OPTIMIZE right there
-- Neither reorders rows, so neither restores clustering; only OPTIMIZE does (see the maintain_tables job).
ALTER TABLE `${catalog}`.`${bronze_schema}`.nyctaxi_trips SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
