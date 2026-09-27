-- Change Data Feed: Delta records the row-level inserts, updates and deletes of each version, so a downstream process
-- can read *what changed* instead of re-reading the table. Gold uses it to rebuild only the days that moved.
-- Changes are recorded from this version onward; a reader asking for an earlier version gets an error, which is why
-- build_trip_metrics falls back to a full rebuild when it has no watermark yet.
ALTER TABLE `${catalog}`.`${silver_schema}`.trips SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
