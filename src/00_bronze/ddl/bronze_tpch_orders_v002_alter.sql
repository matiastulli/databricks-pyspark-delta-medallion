-- Liquid clustering on the filter column. Files then hold narrow o_orderdate ranges, so Delta's per-file min/max
-- statistics can skip whole files for a date filter. Measured before this change: a one-day filter read 3 of 3 files.
--
-- CLUSTER BY only changes the table's definition; existing data is clustered by the next OPTIMIZE (the maintain_tables
-- job runs OPTIMIZE FULL once for that). Liquid clustering can't be combined with partitioning or ZORDER.
ALTER TABLE `${catalog}`.`${bronze_schema}`.tpch_orders CLUSTER BY (o_orderdate);
