-- Naming convention: gold aggregates are agg_<subject>_<grain>; this one is one row per pickup ZIP, ranked.
ALTER TABLE `${catalog}`.`${gold_schema}`.busiest_pickup_zones RENAME TO `${catalog}`.`${gold_schema}`.agg_trips_by_pickup_zip;
