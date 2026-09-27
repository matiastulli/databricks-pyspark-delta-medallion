-- Naming convention: gold aggregates are agg_<subject>_<grain>.
ALTER TABLE `${catalog}`.`${gold_schema}`.daily_trips RENAME TO `${catalog}`.`${gold_schema}`.agg_trips_daily;
