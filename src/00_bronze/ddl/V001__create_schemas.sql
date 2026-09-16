-- The medallion layer schemas. The catalog itself is created by scripts/setup_unity_catalog.sh, because Free
-- Edition can't create a catalog through the API. Not a bundle `schema` resource: development mode would rename it to
-- dev_<user>_<name>, and `bundle destroy` drops it together with its tables (tested, see docs/PLAN.md step 8).
CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${bronze_schema}` COMMENT 'Raw data as ingested, plus ingestion metadata';
CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${silver_schema}` COMMENT 'Cleaned, typed, deduplicated entities';
CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${gold_schema}` COMMENT 'Aggregates and scorecards ready for analysis';
