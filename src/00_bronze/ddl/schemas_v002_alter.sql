-- The layers now hold more than trips (TPC-H in bronze, quarantine in silver, scorecards in gold).
COMMENT ON SCHEMA `${catalog}`.`${bronze_schema}` IS 'Raw data as ingested, plus ingestion metadata';
COMMENT ON SCHEMA `${catalog}`.`${silver_schema}` IS 'Cleaned, typed, deduplicated entities';
COMMENT ON SCHEMA `${catalog}`.`${gold_schema}` IS 'Aggregates and scorecards ready for analysis';
