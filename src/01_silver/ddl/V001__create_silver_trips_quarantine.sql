-- Trips rejected by silver validation, kept as received from bronze plus the rules they broke.
-- Written by src/01_silver/clean_trips.py (insert-only MERGE).
CREATE TABLE IF NOT EXISTS `${catalog}`.`${silver_schema}`.trips_quarantine (
  trip_id               STRING        NOT NULL COMMENT 'SHA-256 of the six source columns',
  rejection_reasons     ARRAY<STRING> NOT NULL COMMENT 'Names of the validation rules the trip broke',
  tpep_pickup_datetime  TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  trip_distance         DOUBLE,
  fare_amount           DOUBLE,
  pickup_zip            INT,
  dropoff_zip           INT,
  _batch_id             STRING        NOT NULL COMMENT 'Bronze batch the trip was first loaded in',
  _ingested_at          TIMESTAMP     NOT NULL COMMENT 'When that bronze batch was loaded',
  _merged_at            TIMESTAMP     NOT NULL COMMENT 'When silver quarantined the row'
)
COMMENT 'NYC taxi trips rejected by silver validation, as received from bronze, one row per trip_id';
