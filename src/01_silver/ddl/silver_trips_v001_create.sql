-- Silver trips: one row per trip, cleaned and typed. Written by src/01_silver/clean_trips.py (insert-only MERGE).
CREATE TABLE IF NOT EXISTS `${catalog}`.`${silver_schema}`.trips (
  trip_id               STRING        NOT NULL COMMENT 'SHA-256 of the six source columns',
  pickup_at             TIMESTAMP     NOT NULL,
  dropoff_at            TIMESTAMP     NOT NULL,
  pickup_date           DATE          NOT NULL,
  trip_duration_minutes DOUBLE        NOT NULL,
  trip_distance_miles   DOUBLE        NOT NULL,
  fare_amount           DECIMAL(10,2) NOT NULL,
  pickup_zip            STRING        NOT NULL COMMENT '5-character ZIP code, zero-padded',
  dropoff_zip           STRING        NOT NULL COMMENT '5-character ZIP code, zero-padded',
  _batch_id             STRING        NOT NULL COMMENT 'Bronze batch the trip was first loaded in',
  _ingested_at          TIMESTAMP     NOT NULL COMMENT 'When that bronze batch was loaded',
  _merged_at            TIMESTAMP     NOT NULL COMMENT 'When silver inserted the row'
)
COMMENT 'NYC taxi trips: deduplicated, cleaned and typed, one row per trip_id';
