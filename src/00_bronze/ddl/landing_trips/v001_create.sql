-- Bronze table for the trips files landing in the `landing` volume (JSON, one file per pickup date).
-- Same shape as nyctaxi_trips: the source columns untouched, plus the ingestion metadata. What differs is how it is
-- loaded: Auto Loader reads only files it hasn't seen, tracked in its checkpoint, instead of re-reading a table.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${bronze_schema}`.landing_trips (
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  trip_distance DOUBLE,
  fare_amount DOUBLE,
  pickup_zip INT,
  dropoff_zip INT,
  _batch_id STRING,
  _ingested_at TIMESTAMP,
  _source_table STRING,
  _source_file STRING
)
COMMENT 'Raw trips from JSON files in the landing volume, loaded incrementally by Auto Loader, one _batch_id per load';
