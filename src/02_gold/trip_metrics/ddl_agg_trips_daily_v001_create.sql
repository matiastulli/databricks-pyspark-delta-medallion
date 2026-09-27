-- Gold scorecard: trips per pickup date. Rebuilt in full by src/02_gold/build_trip_metrics.py after its quality checks.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${gold_schema}`.daily_trips (
  pickup_date          DATE,
  trips                BIGINT,
  revenue              DECIMAL(20,2),
  avg_distance_miles   DOUBLE,
  avg_fare             DECIMAL(10,2),
  avg_duration_minutes DOUBLE
)
COMMENT 'NYC taxi trips per pickup date: trips, revenue and averages. Rebuilt from silver on every run';
