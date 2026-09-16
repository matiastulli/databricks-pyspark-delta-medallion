-- Gold scorecard: pickup ZIP codes ranked by trips. Rebuilt in full by src/02_gold/build_trip_metrics.py after its
-- quality checks. (pickup_zip's comment was inherited from silver when the table was first written; kept as is.)
CREATE TABLE IF NOT EXISTS `${catalog}`.`${gold_schema}`.busiest_pickup_zones (
  rank       INT,
  pickup_zip STRING        COMMENT '5-character ZIP code, zero-padded',
  trips      BIGINT,
  revenue    DECIMAL(20,2),
  avg_fare   DECIMAL(10,2)
)
COMMENT 'NYC taxi pickup ZIP codes ranked by trips. Rebuilt from silver on every run';
