-- A Unity Catalog volume for files arriving from outside the lakehouse. Files land under /Volumes/<catalog>/<schema>/
-- landing/<source>/, and Auto Loader picks them up incrementally, keeping its checkpoint under landing/_checkpoints/.
-- A volume is governed like a table (catalog.schema.volume), so this belongs in DDL like everything else.
CREATE VOLUME IF NOT EXISTS `${catalog}`.`${bronze_schema}`.landing
  COMMENT 'Files landing from outside the lakehouse, plus the Auto Loader checkpoints that read them';
