-- Naming convention: bronze tables are <source_system>_<source_table>. Data and Delta history move with the table.
ALTER TABLE `${catalog}`.`${bronze_schema}`.trips RENAME TO `${catalog}`.`${bronze_schema}`.nyctaxi_trips;
