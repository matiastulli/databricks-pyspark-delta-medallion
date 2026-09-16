"""Bronze logic: the ingestion metadata every bronze table carries."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def add_ingestion_metadata(raw: DataFrame, batch_id: str, source_table: str, source_file: Column | None = None) -> DataFrame:
    """Keeps every source column as is and appends `_batch_id`, `_ingested_at`, `_source_table` and `_source_file`.

    `source_file` defaults to Spark's hidden `_metadata.file_path`, the data file each row was read from. Tests pass a
    literal instead, because DataFrames built in memory have no files behind them.
    """
    if source_file is None:
        source_file = F.col("_metadata.file_path")
    return raw.select(
        "*",
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(source_table).alias("_source_table"),
        source_file.alias("_source_file"),
    )
