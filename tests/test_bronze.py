from pyspark.sql import functions as F

from medallion.bronze import add_ingestion_metadata


def test_source_columns_stay_untouched_and_every_row_gets_the_same_batch_metadata(spark):
    raw = spark.createDataFrame([(1, "a"), (2, None)], "id int, value string")

    bronze = add_ingestion_metadata(raw, "batch-1", "samples.tpch.region", source_file=F.lit("part-0.parquet"))
    rows = bronze.collect()

    assert bronze.columns == ["id", "value", "_batch_id", "_ingested_at", "_source_table", "_source_file"]
    assert [(row.id, row.value) for row in rows] == [(1, "a"), (2, None)]
    assert {(row._batch_id, row._source_table, row._source_file) for row in rows} == {("batch-1", "samples.tpch.region", "part-0.parquet")}
    assert all(row._ingested_at is not None for row in rows)
