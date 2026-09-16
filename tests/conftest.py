import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Plain local Spark: the functions under test only transform DataFrames, so Delta isn't needed here.
    session = (
        SparkSession.builder.master("local[1]")
        .appName("medallion-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
