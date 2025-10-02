from __future__ import annotations
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark() -> SparkSession: # type: ignore
    spark = (
        SparkSession.builder.appName("pytest-claims-etl")
        .master("local[2]")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield spark
    spark.stop()