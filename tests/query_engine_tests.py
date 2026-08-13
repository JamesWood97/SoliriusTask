import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from query_engine.filter import Filter
from query_engine.query_engine import execute_query



@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("solirius-tests")
        .getOrCreate()
    )

    yield spark

    spark.stop()

def test_between_filter(film_df):
    query = Filter(
        "release_year",
        "between",
        ("1979-01-01", "2000-12-31"),
    )

    result = execute_query(query, film_df)

    assert [row.title for row in result.collect()] == [
        "Pulp Fiction"
    ]
