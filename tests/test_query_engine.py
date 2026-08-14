import datetime

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from query_engine.filter import Filter
from query_engine.query_engine import execute_query
from stage2.stage2 import load_film_df



@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("solirius-tests")
        .getOrCreate()
    )

    yield spark#yield instead of return so execution returns to this point after the test is done and we can stop the spark session
    spark.stop()

@pytest.fixture(scope="session")
def film_df(spark_session):
    film_df = load_film_df(spark_session)
    return film_df


def test_equals_filter(film_df):
    query_filter = Filter(
        "title",
        "==",
        "Pulp Fiction",
    )

    result = execute_query(query_filter, film_df)
    assert [row.title for row in result.collect()] == [
        "Pulp Fiction"
    ]

def test_not_equals_filter(film_df):
    query_filter = Filter("title", "!=", "Pulp Fiction")

    result = execute_query(query_filter, film_df)
    assert "Pulp Fiction" not in [row.title for row in result.collect()]


def test_greater_than_filter(film_df):
    query_filter = Filter("release_year", ">", "2000-01-01")
    result = execute_query(query_filter, film_df)
    assert all(x > datetime.date(2000,1,1) for x in [row.release_year for row in result.collect()])

def test_less_than_filter(film_df):
    query_filter = Filter("release_year", "<", "1990-01-01")
    result = execute_query(query_filter, film_df)
    assert all(x < datetime.date(1990,1,1) for x in [row.release_year for row in result.collect()])


def test_greater_than_or_equal_filter(film_df):
    query_filter = Filter("release_year", ">=", "2000-01-01")
    result = execute_query(query_filter, film_df)
    assert all(x >= datetime.date(2000,1,1) for x in [row.release_year for row in result.collect()])


def test_less_than_or_equal_filter(film_df):
    query_filter = Filter("release_year", "<=", "2000-01-01")
    result = execute_query(query_filter, film_df)
    assert all(x <= datetime.date(2000,1,1) for x in [row.release_year for row in result.collect()])


def test_in_filter(film_df):
    query_filter = Filter("title", "in", ("Pulp Fiction", "The Matrix"))
    result = execute_query(query_filter, film_df)
    titles = [row.title for row in result.collect()]
    assert all(title in ("Pulp Fiction", "The Matrix") for title in titles)
    assert len(titles) == 2


def test_not_in_filter(film_df):
    query_filter = Filter("title", "not in", ("Pulp Fiction",))
    result = execute_query(query_filter, film_df)
    assert "Pulp Fiction" not in [row.title for row in result.collect()]


def test_contains_filter(film_df):
    query_filter = Filter("title", "contains", ("Star",))
    result = execute_query(query_filter, film_df)
    titles = [row.title for row in result.collect()]
    assert all("Star" in title for title in titles)


def test_between_filter(film_df):
    query_filter = Filter(
        "release_year",
        "between",
        ("1979-01-01", "1980-01-01"),#note: this will include movies released across 1980 as only year is tracked
    )

    result = execute_query(query_filter, film_df)

    assert set([row.title for row in result.collect()]) == set(['Star Wars: The Empire Strikes Back (Episode V)',
     'Herbie Goes Bananas',
     'Rocky II',
     'Friendship',
     'Popeye',
     'Raging Bull',
     'Lupin the 3rd: The Castle of Cagliostro: Special Edition',
     'Alibaba Aur 40 Chor',
     'Alexandria ... Why?',
     'Joni',
     'Unidentified Flying Oddball',
     'Richard Pryor: Live in Concert',
     'Human experiments',
     'The Apple Dumpling Gang Rides Again',
     'Noorie',
     'Mad Max',
     'Playing for Time',
     'Sultan and the Rock Star',
     'The Blue Lagoon',
     'The Great Gambler',
     'No Longer kids',
     'Sword Masters: Two Champions of Shaolin',
     "Monty Python's Life of Brian",
     'Khubsoorat',
     'The Muppet Movie',
     'The Black Hole',
     'The Long Riders',
     'Return To The 36th Chamber',
     'Whispers',
     'Gol Maal',
     'The Ghosts of Buxley Hall'])


def test_cast_to_type(film_df):
    query_filter = Filter("release_year", ">=", "1980-01-01")