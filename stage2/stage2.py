from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def load_film_df(spark_session):
    df = spark_session.load.parquet("../films/films.parquet")
    return df

def create_temp_genre_df(film_df):
    genre_df = film_df.withColumn("particular_genre", F.explode(F.split(F.col("genres"), ",")))
    genre_df.show(n=10000, truncate=False)
    return genre_df

def main(spark_session):
    film_df = load_film_df(spark_session)
    genre_df = create_temp_genre_df(film_df)