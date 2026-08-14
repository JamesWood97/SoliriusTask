from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

def load_film_df(spark_session):
    parquet_path = Path(__file__).resolve().parent.parent / "output" / "films.parquet"
    df = spark_session.read.parquet(str(parquet_path))
    return df

def create_temp_genre_df(film_df):
    genre_df = (
        film_df.withColumn(
        "particular_genre",
        F.explode(F.split(F.col("genres"), ","))
        )
        .withColumn(
            "particular_genre",
            F.trim(F.col("particular_genre"))
        )
    )

    return genre_df

def save_genre_df(genre_df, output_path):
    genres = [row["particular_genre"] for row in genre_df.select("particular_genre").distinct().collect()]

    for genre in genres:
        (
            genre_df
            .filter(F.col("particular_genre") == genre).drop("particular_genre")
            .write.mode("overwrite").parquet(output_path+"/"+genre+".parquet")
        )

def main(spark_session):
    film_df = load_film_df(spark_session)
    genre_df = create_temp_genre_df(film_df)
    save_genre_df(genre_df, "../output/genres")



if __name__ == "__main__":
    spark_session = SparkSession.builder \
        .appName("CSV Loader") \
        .getOrCreate()
    main(spark_session)