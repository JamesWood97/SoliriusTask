import json
import sys

from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from stage2.stage2 import load_film_df

def add_title_similarity_score(df, film_title):
    # Calculate the title similarity score based on the number of common words in the titles
    words_to_ignore = {"the", "a", "an", "and", "of", "in", "on", "at", "to", "for", "with", "by", "from"}
    film_title_words = set(film_title.lower().split())
    df = df.withColumn(
        "title_similarity_score",
        F.size(F.array_intersect(F.split(F.lower(F.col("title")), " "), F.array(*[F.lit(word) for word in film_title_words if word.lower() not in words_to_ignore])))
    )
    return df

def add_genre_similarity_score(df, film_genre):
    return add_similarity_score(df, "genres", film_genre)

def add_director_similarity_score(df, film_director):
    return add_similarity_score(df, "director", film_director)

def add_cast_similarity_score(df, film_cast):
    return add_similarity_score(df, "cast", film_cast)

def add_similarity_score(df, column, value_string_to_compare):
    words = [genre.strip().lower() for genre in value_string_to_compare.split(",")]
    print(words)
    #using jaccard index to measure similarity
    df = df.withColumn(
        f"{column}_similarity_score",
        F.size(F.array_intersect(
            F.transform(
            F.split(F.lower(F.col(column)), ","),
            lambda x: F.trim(x)
            ),
            F.transform(
            F.array(*[F.lit(word) for word in words]),
            lambda x: F.trim(x)
            )
        ))/
        F.size(F.array_union(
            F.transform(
            F.split(F.lower(F.col(column)), ","),
            lambda x: F.trim(x)
            ),
            F.transform(
                F.array(*[F.lit(word) for word in words]),
                lambda x: F.trim(x)
            )
        ))
    )
    return df

def generate_film_comparison_df(film_id, film_df):
    title_similarity_weight = 2
    genres_overlap_weight = 4
    director_overlap_weight = 1
    cast_overlap_weight = 2
    film = film_df.filter(F.col("id") == film_id).first()
    print(film)
    df = add_title_similarity_score(film_df, film["title"])
    df = add_genre_similarity_score(df, film["genres"])
    df = add_director_similarity_score(df, film["director"])
    df = add_cast_similarity_score(df, film["cast"])
    df = df.withColumn("total_similarity_score",
                       F.lit(title_similarity_weight) * F.col("title_similarity_score") +\
                    F.lit(genres_overlap_weight) * F.col("genres_similarity_score") +\
                    F.lit(director_overlap_weight) * F.col("director_similarity_score") +\
                    F.lit(cast_overlap_weight) * F.col("cast_similarity_score"))
    return df

if __name__ == "__main__":
    spark_session = SparkSession.builder \
        .appName("CSV Loader") \
        .getOrCreate()
    film_df = load_film_df(spark_session)
    comp_df = generate_film_comparison_df(4200, film_df)#760, 4200
    comp_df.select("id", "title", "genres", "title_similarity_score","genres_similarity_score", "director_similarity_score",
                   "cast_similarity_score", "total_similarity_score").orderBy(F.desc("total_similarity_score")).limit(10000).show(n=1100, truncate=False)
