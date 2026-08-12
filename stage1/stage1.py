import csv
import json
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def load_schema(path):
    with open(path) as file:
        schema_dict = json.load(file)

    return StructType.fromJson(schema_dict)

def load_csv(csv_path, spark_session):
    csv_df = (
        spark_session.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(csv_path))
    )

    return csv_df

def transform_films(df):
    df = (df.withColumn("id", F.monotonically_increasing_id())
            .withColumn("durationMins", F.regexp_extract("duration", r"(\d+)", 1).cast("integer"))
            .drop("duration")
            .withColumn(
                "adult",
                F.col("adult")
            )
            .withColumn(
                "release_year",
                F.make_date(F.col("release_year"), F.lit(1), F.lit(1))
            )
        )
    return df

def apply_schema(df, schema):
    return df.select([F.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields])

def main(spark_session):
    schema = load_schema("../resources/json/allFilesSchema.json")
    df = load_csv("../resources/csv/allFilms.csv", spark_session)
    df = transform_films(df)
    df = apply_schema(df, schema)
    df.show()

if __name__ == "__main__":
    spark_session = SparkSession.builder \
        .appName("CSV Loader") \
        .getOrCreate()
    main(spark_session)

