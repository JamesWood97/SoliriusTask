import csv
import json
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession

def load_schema(path):
    with open(path) as file:
        schema_dict = json.load(file)

    return StructType.fromJson(schema_dict)

def load_csv(csv_path, schema, spark_session):
    csv_df = (
        spark_session.read
        .option("header", True)
        .schema(schema)
        .csv(str(csv_path))
    )

    return csv_df

def main(spark_session):
    schema = load_schema("../resources/json/allFilesSchema.json")
    csv_df = load_csv("../resources/csv/allFilms.csv", schema, spark_session)
    csv_df.show()


if __name__ == "__main__":
    spark_session = SparkSession.builder \
        .appName("CSV Loader") \
        .getOrCreate()
    main(spark_session)

