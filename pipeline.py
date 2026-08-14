import luigi
from pyspark.sql import SparkSession
import pathlib
import stage1.stage1 as stage1
import stage2.stage2 as stage2

project_root = pathlib.Path(__file__).resolve().parent

def create_spark_session():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("solirius-film-pipeline")
        .getOrCreate()
    )


class Stage1FilmsTask(luigi.Task):
    """
    Loads the CSV, transforms the film data, applies the supplied
    schema and writes the result as Parquet.
    """
    def output(self, films_path=project_root / "output" / "films.parquet"):
        return luigi.LocalTarget(str(films_path / "_SUCCESS"))

    def run(self):
        spark_session = create_spark_session()
        schema_path = project_root / "resources" / "json" / "allFilesSchema.json"
        csv_path = project_root / "resources" / "csv" / "allFilms.csv"
        films_path = project_root / "output" / "films.parquet"

        try:
            schema = stage1.load_schema(schema_path)
            film_df = stage1.load_csv(csv_path, spark_session)
            film_df = stage1.transform_films(film_df)
            film_df = stage1.apply_schema(film_df, schema)
            stage1.save_df(film_df, str(films_path))

        finally:
            spark_session.stop()

class Stage2FilmsTask(luigi.Task):
    def requires(self):
        return Stage1FilmsTask()
    def output(self):
        return luigi.LocalTarget(project_root / "output" / "genres " / "_PIPELINE_SUCCESS")

    def run(self):
       spark_session = create_spark_session()

       try:
           film_df = stage2.load_film_df(spark_session)
           genre_df = stage2.create_temp_genre_df(film_df)
           stage2.save_genre_df(genre_df, "../output/genres")
       finally:
           spark_session.stop()


class FilmPipeline(luigi.WrapperTask):
    def requires(self):
        return Stage2FilmsTask()


if __name__ == "__main__":
    luigi.build(
        [FilmPipeline()],
        local_scheduler=True,
        detailed_summary=True,
    )