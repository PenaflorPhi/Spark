from pathlib import Path

from pyspark.sql import SparkSession


def get_spark_session(appName: str) -> SparkSession:
    return SparkSession.builder.appName(appName).getOrCreate()


def _find_dataset(filename: str) -> str:
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "datasets").is_dir():
            matches = list((current / "datasets").rglob(filename))
            if matches:
                return str(matches[0])
        current = current.parent
    raise FileNotFoundError(
        f"{filename} not found in any 'datasets/' directory above {Path(__file__)}"
    )


def load_dataset(spark: SparkSession, path: str, format: str = "csv"):
    # Read the CSV into a Spark DataFrame, inferring column types from the data.
    return (
        spark.read.format(format)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    )


def create_and_use_db(spark: SparkSession, db_name: str) -> None:
    spark.sql(f"CREATE DATABASE {db_name}")
    spark.sql(f"USE {db_name}")


if __name__ == "__main__":
    spark = get_spark_session("SparkSQLExampleApp")
    csv_path = _find_dataset("departuredelays.csv")
    df = load_dataset(spark, csv_path, "csv")
    create_and_use_db(spark, "learn_spark_db")

    # Creating managed table
    spark.sql("""
        CREATE TABLE managed_us_delay_flights_tbl (
              date STRING,
              delay INT,
              distance INT,
              origin STRING,
              destination STRING
              )
        """)

    # Creating unmanged table
    # NOTE: If we use the `csv_path` as the path it will try to overwrite
    # the file with a directory of the same name (initial expectation)
    # was for everything to be saved elsewhere with the name of the CSV
    df.write.option("path", f"{csv_path}_tbl").saveAsTable("us_delay_flights_tbl")
