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
    print(f"Dataset found at: {csv_path}")

    df = load_dataset(spark, csv_path, "csv")
    print(f"Dataset loaded: {df.count()} rows")

    df.createOrReplaceTempView("us_delay_flights_tbl")
    print("Registered df as temp view: us_delay_flights_tbl")

    print("\n--- Catalogs ---")
    for c in spark.catalog.listCatalogs():
        print(f"  {c.name}")

    print("\n--- Databases ---")
    for db in spark.catalog.listDatabases():
        print(f"  {db.name} (location: {db.locationUri})")

    print("\n--- Tables ---")
    for t in spark.catalog.listTables():
        print(f"  {t.name} | type: {t.tableType} | isTemporary: {t.isTemporary}")

    print("\n--- Columns: us_delay_flights_tbl ---")
    for col in spark.catalog.listColumns("us_delay_flights_tbl"):
        print(f"  {col.name} | type: {col.dataType} | nullable: {col.nullable}")
