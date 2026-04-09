from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession


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


def distance_gt(df: DataFrame, distance: int) -> None:
    df.select("distance", "origin", "destination").where(
        F.col("distance") > distance
    ).orderBy(F.desc("distance")).show(10)


def flights_between_with_delay(
    df: DataFrame, origin: str, destination: str, delay: int
) -> None:
    df.select("date", "delay", "origin", "destination").where(
        F.col("delay") > delay
    ).where(F.col("origin") == origin).where(
        F.col("destination") == destination
    ).orderBy(F.desc("delay")).show(10)


def convert_dates(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "date",
        F.expr("to_timestamp(concat('2025', lpad(date, 8, '0')), 'yyyyMMddHHmm')"),
    )


def get_distrubtion_per_day(spark: SparkSession) -> None:
    spark.sql("""
    SELECT
        DAY(date) AS day,
        MONTH(date) AS month,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN delay > 0 then 1 ELSE 0 END) as delayed_flights
    FROM
        us_delay_flights_tbl
    GROUP BY
        MONTH(date), DAY(date)
    ORDER BY
        month, day
    """).show(31)


def get_distribution_per_month(spark: SparkSession) -> None:
    spark.sql("""
    SELECT
        MONTH(date) AS month,
        COUNT(*) as total_flights,
        SUM(CASE WHEN delay > 0 THEN 1 ELSE 0 END) as delayed_flights
    FROM
        us_delay_flights_tbl
    GROUP BY
        MONTH(date)
    ORDER BY
        month
    """).show(12)


def label_delays(df: DataFrame) -> DataFrame:
    return (
        df.select("delay", "origin", "destination")
        .withColumn(
            "Flight_Delays",
            F.when(F.col("delay") > 360, "Very Long Delays")
            .when(F.col("delay") > 120, "Long Delays")
            .when(F.col("delay") > 60, "Short Delays")
            .when(F.col("delay") > 0, "Tolerable Delays")
            .when(F.col("delay") == 0, "No Delays")
            .otherwise("Early"),
        )
        .orderBy("origin", F.col("delay").desc())
    )


if __name__ == "__main__":
    spark = get_spark_session("SparkSQLExampleApp")
    csv_path = _find_dataset("departuredelays.csv")
    df = load_dataset(spark, csv_path, "csv")
    df = convert_dates(df)
    df.createOrReplaceTempView("us_delay_flights_tbl")

    distance_gt(df, 1_000)
    flights_between_with_delay(df, "SFO", "ORD", 120)

    get_distrubtion_per_day(spark)
    get_distribution_per_month(spark)

    label_delays(df)
