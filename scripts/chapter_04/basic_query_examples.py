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


def distance_gt(spark: SparkSession, distance: int) -> None:
    spark.sql(f"""
    SELECT
        distance, origin, destination
    FROM
        us_delay_flights_tbl
    WHERE
        distance > {distance}
    ORDER BY
        distance DESC
    """).show(10)


def flights_between_with_delay(
    spark: SparkSession, origin: str, destination: str, delay: int
) -> None:
    spark.sql(f"""
    SELECT
        date, delay, origin, destination
    FROM
        us_delay_flights_tbl
    WHERE
        delay > {delay}
        AND ORIGIN = '{origin}'
        AND DESTINATION = '{destination}'
    ORDER BY
        delay DESC
    """).show(10)


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


def label_delays(spark: SparkSession) -> None:
    spark.sql("""
    SELECT
        delay,
        origin,
        destination,
        CASE
            WHEN delay > 360 THEN 'Very Long Delays'
            WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
            WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
            WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'
            WHEN delay = 0 THEN 'No delays'
            ELSE 'Early'
        END AS Flight_Delays
    FROM
        us_delay_flights_tbl
    ORDER BY
        origin, delay DESC
    """).show(10)


if __name__ == "__main__":
    spark = get_spark_session("SparkSQLExampleApp")
    csv_path = _find_dataset("departuredelays.csv")
    df = load_dataset(spark, csv_path, "csv")
    df = convert_dates(df)
    df.createOrReplaceTempView("us_delay_flights_tbl")

    distance_gt(spark, 1_000)
    flights_between_with_delay(spark, "SFO", "ORD", 120)

    get_distrubtion_per_day(spark)
    get_distribution_per_month(spark)

    label_delays(spark)
