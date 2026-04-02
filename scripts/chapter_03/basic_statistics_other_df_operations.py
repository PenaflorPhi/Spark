from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession


def _find_dataset(filename: str = "sf-fire-calls.csv") -> str:
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


def load_dataframe(spark: SparkSession) -> DataFrame:
    path = _find_dataset()
    return spark.read.option("samplingRatio", 0.1).option("header", True).csv(path)


def slug_columns(df: DataFrame, space="_") -> DataFrame:
    for col in df.columns:
        df = df.withColumnRenamed(col, col.replace(" ", space))

    return df


def convert_time_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "Incident_Date", F.to_timestamp(F.col("Incident_Date"), "yyyy/MM/dd")
        )
        .withColumn(
            "Alarm_DtTm", F.to_timestamp(F.col("Alarm_DtTm"), "yyyy/MM/dd hh:mm:ss a")
        )
        .withColumn(
            "Arrival_DtTm",
            F.to_timestamp(F.col("Arrival_DtTm"), "yyyy/MM/dd hh:mm:ss a"),
        )
    )


def basic_stats(df: DataFrame) -> None:
    df.select(
        F.col("Alarm_DtTm"),
        F.col("Arrival_DtTm"),
        F.col("Number_of_Alarms").cast("int").alias("Number_of_Alarms"),
        F.regexp_replace(F.col("Estimated_Property_Loss"), ",", "")
        .cast("double")
        .alias("Estimated_Property_Loss")
        .cast("double")
        .alias("Estimated_Property_Loss"),
    ).withColumn(
        "ResponseDelayInMins",
        (F.unix_timestamp("Arrival_DtTm") - F.unix_timestamp("Alarm_DtTm")) / 60,
    ).select(
        F.sum("Number_of_Alarms"),
        F.avg("ResponseDelayInMins"),
        F.min("ResponseDelayInMins"),
        F.max("ResponseDelayInMins"),
        F.sum("Estimated_Property_Loss"),
    ).show()


def main() -> None:
    spark = SparkSession.builder.appName("csv_autoschema").getOrCreate()
    df = load_dataframe(spark)

    df = slug_columns(df)
    df = convert_time_columns(df)
    basic_stats(df)


if __name__ == "__main__":
    main()
