from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_timestamp, year


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
        df.withColumn("Incident_Date", to_timestamp(col("Incident_Date"), "yyyy/MM/dd"))
        .withColumn(
            "Alarm_DtTm", to_timestamp(col("Alarm_DtTm"), "yyyy/MM/dd hh:mm:ss a")
        )
        .withColumn(
            "Arrival_DtTm", to_timestamp(col("Arrival_DtTm"), "yyyy/MM/dd hh:mm:ss a")
        )
    )


def select_columns(df: DataFrame) -> DataFrame:
    return df.select("Incident_Date", "Alarm_DtTm", "Arrival_DtTm")


def years_worth_of_info(df: DataFrame) -> DataFrame:
    return df.select(year("Incident_Date")).distinct().orderBy(year("Incident_Date"))


def main() -> None:
    spark = SparkSession.builder.appName("csv_autoschema").getOrCreate()

    # Schema right after loading
    df = load_dataframe(spark)
    df.printSchema()

    # Schema after modifying the column names
    df = slug_columns(df)
    df.printSchema()

    # Schema after modifying the types of two columns
    df = convert_time_columns(df)
    df.printSchema()

    ts_df = select_columns(df)
    ts_df.show(5, False)

    years = years_worth_of_info(df)
    years.show()


if __name__ == "__main__":
    main()
