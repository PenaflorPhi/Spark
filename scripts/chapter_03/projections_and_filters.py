from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct


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


# Column names differ from the book's examples; the filter logic is equivalent.
def where_filter(df: DataFrame) -> None:
    df.select("Incident Number", "Alarm DtTm", "Battalion").where(
        col("Battalion") != "B03"
    ).show(5, truncate=False)


def aggregations(df: DataFrame) -> None:
    df.select("Battalion").where(col("Battalion").isNotNull()).agg(
        countDistinct("Battalion").alias("DistinctBattalions")
    ).show()


def show_distinct(df: DataFrame) -> None:
    df.select("Battalion").where(col("Battalion").isNotNull()).distinct().show(
        10, False
    )


def main() -> None:
    spark = SparkSession.builder.appName("csv_autoschema").getOrCreate()
    df = load_dataframe(spark)

    where_filter(df)
    aggregations(df)
    show_distinct(df)


if __name__ == "__main__":
    main()
