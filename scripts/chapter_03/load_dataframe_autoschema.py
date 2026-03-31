from pathlib import Path

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


def main() -> None:
    spark = SparkSession.builder.appName("csv_autoschema").getOrCreate()
    df = load_dataframe(spark)
    df.show()


if __name__ == "__main__":
    main()
