from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import count


def _find_dataset(filename: str = "mnm_dataset.csv") -> str:
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


def explain(df: DataFrame) -> None:
    count_mnm_df = (
        df.select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )
    count_mnm_df.explain(True)


def main():
    spark = SparkSession.builder.appName("PythonMnMCount").getOrCreate()

    # Locate and load the dataset from the project tree.
    path = _find_dataset()
    mnm_df = load_dataset(spark, path)

    explain(mnm_df)

    spark.stop()


if __name__ == "__main__":
    main()
