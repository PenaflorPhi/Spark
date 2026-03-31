from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum as spark_sum


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


def count_mnms(df: DataFrame):
    # Aggregate total M&M count by state and color across the full dataset,
    # ordered from highest to lowest total.
    return (
        df.select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(spark_sum("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )


def count_per_state(df: DataFrame, state: str = "CA"):
    # Filter to a single state, then aggregate totals by color.
    return (
        df.select("State", "Color", "Count")
        .where(df.State == state)
        .groupBy("State", "Color")
        .agg(spark_sum("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )


def main():
    spark = SparkSession.builder.appName("PythonMnMCount").getOrCreate()

    # Locate and load the dataset from the project tree.
    path = _find_dataset()
    mnm_df = load_dataset(spark, path)

    # Show aggregated counts for all states and colors.
    count_mnm_df = count_mnms(mnm_df)
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % count_mnm_df.count())

    # Show aggregated counts for California only.
    ca_count_mnm_df = count_per_state(mnm_df)
    ca_count_mnm_df.show(n=10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
