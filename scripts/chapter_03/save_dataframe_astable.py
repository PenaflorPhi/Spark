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


def _find_project_root() -> str:
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / ".git").exists() or (current / ".venv").exists():
            return str(current)
        current = current.parent
    raise FileNotFoundError(
        f"Project root not found (no .git or .venv) above {Path(__file__)}"
    )


def load_dataframe(spark: SparkSession) -> DataFrame:
    path = _find_dataset()
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def save_dataframe(
    df: DataFrame, spark: SparkSession, format: str, table_name: str
) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    df.write.format(format).mode("overwrite").saveAsTable(table_name)


def main() -> None:
    warehouse_path = str(Path(_find_project_root()) / "spark-warehouse")

    spark = (
        SparkSession.builder.appName("save_dataframe")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .enableHiveSupport()
        .getOrCreate()
    )

    df = load_dataframe(spark)
    save_dataframe(df, spark, "parquet", "fires")

    spark.sql("SELECT * FROM fires LIMIT 5").show()


if __name__ == "__main__":
    main()
