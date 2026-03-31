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
    return spark.read.option("samplingRatio", 0.1).option("header", True).csv(path)


def save_dataframe(df: DataFrame, format: str, path: Path, file_name: str) -> None:
    if not path.is_dir():
        path.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").format(format).save(str(path / file_name))


def main() -> None:
    spark = SparkSession.builder.appName("save_dataframe").getOrCreate()
    df = load_dataframe(spark)
    output_path = Path(_find_project_root()) / "output"
    save_dataframe(df, "parquet", output_path, "fires.parquet")
    save_dataframe(df, "csv", output_path, "fires.csv")


if __name__ == "__main__":
    main()
