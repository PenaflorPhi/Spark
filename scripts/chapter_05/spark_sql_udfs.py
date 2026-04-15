import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType


def get_session(appName: str) -> SparkSession:
    return SparkSession.builder.appName(appName).getOrCreate()


def cubed(s: float) -> float:
    return s**3


@pandas_udf(LongType())
def cubed_udf(s: pd.Series) -> pd.Series:
    return s**3


if __name__ == "__main__":
    spark = get_session("udfs")
    spark.udf.register("cubed", cubed, LongType())
    spark.range(1, 9).createOrReplaceTempView("udf_test")
    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

    x = pd.Series([1, 2, 3])
    print(cubed_udf(x))

    df = spark.range(1, 4)
    df.select("id", cubed_udf(col("id"))).show()
