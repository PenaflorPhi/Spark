from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def main():
    spark = SparkSession.builder.appName("schema").getOrCreate()
    schema = StructType(
        [
            StructField("author", StringType(), False),
            StructField("title", StringType(), False),
            StructField("pages", IntegerType(), False),
        ]
    )

    print(schema.simpleString())

    df = spark.createDataFrame([], schema)
    df.printSchema()


if __name__ == "__main__":
    main()
