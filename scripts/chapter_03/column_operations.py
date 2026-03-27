import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def generate_blog_df(spark: SparkSession) -> DataFrame:
    schema = "`Id` INT, `First` STRING, `Last` STRING, `URL` STRING, `Published` STRING, `Hits` INT, `Campaign` ARRAY<STRING>"

    data = [
        [
            1,
            "Jules",
            "Damji",
            "http://tinyurl.1",
            "1/4/2025",
            4535,
            ["twitter", ":LinkedIn"],
        ],
        [
            2,
            "Brooke",
            "Wenig",
            "https://tinyurl.2",
            "5/5/2018",
            8908,
            ["twitter", "LinkedIn"],
        ],
        [
            3,
            "Denny",
            "Lee",
            "https://tinyurl.3",
            "6/7/2019",
            7659,
            ["web", "twitter", "FB", "LinkedIn"],
        ],
        [
            4,
            "Tathagata",
            "Das",
            "https://tinyurl.4",
            "5/12/2018",
            10568,
            ["twitter", "FB"],
        ],
        [
            5,
            "Matei",
            "Zaharia",
            "https://tinyurl.5",
            "5/14/2014",
            40578,
            ["web", "twitter", "FB", "LinkedIn"],
        ],
        [
            6,
            "Reynold",
            "Xin",
            "https://tinyurl.6",
            "3/2/2015",
            25568,
            ["twitter", "LinkedIn"],
        ],
    ]

    blogs_df = spark.createDataFrame(data, schema)
    return blogs_df


def column_operations(df: DataFrame) -> None:
    # Print all column names as a Python list
    print(df.columns)

    # Access a particular column with col() — returns a Column object
    id_col = F.col("Id")
    print(id_col)  # Column<'Id'>

    # Use an expression to compute a value
    df.select(F.expr("Hits * 2")).show(2)

    # Same result using col() — both approaches are equivalent
    df.select(F.col("Hits") * 2).show(2)

    # Add a new column "Big Hitters" based on a conditional expression
    df.withColumn("Big Hitters", F.expr("Hits > 10000")).show()

    # Concatenate First, Last, and Id into a new column "AuthorsId"
    df.withColumn(
        "AuthorsId", F.concat(F.expr("First"), F.expr("Last"), F.expr("Id"))
    ).select(F.col("AuthorsId")).show(4)

    # These three statements are equivalent — all select the same column
    df.select(F.expr("Hits")).show(2)  # using expr()
    df.select(F.col("Hits")).show(2)  # using col()
    df.select("Hits").show(2)  # using string shorthand

    # Sort by "Id" in descending order using col().desc()
    df.sort(F.col("Id").desc()).show()

    # "$Id" is Scala-specific $ string interpolation syntax — not valid in Python.
    # In Python, use col() or a string column name directly instead.
    df.sort(F.col("Id").desc()).show()


def main():
    spark = SparkSession.builder.appName("schema_definition_ddl").getOrCreate()

    blogs_df = generate_blog_df(spark)
    blogs_df.show()
    print(blogs_df.printSchema())

    column_operations(blogs_df)


if __name__ == "__main__":
    main()
