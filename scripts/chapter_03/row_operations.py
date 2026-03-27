from pyspark.sql import Row, SparkSession
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


def display_row() -> None:
    df_row = Row(
        Id=6,
        First="Reynold",
        Last="Xin",
        URL="https://tinyurl.6",
        Hits=25568,
        Published="3/2/2015",
        Campaign=["twitter", "LinkedIn"],
    )
    print(df_row["First"])
    print(df_row[0])


def row_dataframe_creation(spark: SparkSession) -> None:
    rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
    authors_df = spark.createDataFrame(rows, ["Author", "State"])
    authors_df.show()


def main():
    spark = SparkSession.builder.appName("schema_definition_ddl").getOrCreate()

    blogs_df = generate_blog_df(spark)
    blogs_df.show()
    print(blogs_df.printSchema())

    display_row()
    row_dataframe_creation(spark)


if __name__ == "__main__":
    main()
