from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("read-parquets").getOrCreate()
    file_path = "../../output/fires.parquet/part-00000-5bf7857a-5e47-40da-80c6-941d72f81b78-c000.snappy.parquet"

    df = spark.read.format("parquet").load(file_path)
    df.show()
