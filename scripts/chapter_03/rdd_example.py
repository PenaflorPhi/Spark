from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("rdd_example").getOrCreate()
    spark_context = spark.sparkContext

    dataRDD = spark_context.parallelize(
        [("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)]
    )

    agesRDD = (
        dataRDD.map(lambda x: (x[0], (x[1], 1)))
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
        .map(lambda x: (x[0], x[1][0] / x[1][1]))
    )

    print(agesRDD.collect())

    spark.stop()


if __name__ == "__main__":
    main()
