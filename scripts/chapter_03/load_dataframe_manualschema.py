from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

fire_schema = StructType(
    [
        StructField("Incident Number", IntegerType(), True),
        StructField("Exposure Number", IntegerType(), True),
        StructField("ID", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("Incident Date", TimestampType(), True),
        StructField("Call Number", IntegerType(), True),
        StructField("Alarm DtTm", TimestampType(), True),
        StructField("Arrival DtTm", TimestampType(), True),
        StructField("Close DtTm", TimestampType(), True),
        StructField("City", StringType(), True),
        StructField("zipcode", IntegerType(), True),
        StructField("Battalion", StringType(), True),
        StructField("Station Area", StringType(), True),
        StructField("Box", StringType(), True),
        StructField("Suppression Units", IntegerType(), True),
        StructField("Suppression Personnel", IntegerType(), True),
        StructField("EMS Units", IntegerType(), True),
        StructField("EMS Personnel", IntegerType(), True),
        StructField("Other Units", IntegerType(), True),
        StructField("Other Personnel", IntegerType(), True),
        StructField("First Unit On Scene", StringType(), True),
        StructField("Estimated Property Loss", FloatType(), True),
        StructField("Estimated Contents Loss", FloatType(), True),
        StructField("Fire Fatalities", IntegerType(), True),
        StructField("Fire Injuries", IntegerType(), True),
        StructField("Civilian Fatalities", IntegerType(), True),
        StructField("Civilian Injuries", IntegerType(), True),
        StructField("Number of Alarms", IntegerType(), True),
        StructField("Primary Situation", StringType(), True),
        StructField("Mutual Aid", StringType(), True),
        StructField("Action Taken Primary", StringType(), True),
        StructField("Action Taken Secondary", StringType(), True),
        StructField("Action Taken Other", StringType(), True),
        StructField("Detector Alerted Occupants", StringType(), True),
        StructField("Property Use", StringType(), True),
        StructField("Area of Fire Origin", StringType(), True),
        StructField("Ignition Cause", StringType(), True),
        StructField("Ignition Factor Primary", StringType(), True),
        StructField("Ignition Factor Secondary", StringType(), True),
        StructField("Heat Source", StringType(), True),
        StructField("Item First Ignited", StringType(), True),
        StructField("Human Factors Associated with Ignition", StringType(), True),
        StructField("Structure Type", StringType(), True),
        StructField("Structure Status", StringType(), True),
        StructField("Floor of Fire Origin", IntegerType(), True),
        StructField("Fire Spread", StringType(), True),
        StructField("No Flame Spread", StringType(), True),
        StructField("Number of floors with minimum damage", IntegerType(), True),
        StructField("Number of floors with significant damage", IntegerType(), True),
        StructField("Number of floors with heavy damage", IntegerType(), True),
        StructField("Number of floors with extreme damage", IntegerType(), True),
        StructField("Detectors Present", StringType(), True),
        StructField("Detector Type", StringType(), True),
        StructField("Detector Operation", StringType(), True),
        StructField("Detector Effectiveness", StringType(), True),
        StructField("Detector Failure Reason", StringType(), True),
        StructField("Automatic Extinguishing System Present", StringType(), True),
        StructField("Automatic Extinguishing Sytem Type", StringType(), True),
        StructField("Automatic Extinguishing Sytem Perfomance", StringType(), True),
        StructField("Automatic Extinguishing Sytem Failure Reason", StringType(), True),
        StructField("Number of Sprinkler Heads Operating", IntegerType(), True),
        StructField("Supervisor District", StringType(), True),
        StructField("neighborhood_district", StringType(), True),
        StructField("point", StringType(), True),
        StructField("data_as_of", TimestampType(), True),
        StructField("data_loaded_at", TimestampType(), True),
    ]
)


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


def load_dataframe(spark: SparkSession) -> DataFrame:
    path = _find_dataset()
    return spark.read.csv(path, header=True, schema=fire_schema)


def main() -> None:
    spark = SparkSession.builder.appName("csv_manualschema").getOrCreate()
    df = load_dataframe(spark)
    df.show()


if __name__ == "__main__":
    main()
