
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("").getOrCreate()

schema = StructType([
    StructField("_corrupt_record", StringType())
    , StructField("name", StringType())
    , StructField("age", IntegerType())
    , StructField("city", StringType())
])

df = spark.read.format("csv").option("mode", "PERMISSIVE").option("header", "true") \
    .option("columnNameOfCorruptRecord", "_corrupt_record")\
    .schema(schema)\
    .load("data/corrupt_records.csv")

df.show(truncate=False)

spark.stop()
