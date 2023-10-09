
"""
Input is a text file where the record fields and record both are
comma delimited. ie. The records are not properly separated. We need to
split them into separate records based on every 5th delimiter (comma in this case)

Solution:
    Read the text file
    replace every 5th occurrence of the delimiter
    split each record into separate lines
    split into multiple columns
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

input_path = r"data/un-delimited-records.txt"

spark = SparkSession.builder.appName("").getOrCreate()

df = spark.read.format("text").load(input_path)

# replace every 5th occurrence of the delimiter
df = df.withColumn("replaced", regexp_replace("value", "(.*?,){5}", "$0-"))
# split each record into separate lines
df = df.withColumn("exploded", explode_outer(split("replaced", ",-")))

# split into multiple columns
schema = ["name", "education", "experience", "tech", "mobile"]
df = df.select("exploded").rdd.map(lambda x: x[0].split(","))\
    .toDF(schema)

df.show()

spark.stop()
