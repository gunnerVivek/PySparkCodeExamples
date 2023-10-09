"""
The efficient way to check for Non-empty DataFrame is to
get head(1) of DF and check if data exist in it.
"""

from pyspark.sql import SparkSession

empty_csv_path = "data/empty_csv.csv"
non_empty_csv_path = "data/non_empty.csv"

spark = SparkSession.builder.appName("Empty DF Check").getOrCreate()

empty_df = spark.read.format("csv").option("header", "true").load(empty_csv_path)

if bool(empty_df.head(1)):  # This is the expected choice
    print("Empty DF is Not empty")
else:
    print("Empty DF is Empty")

non_empty_df = spark.read.format("csv").option("header", "true").load(non_empty_csv_path)

if bool(non_empty_df.head(1)):
    print("Non Empty DF is not Empty")
else:  # This is the expected choice
    print("Non Empty DF is Empty")

spark.stop()
