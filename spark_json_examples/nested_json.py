
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


parser = argparse.ArgumentParser(description="Command line arguments")
parser.add_argument("--input_path", required=True)
parser.add_argument("--appName", required=True)

args = parser.parse_args()

input_path = args.input_path
appName = args.appName

# master is coming from
spark = SparkSession.builder\
        .appName(appName)\
        .getOrCreate()

restaurant_json_data = spark.read.format("json")\
    .option("multiline", "true")\
    .option("inferSchema", "true")\
    .option("mode", "permissive")\
    .load("/home/vivek/User_Data/repositories/spark_code_examples/spark_json_examples/data/")

exploded_json = restaurant_json_data.select("*", f.explode_outer("restaurants").alias("new_restaurants"))

# exploded_json.cache()

# code, message, results_found, results_shown, results_start, status, res_id, establishment_types, name
final_data = (
    restaurant_json_data
    .select("code", "message", "results_found", "results_start", "status"
            , f.explode_outer("restaurants").alias("exploded_restaurants")
    )
    .select(
        "*"
        , "exploded_restaurants.restaurant.R.res_id"
        , "exploded_restaurants.restaurant.establishment_types"
        , "exploded_restaurants.restaurant.name"
    )  # example command
    .drop("exploded_restaurants")
)  # this pair of braces allow us to stretch over multi-line

final_data.printSchema()
final_data.show(3)

spark.stop()
