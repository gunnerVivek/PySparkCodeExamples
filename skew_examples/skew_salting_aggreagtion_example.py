from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Salting Example").getOrCreate()

# setting for the demo
spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.set("spark.sql.adaptive.enabled", "false")

schema = StructType([StructField("key", IntegerType(), True)])

# create skewed DF
df0 = spark.createDataFrame([(0,)]*999990, schema).repartition(1)
df1 = spark.createDataFrame([(1,)]*15, schema).repartition(1)
df2 = spark.createDataFrame([(2,)]*10, schema).repartition(1)
df3 = spark.createDataFrame([(3,)]*5, schema).repartition(1)

df_skew = df0.unionAll(df1).unionAll(df2).unionAll(df3)

# df_skew.show()

# +---------+------+
# |partition| count|
# +---------+------+
# |        0|999990|
# |        1|    15|
# |        2|    10|
# |        3|     5|
# +---------+------+

# df_skew.withColumn("partition", f.spark_partition_id())\
#     .groupBy("partition")\
#     .count()\
#     .orderBy("partition")\
#     .show()

# this has been set to 3 for the experiment above
SALT_NUMBER = int(spark.conf.get("spark.sql.shuffle.partitions"))
SALT_NUMBER_MIN = 0

# post groupBy with salt, key the data gets distributed across partitions almost uniformly
df_skew\
    .withColumn("salt", f.floor(f.rand() * (SALT_NUMBER - SALT_NUMBER_MIN)) + SALT_NUMBER_MIN)\
    .groupBy("key", "salt")\
    .agg(f.count("key").alias("count"))\
    .groupby("key")\
    .agg(f.sum("count").alias("count"))\
    .show()


spark.stop()
