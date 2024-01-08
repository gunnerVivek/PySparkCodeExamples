
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Salting Example").getOrCreate()

# setting for the demo
spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.set("spark.sql.adaptive.enabled", "false")


schema = StructType([StructField("key", IntegerType(), True)])

# create an uniform data type
df_uniform = spark.createDataFrame([(i,) for i in range(1000000)], schema)

# show uniform data
# +---------+------+
# |partition| count|
# +---------+------+
# |        0|249856|
# |        1|249856|
# |        2|249856|
# |        3|250432|
# +---------+------+
#
# df_uniform.withColumn("partition", spark_partition_id())\
#     .groupBy("partition")\
#     .count()\
#     .orderBy("partition")\
#     .show()




# create skewed DF
df0 = spark.createDataFrame([(0,)]*999990, schema).repartition(1)
df1 = spark.createDataFrame([(1,)]*15, schema).repartition(1)
df2 = spark.createDataFrame([(2,)]*10, schema).repartition(1)
df3 = spark.createDataFrame([(3,)]*5, schema).repartition(1)

df_skew = df0.unionAll(df1).unionAll(df2).unionAll(df3)

# show skewed data
# +---------+------+
# |partition| count|
# +---------+------+
# |        0|999990|
# |        1|    15|
# |        2|    10|
# |        3|     5|
# +---------+------+
# df_skew.withColumn("partition", spark_partition_id())\
#     .groupBy("partition")\
#     .count()\
#     .orderBy("partition")\
#     .show()

# join uniform DF with skewed DF
# df_joined = df_skew.join(df_uniform, on="key", how="inner")

# show Data Skew after partition
# +---------+-------+
# |partition|  count|
# +---------+-------+
# |        0|1000005|
# |        1|     15|
# +---------+-------+
#
# df_joined.withColumn("partition", spark_partition_id())\
#     .groupBy("partition")\
#     .count()\
#     .orderBy("partition")\
#     .show()

#  --------------- SALTING -----------------------  #

# this has been set to 3 for the experiment above
SALT_NUMBER = int(spark.conf.get("spark.sql.shuffle.partitions"))
SALT_NUMBER_MIN = 0

# add the salt column to Skewed data (larger data)
df_skew = df_skew.withColumn("salt"
             , f.floor(f.rand() * (SALT_NUMBER - SALT_NUMBER_MIN)) + SALT_NUMBER_MIN
         )
df_skew.show()
# add array of salt numbers as column to uniform DF
# and add a column to explode the salt array
df_uniform = df_uniform\
    .withColumn("salt_array", f.array([f.lit(i) for i in range(SALT_NUMBER)]))\
    .withColumn("salt", f.explode(f.col("salt_array")))


# the data will be joined based on key(actual value) & salt
# this assures the join happens on the actual values
df_joined = df_skew.join(df_uniform, ["key", "salt"], 'inner')


## show uniform distribution on joining post salting
# +---+----+------+
# |key|salt| count|
# +---+----+------+
# |  0|   1|333952|
# |  0|   2|333394|
# |  0|   0|332644|
# |  1|   2|     6|
# |  2|   1|     5|
# |  1|   0|     5|
# |  3|   0|     4|
# |  1|   1|     4|
# |  2|   0|     3|
# |  2|   2|     2|
# |  3|   2|     1|
# +---+----+------+

# df_joined\
#     .groupBy("key", "salt")\
#     .count()\
#     .orderBy(["count", "key", "salt"], ascending=False)\
#     .show()

spark.stop()
