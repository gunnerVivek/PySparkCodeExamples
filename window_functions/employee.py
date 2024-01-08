"""
1. Department wise salary rank
2. avg salary per department
3. Highest salary department wise
4. comparative salary within dept
"""

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([
    StructField("emp_id", StringType())
    , StructField("name", StringType())
    , StructField("department", StringType())
    , StructField("salary", FloatType())
])

input_path = r"data/employee_data.csv"

spark = SparkSession.builder.appName("").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")  # read everything as string type; need to clean
    .load(input_path)
)

df = df.withColumn("salary", split("salary", "\$")[1].cast("float"))

# df.show(10)

# df = spark.createDataFrame(df.rdd, schema=schema)

windowSpec = Window.partitionBy("department")

df.select("*"
          , dense_rank().over(windowSpec.orderBy(desc("salary"))).alias("intra_dept_rank")
          , avg("salary").over(windowSpec).alias("avg_sal_dept")
          , sum("salary").over(windowSpec).alias("sum_sal_dept")
          , max("salary").over(windowSpec).alias("max_sal_dept")
          ) \
    .select("*", round(col("salary")/col("avg_sal_dept"), 2).alias("comp_sal_dept"))\
    .show(10)

spark.stop()

# http://192.168.100.1/login?dst=http://nmcheck.gnome.org/
# http://103.172.150.41/user/index.php
