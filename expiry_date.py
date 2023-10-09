
"""
Main points:
    use date_add() is used to add to the recharge date to find expiry date
     The recharge dat is provided as int in yyyyMMdd format. We need to
     convert it string for date_add() and also need to provide custom yyyyMMdd
     format. by default yyyy-MM-dd is expected.

     date_add(col, int) is expected but when using a column name for days to add
     we need to use expr() so that we can provide col instead of int.

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

input_path = r"data/expiry_date.csv"

spark = SparkSession.builder.appName("k").getOrCreate()

# inferSchema is necessary otherwise every column will be loaded as String type
data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input_path)
data.printSchema()
# data.show()

# calculate expiry date
out_df = (data
    .select("*", to_date(col("RechargeDate").cast("string"), "yyyyMMdd").alias("RechargeDate_s"))  # convert date from int to date format
    .select("*", expr("date_add(RechargeDate_s, ValidDays)").alias("ExpiryDate"))
    .drop("RechargeDate_s")
)

# out_df.explain()

out_df.show()

