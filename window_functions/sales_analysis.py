"""
1. Rolling sales each day
2. Sales MoM
3. Sales loss or gain compared to last month in %
4. percentage of sales each month compared to last 6 months sales
"""



from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


input_path = r"data/sales_data.parquet"

spark = SparkSession.builder.appName("Sales Analysis").getOrCreate()

df = spark.read.format("parquet")\
    .option("inferSchema", "true")\
    .load(input_path)\
    .filter(col("Year")==lit("2014"))\
    .select("OrderDate", "Category", "City", "Country", "sales", "Year", "Month")

windowSpec = Window.partitionBy("Category").orderBy([asc(col("Year")), asc(col("Month"))])
# windowSpec_2 = windowSpec.rowsBetween(-5, Window.currentRow)

df.groupBy("Category", "Year", "Month").agg(sum("sales").alias("Sales"))\
    .select("*", lag("Sales", 1).over(windowSpec).alias("last_month_sales"))\
    .select("Category","Year","Month","Sales"
            , round(((col("Sales") - col("last_month_sales"))/col("Sales"))*100,2).alias("loss_gain")
            , round((col("Sales")/sum("Sales").over(
                            windowSpec.rowsBetween(-5, Window.currentRow)))*100, 2
                    ).alias("sales_comp_last6months")
            )\
    .show()


# df.printSchema()
# df.show(10)




# output_path = r"data/sales_data.parquet"
# df.write.format("parquet").partitionBy("Year", "Month").mode("overwrite").save(output_path)


spark.stop()
