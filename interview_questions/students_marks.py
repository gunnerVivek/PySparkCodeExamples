"""
Find best three marks of each of the students and get avg of these best
marks for each of the students.

First we will need to un-pivot (melt)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from typing import Iterable


input_path = r"data/students_marks.csv"

spark = SparkSession.builder.appName("").getOrCreate()

df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load(input_path)

df.show()

def melt_df(df: DataFrame
            , id_vars: Iterable[str], value_vars: Iterable[str]
            , var_name: str, value_name: str
            ):
    '''
    :param df:
    :param id_vars: column to anchor the melt operation on
    :param value_vars: existing column names to melt
    :param var_name: new categorical column
    :param value_name: column to hold value for the new categorical column
    :return: Melted df
    '''

    str_vars = ",".join([f"'{c}',{c}" for c in value_vars])
    num_vars = len(value_vars)
    stack_expr = f"stack({num_vars},{str_vars}) as ({var_name},{value_name})"

    return df.select(','.join(id_vars), expr(stack_expr))

id_vars = ['Student']
value_vars = ['English','Maths','Science','Geography','History']
melt_col_category = "subject"
melt_col_value = "marks"


df = melt_df(df, id_vars, value_vars, melt_col_category, melt_col_value)

windowSpec_rank = Window.partitionBy("Student").orderBy(desc("marks"))
windowSpec_avg = Window.partitionBy("Student")

df = df.withColumn("rank", rank().over(windowSpec_rank))\
    .filter(col("rank") <= 3)\
    .withColumn("avg_top_3", avg("marks").over(windowSpec_avg))\
    .groupBy("Student").agg(round(avg("marks"), 2).alias("avg_top_3"))\
    .distinct()

# this is equivalent to the group by above
# .select(col("Student"), round(col("avg_top_3"),2))\

df.show()

spark.stop()
