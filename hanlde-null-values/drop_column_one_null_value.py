
'''
Program to drop all those columns that have even a Single Null value in them.
Either NULL or Blank value (only White Spaces) is treated as NULL
'''

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession\
        .builder\
        .appName("Drop Even if Single NULL")\
        .master("local[2]")\
        .getOrCreate()

input_data = (
    spark.read.format("csv")
    .option("inferSchema", "true")
    .option("mode", "permissive")
    .option("header", "true")
    .load("data/null_values_data.csv")
)


def drop_col_even_single_null(df: DataFrame)->DataFrame:
    '''
    Drops a column even if there is a single Null in the column.

    :param df: Input DataFrame
    :return: The :param df DataFrame with all those columns that has even a
        single Null dropped
    '''

    null_counts = (df.select(
        [count(when(col(c).isNull() | trim(col(c)).eqNullSafe(""), c)).alias(c)
          for c in df.columns
        ])  # creates a single row DF with NUll count as value for each column
        .collect()[0].asDict()
    )

    # list of columns that have Null values
    cols_to_drop = [k for k,v in null_counts.items() if v>0]

    # returned DF with columns dropped
    return df.drop(*cols_to_drop)


# drop the columns with even a single Null value
null_count = input_data.transform(drop_col_even_single_null)
null_count.show()

spark.stop()
