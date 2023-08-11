"""
Program to drop all those columns whose entire column value is Null.
Either NULL or Blank value (only White Spaces) is treated as NULL
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame

spark = SparkSession.builder.appName("NullValuesColumn").getOrCreate()

# The last column is all Null
columns = StructType([
    StructField("dept_name", StringType(), True)
    , StructField("dept_id", StringType(), True)
    , StructField("salary", StringType(), True)
    , StructField("city", StringType(), True)
])

data = [("Finance",10,None,"   "),
        ("Marketing",20,None, ""),
        ("Sales",30,None, " "),
        ("IT",40,"","  ")
      ]

df = spark.createDataFrame(data=data, schema=columns)

df.show()


def drop_columns_all_null(df: DataFrame) -> DataFrame:
    """
    Finds amx of each column. If max is None than the column has only
    Null values.

    :param df: Input DataFrame to checked for Nulls
    :return : The Dataframe with column containing all Null value removed
    """
    rows_with_data = (df
        .select(*[when((col(c).rlike("^[\\s]*$")) | (col(c).eqNullSafe("")), None)
                .otherwise(col(c)).alias(c) for c in df.columns]
        ) # replace all only spaces or blank strings with NULL values
        .agg(*[max(c).alias(c) for c in df.columns]) # get max value for each column
        .take(1)[0] # Row(dept_name='Finance', dept_id='10', salary='125000')
    )

    cols_to_drop = [column for column, max_val in rows_with_data.asDict().items()
                    if max_val is None]
    new_df = df.drop(*cols_to_drop)

    return new_df


new_df = drop_columns_all_null(df)
new_df.show()

spark.stop()
