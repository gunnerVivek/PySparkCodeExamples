
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

input_path_1 = r"data/merge_df_1.csv"
input_path_2 = r"data/merge_df_2.csv"

spark = SparkSession.builder.appName("").getOrCreate()

# Schema: ame,ge
df_1 = spark.read.format("csv").option("header", "true").load(input_path_1)
# Schema: age,gender,name
df_2 = spark.read.format("csv").option("header", "true").load(input_path_2)

dfs = [df_1,df_2]

# Both the columns should have same number of columns AND
# order of all the columns in all the dataframes in the list should be
# the same for unionAll to work.
# final_df = reduce(DataFrame.unionAll, dfs)

# unionByName is used when columns are having same name
# and the columns don't need to be in same order in both DFs
# unionByName can be used even when different number of Columns or columns
# with different names are present. resulting DF will have columns with Nul value
# for the source DF with missing column
# using lambda allows us to use allowMissingColumns parameter
final_df = reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), dfs)

final_df.show()

spark.stop()
