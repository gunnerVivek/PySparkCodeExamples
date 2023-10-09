
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("").getOrCreate()

input_path = r"data/multiple_delimiters.csv"

### Spark 3.0 Solution
data = spark.read\
    .format("csv")\
    .option("delimiter", "~|")\
    .option("header", "true")\
    .load(input_path)

data.show()

#### Spark 2.4.4 Solution ########
# Spark 2 multiple delimiters was not allowed

# 1. read as text data
# this will produce single column - all columns truncated into one
# the resulting column will have column name as 'value'
data = spark.read.text(input_path)

# 2. get the Header from the first row
header = data.first()[0]

# 3. get schema form header
schema = header.split("~|")

# 4. For all records except first(header), get the data into seperate columns
data = data.filter(data['value']!=header).rdd.map(lambda x: x[0].split('~|')).toDF(schema)

data.show()

spark.stop()
