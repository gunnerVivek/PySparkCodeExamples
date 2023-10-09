
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("").getOrCreate()

sc = spark.sparkContext

data = ['1,Vivek,2100000', '2,Mani,1800000', '3,Azhar,3100000']
data_rdd = sc.parallelize(data)

# map operation
# the above data will be changed to 3 columns(3 nested list)
# O/P: [['1', 'Vivek', '2100000'], ['2', 'Mani', '1800000'], ['3', 'Azhar', '3100000']]
map_data = data_rdd.map(lambda x: x.split(','))
print(map_data.collect())

# flatmap operation
# O/P: ['1', 'Vivek', '2100000', '2', 'Mani', '1800000', '3', 'Azhar', '3100000']
flatmap_rdd = data_rdd.flatMap(lambda x: x.split(','))
print(flatmap_rdd.collect())
