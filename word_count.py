
from pyspark.sql import SparkSession

input_path = r"data/word_count.txt"

spark = SparkSession.builder.appName("").getOrCreate()

file = spark.sparkContext.textFile(input_path)

words = file.flatMap(lambda line: line.split(" "))
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

print(word_counts.collect())
word_counts.saveAsTextFile("output/word_counts.txt")
