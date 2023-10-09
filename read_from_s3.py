
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# https://sparkbyexamples.com/amazon-aws/write-read-csv-file-from-s3-into-dataframe/#s3-dependency
# https://saturncloud.io/blog/how-to-load-csv-files-from-s3-with-pyspark-solving-the-no-filesystem-for-scheme-s3n-issue/

input_path = r"s3a://viv-org-hadoop-poc-artifacts/sample_text.csv"
# input_path = r"data/sample_text.csv"

conf = SparkConf()
                conf.set("spark.jars", "jars/hadoop-aws-3.3.1.jar,jars/aws-java-sdk-bundle-1.11.375.jar")
# conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
# # conf.set("spark.hadoop.fs.s3a.awsAccessKeyId", "AKIAZ32Q7LTBAU7TKKHM")
conf.set("fs.s3a.access.key", "AKIAZ32Q7LTBAU7TKKHM")
conf.set("fs.s3a.secret.key", "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")

sc = SparkContext(conf=conf)

# spark = SparkSession(sc)\
spark = SparkSession(sc)\
        .builder\
        .appName("Read From S3")\
        .getOrCreate()

# schema = StructType([
#         StructField("Author", StringType())
#         , StructField("Publications", ArrayType(StringType()))
# ])

input_df = spark.read\
        .format("csv")\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .load(input_path)

input_df.printSchema()
input_df.show()
input_df.select("*", explode_outer(split("Publications", "\|")).alias("Book"))\
        .select("*", coalesce(length("Book"), lit(0)).alias("book_name_length")
                , posexplode_outer(split("Publications","\|")) #will output 2 columns "pos" and "col"
                )\
        .drop("col").withColumnRenamed("pos", "index")\
        .show()


spark.stop()
