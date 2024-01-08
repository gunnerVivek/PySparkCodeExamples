"""
-- We define the install date of a player to be the first login day of that player.
-- We also define day 1 retention of some date X to be the number of players whose install
-- date is X and they logged back in on the day right after X , divided by the number of
-- players whose install date is X, rounded to 2 decimal places.
--
-- Write an SQL query that reports for each install date, the number of players that installed the game on that day
-- and the day 1 retention.
--
 The query result format is in the following example:

-- Activity table:
-- +-----------+-----------+------------+--------------+
-- | player_id | device_id | event_date | games_played |
-- +-----------+-----------+------------+--------------+
-- | 1         | 2         | 2016-03-01 | 5            |
-- | 1         | 2         | 2016-03-02 | 6            |
-- | 2         | 3         | 2017-06-25 | 1            |
-- | 3         | 1         | 2016-03-01 | 0            |
-- | 3         | 4         | 2016-07-03 | 5            |
-- +-----------+-----------+------------+--------------+
--
-- Result table:
-- +------------+----------+----------------+
-- | install_dt | installs | Day1_retention |
-- +------------+----------+----------------+
-- | 2016-03-01 | 2        | 0.50           |
-- | 2017-06-25 | 1        | 0.00           |
-- +------------+----------+----------------+
--
-- Player 1 and 3 installed the game on 2016-03-01 but only player 1 logged back in on 2016-03-02 so the day 1 retention of 2016-03-01 is 1/2 = 0.50
-- Player 2 installed the game on 2017 -06-25 but didn't log back in on 2017-06-26 so the day 1 retention of 2017-06-25 is 0/1 = 0.00

"""

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf = SparkConf()
spark_conf.set("spark.sql.legacy.timeParserPolicy","LEGACY") # enables to use java.text.SimpleDateFormat

spark = SparkSession(SparkContext(conf=spark_conf)).builder.appName("").getOrCreate()

schema = StructType([
    StructField("player_id", StringType())
    , StructField("device_id", StringType())
    , StructField("event_date", DateType())
    , StructField("games_played", IntegerType())
])

# data = [
#  [1,2,"2016-03-01",5], [1,2,"2016-03-02",6], [2,3,"2017-06-25",1]
#  , [3,1,"2016-03-01",0], [3,4,"2016-07-03",5]
# ]
input_path = r"data/game_play.csv"
df = spark.read.format("csv").option("header", "true").schema(schema).load(input_path)



spark.stop()
