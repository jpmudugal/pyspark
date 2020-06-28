<<<<<<< HEAD
#Code for testing GIT
#updating the code to enable spark3 code

from pyspark.sql import SparkSession
=======
// Code for testing GIT
//reverting master code back to Sparrk2

from pyspark import SparkConf, SparkContext
>>>>>>> spark3
import collections

spark = SparkSession.builder\
    .appName('Spark Program')\
    .master("local[2]")\
    .config("testing config only")\
    .getOrCreate()

sc = spark.sparkContext

lines = sc.textFile("u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
