

from pyspark.sql import SparkSession
import collections

spark = SparkSession.builder\
    .appName('Movie Ratings')\
    .master('local[2]')\
    .getOrCreate()

lines = spark.sparkContext.textFile("../data/u.data")

ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
