from pyspark.sql import SparkSession


spark = SparkSession.builder\
    .appName('Movie Ratings')\
    .master('local')\
    .config('spark.ui.port',10352)\
    .getOrCreate()

import time
start = time.time()


ratings = spark.sparkContext.textFile("../data/u.data") \
    .map(lambda x: (x.split()[2],1))\
    .reduceByKey(lambda x,y: x+y)

for i in sorted(ratings.collect()):
    print(i[0], i[1])

print('Time taken', round(time.time()-start), 'seconds.')


