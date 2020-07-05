from pyspark import SparkConf, SparkContext
import collections



conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

import time
start = time.time()

lines = sc.textFile("../data/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

print('Time taken', round(time.time()-start), 'seconds.')