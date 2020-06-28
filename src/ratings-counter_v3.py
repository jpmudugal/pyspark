from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, IntegerType, Row
from pyspark.sql.functions import col, round

spark = SparkSession.builder\
    .appName('V3 ratings counter program')\
    .master('local')\
    .getOrCreate()

lines = spark.sparkContext.textFile("../data/u.data")\
    .map(lambda x : Row(userID=int(x.split()[0]),\
                                 itemID=int(x.split()[1]),\
                                 rating=int(x.split()[2]),\
                                 timestamp=x.split()[3]))

totalRatings = lines.count()

schema = StructType([StructField("userID",IntegerType(),True),\
    (StructField("itemID",IntegerType(),True)),\
    (StructField("rating",IntegerType(),True)),\
    (StructField("timestamp",StringType(),True))])

ratingsDF = spark.createDataFrame(lines,schema)

sortedRatings = ratingsDF.coalesce(1).groupBy('rating')\
    .count()\
    .sort('rating')

perRatings = sortedRatings.withColumn("%", round(col('count')*100/totalRatings,2))

perRatings.show()