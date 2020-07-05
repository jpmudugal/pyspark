from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, IntegerType, Row
from pyspark.sql.functions import col, round

spark = SparkSession.builder\
    .appName('V3 ratings counter program')\
    .master('local')\
    .config('spark.ui.port',10345)\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#import time
#start = time.time()

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

ratingsDF.createTempView('ratings')

ratingSQLdf = spark.sql("select rating, count(*) as count, \
                        round(count(*)*100/"+str(totalRatings)+",2) as percentage  from ratings group by rating order by 1")

ratingSQLdf.coalesce(1).show()