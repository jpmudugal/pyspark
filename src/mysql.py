from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.config("jars", "~/spark3/jars/mysql-connector-java-5.1.49.jar")\
    .config("driver-class-path", "~/spark3/jars/mysql-connector-java-5.1.49.jar")\
    .getOrCreate()



movie_ratings = spark.read.load(path="../data/u.data",format="csv",header=False,sep="\t")\
    .withColumnRenamed("_c0","user_id").withColumnRenamed("_c1","item_id").withColumnRenamed("_c2","rating")

movie_dict = spark.read.load(path="../data/u.item",format="csv",header=False,sep="|")\
    .withColumnRenamed("_c0","movie_id").withColumnRenamed("_c1","movie_name")\
    .select(col("movie_id"),col("movie_name"))


#df.show()
movie_dict.write.jdbc(url="jdbc:mysql://localhost:3306/pyspark",\
                table="movie_dict",\
                properties={"user":"root","password":"prajaya123","driver":"com.mysql.jdbc.Driver"})

movie_ratings.write.jdbc(url="jdbc:mysql://localhost:3306/pyspark",\
                table="movie_ratings",\
                properties={"user":"root","password":"prajaya123","driver":"com.mysql.jdbc.Driver"})