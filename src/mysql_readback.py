from pyspark.sql import SparkSession

spark = SparkSession.builder.config("jars", "~/spark3/jars/mysql-connector-java-5.1.49.jar")\
    .config("driver-class-path", "~/spark3/jars/mysql-connector-java-5.1.49.jar")\
    .getOrCreate()

sql_query = """select a.item_id, b.movie_name,count(a.item_id) as tot 
from movie_dict b, movie_ratings a where a.item_id = b.movie_id 
and rating = 5 group by a.item_id, b.movie_name 
order by count(a.item_id) desc limit 10"""

df = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/pyspark").\
                      option("user","root").\
                      option("password","prajaya123").\
                      option("driver","com.mysql.jdbc.Driver").\
                      option("query",sql_query).load()

df.show()