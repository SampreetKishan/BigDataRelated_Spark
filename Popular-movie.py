from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

spark = SparkSession.builder.appName("popularMovie").getOrCreate()

schema = StructType([
    StructField("userID",IntegerType()),
    StructField("movieID",IntegerType()),
    StructField("rating",IntegerType()),
    StructField("timestamp",IntegerType())
])

rawdata = spark.read.schema(schema).option("sep","\t").csv("file:///Users/samkishan/server/u.data")

print("Let's find the movie with the most ratings")
print("Method1")
rawdata.createOrReplaceTempView("movie_data")

popularMovies = spark.sql("""
    select movieID, count(rating) as num_ratings
    from movie_data
    group by movieID
    order by num_ratings DESC
""")

popularMovies.show(10)

print("\n---------------------\n\n")
print("Method2")
popularMovies_2 = rawdata.groupby("movieID").agg(func.count("rating").alias("num_ratings"))
popularMovies_2 = popularMovies_2.orderBy(func.desc("num_ratings"))
popularMovies_2.show()

spark.stop()