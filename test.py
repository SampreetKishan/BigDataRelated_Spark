from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
spark = SparkSession.builder.appName("MovieRecommender").getOrCreate()

movieNameSchema = StructType([
    StructField("movieID",IntegerType()),
    StructField("movieName",StringType())
])
moviesNames = spark.read.schema(movieNameSchema).option("sep","|").csv("file:///Users/samkishan/server/u.item")

#moviesNames.show(20)

movieID = int(input("Enter movie ID: "))

def movieMapper(movieID):
    moviesNames_1 = moviesNames.filter(func.col("movieID")==movieID).select("movieName")
    print(moviesNames_1.show())
    return moviesNames_1.collect()[0][0]

print("The movie name of movie id "+str(movieID)+" is "+str(movieMapper(movieID)))


spark.stop()
