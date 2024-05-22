#Objective: Find the most popular movie.
#movie ratings dataset: u.data; movieID<>movieName dataset: u.item
# use the sc.broadcast variable to broadcast u.item to all executors.


import csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import  StructType, StructField,IntegerType, FloatType, StringType

nameDict = {}

#Open the u.item file and populate the nameDict dictionary
with open("/Users/samkishan/server/u.item", mode = "r", encoding= "latin-1") as f:
    reader = csv.reader(f, delimiter = "\t")
    for row in reader:
        new_row = row[0].split("|")
        movieID = new_row[0]
        movieName = new_row[1]
        nameDict[int(movieID)] = movieName

#print(nameDict)


movie_schema = StructType([
    StructField("userID",IntegerType()),
    StructField("movieID",IntegerType()),
    StructField("rating",IntegerType()),
    StructField("timestamp",IntegerType())
])


spark = SparkSession.builder.appName("PopularMovies_name").getOrCreate()

print("Lets broadcast the movieID<>movieName dictionary")

#Let's a broadcast variable nameDictBroadcast
nameDictBroadcast = spark.sparkContext.broadcast(nameDict)


rawMovieData = spark.read.schema(movie_schema).option("sep","\t").csv("file:///Users/samkishan/server/u.data")
popularMovies_2 = rawMovieData.groupby("movieID").agg(func.count("rating").alias("num_ratings"))
popularMovies_2 = popularMovies_2.orderBy(func.desc("num_ratings"))

#Create a Python function that returns the movieName from a movieID provided using the broadcast variable nameDictBroadcast
def lookupMovieName(movie_id):
    return nameDictBroadcast.value[movie_id]

#let's conver this regular Python function to a UDF
lookupMovieNameUDF = func.udf(lookupMovieName)

#Add a new column to the aggregated dataframe that provides the movieName
popularMovies_2 = popularMovies_2.withColumn("movieName",lookupMovieNameUDF(func.col("movieID")))

popularMovies_2.show(20)



spark.stop()
