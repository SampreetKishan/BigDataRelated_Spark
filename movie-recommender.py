#Movie recommender system using item based colloborative filtering method

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

#Start the spark session and instruct spark to use all the cores in the local system since this is a compute heavy process
spark = SparkSession.builder.appName("MovieRecommender").master("local[*]").getOrCreate()

movieSchema = StructType([
    StructField("userID", IntegerType()),
    StructField("movieID", IntegerType()),
    StructField("rating", IntegerType()),
    StructField("timestamp", IntegerType())
])

movie_data = spark.read.schema(movieSchema).option("sep","\t").csv("file:///Users/samkishan/server/u.data")

#select just the userID, movieID, and rating columns. No need for timestamp
movie_data = movie_data.select("userID","movieID","rating")
#movie_data.show(20)
#print(movie_data.count())


#Let's do a self-join on this dataset
moviePairs = movie_data.alias("ratings1").join(movie_data.alias("ratings2"))

#We only want the ratings of pairs of movies by the same user
moviePairs = moviePairs.filter(func.col("ratings1.userID")==func.col("ratings2.userID"))

#let's clean up the names of the columns
moviePairs = moviePairs.select(func.col("ratings1.userID").alias("user1"),
                               func.col("ratings1.movieID").alias("movie1"),
                               func.col("ratings1.rating").alias("rating1"),
                               func.col("ratings2.userID").alias("user2"),
                               func.col("ratings2.movieID").alias("movie2"),
                               func.col("ratings2.rating").alias("rating2"))

#let's removie duplicates.. For ex. movie pairs ids (10, 11) is the same as (11,10)
moviePairs = moviePairs.filter(func.col("movie1")>func.col("movie2"))

#Uncomment the next two lines only for troubleshooting. Else, it'll slow down the process.
#moviePairs.show(200)
#print(moviePairs.count())


def cosineSimiliarity(spark, data):
    data_step1 = data.withColumn("xx",func.col("rating1") * func.col("rating1")).\
        withColumn("yy", func.col("rating2") * func.col("rating2")).\
        withColumn("xy", func.col("rating1") * func.col("rating2"))

    data_step2 = data_step1.groupby("movie1", "movie2").agg(
        func.sum(func.col("xy")).alias("numerator"),
        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"),
        func.count(func.col("xx")).alias("num_pairs")
    )

    final_step = data_step2.withColumn("similar_score", func.when(func.col("denominator")!=0, func.col("numerator") / func.col("denominator")).otherwise(0))
    final_step = final_step.select("movie1", "movie2", "similar_score","num_pairs")

    return final_step


#Let's call the above function to determine the cosine similarity score value for each movie pair.
#Here is a link to the formula: https://towardsdatascience.com/cosine-similarity-how-does-it-measure-the-similarity-maths-behind-and-usage-in-python-50ad30aad7db?gi=933649f8b6a4
#Let's cache the resulting df since we may have to use it a few times.. This'll store the df in the executors memory
MoviesPairsScores = cosineSimiliarity(spark, moviePairs).cache()

movie_a = int(input("Enter a movieID: "))

#Set minimum thresholds
score_threshold = 0.90
min_num_pairs = 50

#From the resulting MoviesPairsScores df, filter to find the rows that contain the movieID in question
# (either in movie1 or movie2) columns
# Also filter out entries that have low similarity scores and/or not enough ratings (num_pairs)
MoviesPairsScores = MoviesPairsScores.filter(
    (func.col("movie1") == movie_a) | (func.col("movie2")==movie_a)
).filter(func.col("similar_score")>=score_threshold).filter(func.col("num_pairs")>=min_num_pairs)

#MoviesPairsScores = MoviesPairsScores.filter(func.col("movie1")!=func.col("movie2"))
MoviesPairsScores = MoviesPairsScores.orderBy("similar_score", ascending=False) #.orderBy("num_pairs", ascending = False)




#MoviesPairsScores = MoviesPairsScores.select("movie1","movie2","similar_score", "num_pairs")
#for item in MoviesPairsScores.take(10):
#    print(item)


#We could also ingest the u.item dataset that provides movieID <> movieName mapping
movieNameSchema = StructType([
    StructField("movieID",IntegerType()),
    StructField("movieName",StringType())
])

#Let's connect to the u.item dataset

moviesNames = spark.read.schema(movieNameSchema).option("sep","|").csv("file:///Users/samkishan/server/u.item")

def movieMapper(movieID):
    moviesNames_1 = moviesNames.filter(func.col("movieID")==movieID).select("movieName")
    #print(moviesNames_1.show())
    return moviesNames_1.collect()[0][0]

print("The most similar movies to "+ movieMapper(movie_a)+" are: ")

for item in MoviesPairsScores.take(5):
    print(" -------------------------------------------------------------")
    print("Movie1: "+ movieMapper(item[0]))
    print("Movie2: "+ movieMapper(item[1]))
    print("Similarity score: "+ str(item[2]))
    print("Number of pairs: "+ str(item[3]))

spark.stop()