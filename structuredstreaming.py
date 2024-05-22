#Structured Stream
#author:Sampreet Kishan
#Objective: to analyze a stream of data using structured streaming.
#More info: We design this code to monitor a folder ,in this case "logs", for new data coming in as a stream.
#I am making use of a simple text file called accessLogs_handwritten.txt
#When I create a copy of the same txt file and add it to the folder "logs", the code adds up and aggregates the "amount" for each day of the week.

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql import functions as func
from pyspark.sql import Row
from pyspark.streaming import StreamingContext

#Create a sparkSession
spark = SparkSession.builder.appName("Streaming HTTP codes").getOrCreate()

#Monitor for new data/logs in the /Users/samkishan/server/logs folder
accessLines = spark.readStream.text("/Users/samkishan/server/logs")


# Parse out the common log format to a DataFrame
#sample data: "1 Monday 19"
#Name of sample file:"accessLogs_handwritten.txt
index = r'^\d\s'
day = r'\s(\w+)\s'
amount = r'\s(\d+)$'

#Accesslines is a datastream dataframe that has a column "value" that contains each line from the original file/stream/logs

logsDF = accessLines.select(regexp_extract('value',index,1).alias("index"),
                            regexp_extract('value',day,1).alias("day"),
                            regexp_extract('value',amount, 1).alias("amount"))

# Provides totals for every day of the week
TotalAmountsDF = logsDF.groupBy("day").agg(func.sum("amount").alias("total_sum"))

# Kick off our streaming query, dumping results to the console
StreamingDF = TotalAmountsDF.writeStream.outputMode("Complete").format("console").queryName("totalAmounts").start()

# Run forever until terminated
StreamingDF.awaitTermination()

spark.stop()

