from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCounter").getOrCreate()

rawdata = spark.read.text("file:///Users/samkishan/server/Book.txt")

parseddata = rawdata.select(func.explode(func.split(rawdata.value,"\\W+")).alias("word"))

parseddata = parseddata.select(func.lower(parseddata.word).alias("word"))
#parseddata.printSchema()
#parseddata.show()

AggrData = parseddata.groupby("word").agg(func.count(parseddata.word).alias("count"))
AggrData = AggrData.sort(AggrData["count"].desc())
AggrData.show()



spark.stop()