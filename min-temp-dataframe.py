from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName("Min-temp").getOrCreate()


schema = StructType([
    StructField("location", StringType()),
    StructField("time", StringType()),
    StructField("metric", StringType()),
    StructField("value", IntegerType()),
])

rawdata = spark.read.schema(schema).csv("file:///Users/samkishan/server/1800.csv")
rawdata.printSchema()
#rawdata.show()
min_temp_data = rawdata.filter(rawdata.metric == "TMIN")
min_temp_data = min_temp_data.groupby("location").min("value")

min_temp_data = min_temp_data.withColumn("temperature", func.col("min(value)")*0.1)
min_temp_data = min_temp_data.select(min_temp_data.location, min_temp_data.temperature)
min_temp_data.show()
spark.stop()