#Objective: Find out which superhero in the Marvel universe has the most connections
#Dataset1: Marvel+Graph: contains a superheroID and the IDs of superheros he's featured along with. A popular superhero
# can span multiple lines.
#Dataset2: Marvel+Name: mapping between superheroID and superheroName

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName("PopularSuperhero").getOrCreate()
#print("Let's read the marvel+graph data as an RDD using SC")
marvelGraph_rawdata = spark.sparkContext.textFile("file:///Users/samkishan/server/Marvel+Graph")

marvelGraph_rawdata = marvelGraph_rawdata.map(lambda x: (int(x.split(" ")[0]), int(len(x)-1))) #.groupByKey(lambda x,y: x+y)
#marvelGraph_rawdata = marvelGraph_rawdata.reduceByKey(lambda x,y: x+y)



#for item in marvelGraph_rawdata.take(10):
#    print(item)

#Let's ingest the Marvel+Name mapping dataset

mapping_schema = StructType([
    StructField("superheroID",IntegerType()),
    StructField("superheroName",StringType())
])
mapping_rawdata_df = spark.read.schema(mapping_schema).option("sep"," ").csv("file:///Users/samkishan/server/Marvel+Names")
#mapping_rawdata_df.show(20)

connections_schmea = StructType([
    StructField("superheroID",IntegerType()),
    StructField("num_friends",IntegerType())
])
marvelGraph_rawdata_df = spark.createDataFrame(marvelGraph_rawdata, connections_schmea)
marvelGraph_rawdata_df = marvelGraph_rawdata_df.groupby("superheroID").agg(func.sum("num_friends").alias("num_friends"))

#print("lets' create sql views for the two dataframes")
marvelGraph_rawdata_df.createOrReplaceTempView("marvel_data")
mapping_rawdata_df.createOrReplaceTempView("mapping_data")

print("The most popular superheros are: ")
combined_data = spark.sql("""
    select marvel_data.superheroID as id, marvel_data.num_friends as num_friends, mapping_data.superheroName as name
    from marvel_data
    join mapping_data
    on marvel_data.superheroID = mapping_data.superheroID
    order by num_friends DESC
""")

combined_data.show(20)
spark.stop()