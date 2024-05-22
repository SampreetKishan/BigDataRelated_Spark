from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("PopularSuperHero").getOrCreate()

friends_rawdata = spark.sparkContext.textFile("file:///Users/samkishan/server/Marvel+Graph")

def friends_mapper(x):
    x = x.split(" ")
    superhero_id = int(x[0])
    num_friends = len(x) - 1
    return(superhero_id, num_friends)

friends_rawdata = friends_rawdata.map(friends_mapper)
friends_rawdata = friends_rawdata.reduceByKey(lambda x,y: x+y)
print("After combining")
print("let's convert the rdd to df")
friends_schema = StructType([
    StructField("superheroID", IntegerType()),
    StructField("num_friends", IntegerType())
])
friends_df = spark.createDataFrame(friends_rawdata, friends_schema)
print("let's create a sql view for the df")

friends_df.createOrReplaceTempView("friends")
friends_df_view = spark.sql("select * from friends")
#friends_df_view.show(20)



print("Let's work on the mapping data")

mapping_schema = StructType([
    StructField("superheroID", IntegerType()),
    StructField("superheroName", StringType())
])

mapping_rawdata_df = spark.read.schema(mapping_schema).option("sep"," ").csv("file:///Users/samkishan/server/Marvel+Names")
#mapping_rawdata_df.show(20)
mapping_rawdata_df.createOrReplaceTempView("mapping")

combined_df = spark.sql("""
    select friends.num_friends, mapping.superheroName
    from friends
    join mapping on friends.superheroID = mapping.superheroID
    order by friends.num_friends DESC
""")

combined_df.show(20)
spark.stop()