from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(x):
    x = x.split(",")
    id = int(x[0])
    name = x[1]
    age = int(x[2])
    friends = int(x[3])
    return Row(id = id,
               name = name,
               age = age,
               friends = friends)

#read the data in as an RDD using SparkContext
rawdata = spark.sparkContext.textFile("file:///Users/samkishan/server/fakefriends.csv")
lines = rawdata.map(mapper)

#Then convert it to an dataframe
schemaPeople = spark.createDataFrame(lines).cache()
schemaPeople.createOrReplaceTempView("people")


teens = spark.sql("select * from people where age>=13 and age<=19")

for teen in teens.collect():
    print(teen)


schemaPeople.groupby("age").avg("friends").orderBy("age").show()

spark.stop()