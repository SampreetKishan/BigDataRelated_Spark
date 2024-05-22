from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as func
from pyspark.sql import Row

spark = SparkSession.builder.appName("SparkSQLDataframe").getOrCreate()

rawdata = spark.read.option("header","True").option("inferSchema","True").csv("file:///Users/samkishan/server/fakefriends-header.csv")

def age_adder(x):
    return x+10

spark.udf.register("AgeAdder",age_adder,returnType=IntegerType())

rawdata.createOrReplaceTempView("fakefriends")

adding_age = spark.sql("select name, AgeAdder(age) as age_plus_10 from fakefriends")

#adding_age.show()


#rawdata.groupby("age").count().show()

#rawdata.select(rawdata.name, rawdata.age + 10).show()



print("Average number of friends")

average_friends = spark.sql(""" select age, round(avg(friends),2) as average_friends 
                                from fakefriends 
                                group by age
                        """)
#average_friends.show(100)

#Let's not use SparkSQL.. Instead use Spark inbuilt functions to work this problem

AgeAndFriends = rawdata.select("age","friends")

AgeAndFriends = AgeAndFriends.groupby("age").agg(func.round(func.avg("friends"),2).alias("Avg_friends")).sort("age").show(100)

spark.stop()