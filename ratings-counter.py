from pyspark import SparkConf, SparkContext
import collections

#set up the spark conf object. Then set up the spark context object
conf = SparkConf().setMaster("local").setAppName("ratings-counter")
sc = SparkContext(conf = conf)

rawdata = sc.textFile("file:///Users/samkishan/server/u.data")

ratings = rawdata.map(lambda x: x.split()[2])

#print(ratings.take(10))

ratings_distribution = ratings.countByValue()
#ratings_distribution = ratings_distribution.collect()

for item in ratings_distribution.items():
    print(item)


sc.stop()
