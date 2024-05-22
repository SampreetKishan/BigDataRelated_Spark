from pyspark import SparkConf,SparkContext
import collections

def split_function(x):
    x = x.split(",")
    age = int(x[2])
    num_friends = int(x[3])
    return(age,(num_friends,1))

conf = SparkConf().setMaster("local").setAppName("friendsByAge")
sc = SparkContext(conf = conf)

rawdata = sc.textFile("file:///Users/samkishan/server/fakefriends_2.csv")

print("Number of entries in the dataset:", rawdata.count())

#for item in rawdata.take(10):
#    print(item)

#friends_data = rawdata.map(lambda x:(int(x.split(",")[2]), int(x.split(",")[3])))
#for item in friends_data.take(10):
#    print(item)

friends_data = rawdata.map(split_function)
friends_data_distribution = friends_data.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

friends_data_distribution = friends_data_distribution.mapValues(lambda x:x[0]/x[1])

for item in friends_data.take(10):
    print(item)

for item in friends_data_distribution.take(10):
    print(item)


sc.stop()