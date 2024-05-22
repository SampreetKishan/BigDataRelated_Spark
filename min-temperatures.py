from pyspark import SparkConf, SparkContext

def ParseCSV(x):
    x = x.split(",")
    location = x[0]
    metric = x[2]
    value = int(x[3])
    return (location, metric, value)



conf = SparkConf().setMaster("local").setAppName("Min-temperature")
sc = SparkContext(conf = conf)

rawdata = sc.textFile("file:///Users/samkishan/server/1800.csv")

parsed_data = rawdata.map(ParseCSV)

parsed_data = parsed_data.filter(lambda x: "TMIN" in x[1])

def parsed_2(x):
    location = x[0]
    value = int(x[2]) * 0.1
    return(location, value)

parsed_data = parsed_data.map(parsed_2)

min_temp = parsed_data.reduceByKey(lambda x,y: x if x<y else y)

print("minimum temperatures")
for item in min_temp.collect():
    print(item)

max_temp = parsed_data.reduceByKey(lambda x,y: x if x>y else y)

print("maximum temperatures")
for item in max_temp.collect():
    print(item)



sc.stop()