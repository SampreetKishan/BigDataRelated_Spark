from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

SuperHeroID_start  = 5306 # Spiderman
SuperHeroID_target = 14 # Adam 3,031


#function to print a sample of the RDD
def print_rdd(x):
    for item in x.take(10):
        print(item)

spark = SparkSession.builder.appName("BFS").getOrCreate()


# set up a spark accumulator as the counter with an initial value of 0. The value is incremented by 1 if there is a hit
# This accumulator is shared by all the processors.
hitCounter = spark.sparkContext.accumulator(0)

#A mapper function to produce BFS nodes
# node formart: (superheroID, (list of connections), distance, color)
# distance initially is infinity or 9999
# color of processed is black, processing node is gray, unprocessed node is white
def convertBFS(x):
    superheroID = int(x[0])
    connections = x[1]
    color = 'WHITE'
    distance = 9999

    if(superheroID == SuperHeroID_start):
        distance = 0
        color = 'GRAY'

    return (superheroID, (connections, distance, color))


marvel_rawdata = spark.sparkContext.textFile("file:///Users/samkishan/server/Marvel+Graph")
marvel_rawdata = marvel_rawdata.map(lambda x: (x.split(" ")[0], x.split(" ")[:-1]))
#marvel_rawdata = marvel_rawdata.reduceByKey(lambda x,y: x+y)
marvel_rawdata = marvel_rawdata.map(convertBFS)
print_rdd(marvel_rawdata)

#We are going to assume any two characters have a maximum degree of separation of 10


def mapBFS(node):
    # (heroID, ([connections], distance, color))
    heroID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]
    results = []

    if(color == "GRAY"):
        for connection in connections:
            new_heroID = int(connection)
            new_color = "GRAY"
            new_distance = distance + 1
            new_connections = []

            new_node = (new_heroID, (new_connections, new_distance, new_color))
            results.append(new_node)

            if(int(connection) == int(SuperHeroID_target)):
                hitCounter.add(1)
        color = "BLACK"

    org_results = (heroID, (connections,distance, color))
    results.append(org_results)
    return results


def bfsReduce(data1, data2):
    #data = ([connections], distance, color)
    connections_1 = data1[0]
    connections_2 = data2[0]
    distance_1 = data1[1]
    distance_2 = data2[1]
    color_1 = data1[2]
    color_2 = data2[2]

    #Note: one of the connections is always going to be an empty list



    # preserve the shortest distance

    final_connections = []
    final_color = "WHITE"
    final_distance = 9999

    # preserve the connections of the original node
    if(len(connections_1)>0):
        final_connections = connections_1
    elif(len(connections_2)>0):
        final_connections = connections_2

    # preserve the darkest color
    if(color_1=="BLACK"):
        final_color=color_1
    elif(color_2=="BLACK"):
        final_color=color_2
    elif(color_1 =="GRAY" and (color_2=="WHITE" or color_2=="GRAY")):
        final_color=color_1
    elif(color_2=="GRAY" and (color_1=="WHITE" or color_1=="GRAY")):
        final_color= color_2

    if(distance_1 < distance_2):
        final_distance = distance_1
    elif(distance_2 < distance_1):
        final_distance = distance_2

    return(final_connections, final_distance, final_color)




for iteration in range(0,10):
    print("Processing for the " + str(iteration+1)+ "the time")

    mapped_df = marvel_rawdata.flatMap(mapBFS)

    print("Processing rows: ")
    print(str(mapped_df.count()))

    if (hitCounter.value> 0):
        print("Found the hero ID!! "+str(hitCounter.value)+" ways")
        break

    marvel_rawdata = mapped_df.reduceByKey(bfsReduce)



spark.stop()
