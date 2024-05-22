from pyspark import SparkConf, SparkContext
import collections

def parse_data(x):
    x=x.split(",")
    cust_id = x[0]
    item_id = x[1]
    amount = round(float(x[2]),2)
    #print(amount)
    return (cust_id, amount)

def swap(x):
    return(x[1],x[0])

conf = SparkConf().setMaster("local").setAppName("customerOrders")
sc = SparkContext(conf = conf)
rawdata = sc.textFile("file:///Users/samkishan/server/customer-orders.csv")

parsed_data = rawdata.map(parse_data)

#print(parsed_data.take(20))

aggr_data = parsed_data.reduceByKey(lambda x,y: x+y).map(swap).sortByKey(ascending=False).map(swap)

print("Customers who spent the most:")
print(aggr_data.take(20))
sc.stop()