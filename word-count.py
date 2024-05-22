from pyspark import SparkConf, SparkContext
import re

def normalizeWords(x):
    return re.compile(r'\W+',re.UNICODE).split(x.lower())

conf = SparkConf().setMaster("local").setAppName("word count")
sc = SparkContext(conf = conf)

rawdata = sc.textFile("file:///Users/samkishan/server/Book.txt")

words = rawdata.flatMap(normalizeWords)

#words = words.map(lambda x:(x,1))

words = words.countByValue()


for item in words.items():
    print(item)

sc.stop()