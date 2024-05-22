from pyspark import SparkConf, SparkContext
import collections, re

def normalizeText(x):
    return re.compile(r'\W+', re.UNICODE).split(x.lower())


conf = SparkConf().setMaster("local").setAppName("Word counts2")
sc = SparkContext(conf = conf)

rawdata = sc.textFile("file:///Users/samkishan/server/Book.txt")

words = rawdata.flatMap(normalizeText)

words = words.map(lambda x:(x,1))

words = words.reduceByKey(lambda x,y: x+y)

def swap(x):
    return(x[1],x[0])

words = words.map(swap)

words_asc = words.sortByKey()

words_asc = words_asc.map(swap)

print("Least common words:")
print(words_asc.take(20))

words_dsc = words.sortByKey(ascending=False)
words_dsc = words_dsc.map(swap)

print("most common words:")
print(words_dsc.take(20))

sc.stop()