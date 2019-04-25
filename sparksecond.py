import re
from pyspark import SparkContext
from collections import Counter
from operator import add

if __name__ == "__main__":
  sc=SparkContext("local[*]", "wordcount")
  sc.setLogLevel("Error")

  data = sc.textFile("/home/sid/spark/data/training_set_tweets.txt")
  count = data.flatMap(lambda x: re.findall(r'(?<!RT\s)@\S+',x))
  c = count.map(lambda x: (x,1)).reduceByKey(lambda a, b: a+b)
  cSorted = c.sortBy(lambda a: -a[1])
  print(cSorted.take(10))
