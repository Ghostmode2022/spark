import re
from pyspark import SparkContext
from collections import Counter
from operator import add

def selectLine(a):
  if len(a.split('\t')) == 4:
    line = a.split('\t')[3]
    if re.match(r'(2009-09-(1[6-9]|20))', line):
      return a

if __name__ == "__main__":
  sc=SparkContext("local[*]", "wordcount")
  sc.setLogLevel("Error")

  data = sc.textFile("/home/sid/spark/data/training_set_tweets.txt")
  count = data.filter(selectLine)
  count1 = count.flatMap(lambda x: re.findall(r'\s([@][\w_-]+)',x))
  c = count1.map(lambda x: (x,1)).reduceByKey(lambda a, b: a+b)
  cSorted = c.sortBy(lambda a: -a[1])
  print(cSorted.take(10))
