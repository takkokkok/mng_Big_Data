#!/usr/bin/env python

from pyspark import SparkContext

def main():
  sc = SparkContext(appName = "Count2Words")
  input_file = sc.textFile('/user/cloudera/wz/data04.txt')
  tokens = input_file.map(lambda line : line.strip().split(" "))
  words = tokens.flatMap(lambda xs : (tuple(x) for x in zip(xs, xs[1:])))
  biwords = words.map(lambda x: (x,1))
  wz = biwords.reduceByKey(lambda a, b: a+b)
  wz.saveAsTextFile('/user/cloudera/wz/output')
  sc.stop()
  
if __name__ == '__main__':
  main()
