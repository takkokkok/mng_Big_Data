#!/usr/bin/env python

from pyspark import SparkContext

def main():
  sc = SparkContext(appName = "Count2Words")
  input_file = sc.textFile('/user/cloudera/c2w/data04.txt')
  tokens = input_file.map(lambda line : line.strip().split(" "))
  words = tokens.flatMap(lambda xs : (tuple(x) for x in zip(xs, xs[1:])))
  biwords = words.map(lambda x: (x,1))
  c2w = biwords.reduceByKey(lambda a, b: a+b)
  c2w.saveAsTextFile('/user/cloudera/c2w/output')
  sc.stop()
  
if __name__ == '__main__':
  main()
