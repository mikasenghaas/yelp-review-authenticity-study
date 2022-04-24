import argparse
import pyspark as spark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as func     

from part1 import (
    question1, 
    question2,
    question3,
    question4,
    question5)

def main():
  # setup
  conf = SparkConf()
  conf.set("spark.executor.memory", "4G")
  conf.set("spark.executor.instances", "8")

  spark = SparkSession.builder \
                      .appName('lsda-assignment2') \
                      .config(conf=conf) \
                      .getOrCreate()
  spark.sparkContext.setLogLevel('OFF')

  # read in the business and review files
  bs = spark.read.parquet("/datasets/yelp/parquet/business.parquet")
  rs = spark.read.parquet("/datasets/yelp/parquet/review.parquet")
  us = spark.read.parquet("/datasets/yelp/parquet/user.parquet")

  # part 1
  print("Question 1" + "-"*10)
  question1(bs)

  print("Question 2" + "-"*10)
  question2(bs)

  print("Question 3" + "-"*10)
  influencers = question3(us)
  print(f"Found Influencers:\n{influencers.show()}")

  print("Question 4" + "-"*10)
  question4(rs, bs, ans3)

  print("Question 5" + "-"*10)
  question5(bs)

  # part 2

if __name__ == "__main__":
  main()
