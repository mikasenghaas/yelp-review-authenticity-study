# part2.py
# authenticity study
import pyspark as spark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import explode, col, split

def eda(rs, bs):
  # filter for restaurants reviews
  print(bs.categories)
  res = bs[bs.categories.contains('Restaurants')]
  res_bus = res.join(rs, "business_id")

  auth = res_bus\
          .filter(res_bus.text.rlike('[Aa]uthentic[a-z]*'))

  auth_ratio = auth.count() / res_bus.count()
  print(f"Reviews with a variant of authentic: {auth.count()}")
  print(f"Ratio of reviews with a variant of authentic: {auth_ratio}")
  
  leg = res_bus\
          .filter(res_bus.text.rlike('legitimate'))
  leg_ratio = leg.count() / res_bus.count()

  print(f"Reviews with a variant of legitimate: {leg.count()}")
  print(f"Percentage of reviews with a variant of legitimate: {leg_ratio}")

  # get review count per category
  res_bus_exploded = res_bus.withColumn('single_categories', 
      explode(split(col('categories'), ', ')))
  res_bus_exploded = res_bus_exploded.groupBy('single_categories').count()
  res_bus_exploded = res_bus_exploded.withColumnRenamed('count', 'review_count')

  # get auth review count per category
  auth_exploded = auth.withColumn('single_categories', 
      explode(split(col('categories'), ', ')))
  auth_exploded = auth_exploded.groupBy('single_categories').count()
  auth_exploded = auth_exploded.withColumnRenamed('count',
      'authentic_review_count')

  # join and compute ratio of authentic reviews per category
  auth_ratio = res_bus_exploded.join(auth_exploded, on='single_categories')
  auth_ratio = auth_ratio.withColumn('auth_ratio',
      auth_ratio.authentic_review_count / auth_ratio.review_count)

  # show
  auth_ratio.sort('review_count', ascending=False).show()
  auth_ratio.sort('auth_ratio', ascending=False).show()

  # show only relevant categories
  categories = ['Mexican', 'Indian', 'Japanese', 'Italian', 'Chinese',
      'Mexican', 'French', 'Greek']

  auth_ratio.filter(auth_ratio.single_categories.isin(categories))\
            .sort('auth_ratio', ascending=False).show()

  # geographic differences
  res_bus = res_bus.withColumn("auth_lang",
      res_bus.text.rlike('[Aa]uthentic[a-z]*'))

  res_bus_cube = res_bus.cube("state", "city", "auth_lang")\
                        .count()\
                        .orderBy("count", ascending=False)
  res_bus_cube.show()

  # hypothesis testing
  # get average review for each authentic review in different regions
  auth_reviews = res_bus_exploded.join(auth_exploded, on='single_categories')

  european = ['Modern European', 'Austrian', 'Italian', 'French', 'Eastern
  European', 'American (New')]
  non_european = ['Mexican', 'Turkish', 'Thai', 'Indian', 'Indian', 'Chinese',
  'Indonesian', 'African', 'Mongolian', 'Taiwanese', 'Argentine', 'Japanese',
  'Latin American', 'Filipino', 'Vietnamese', 'Pakistani', 'Burmese', 'New
  Mexican Cuisine', 'Persian/Iranian', 'Bangladeshi']

  auth = auth.withColumn('single_categories', 
      explode(split(col('categories'), ', ')))

  europeans = auth.filter(auth_reviews.isin(european))\
                          .groupBy('single_categories')\
                          .mean('stars')
  europeans.show()

if __name__ == '__main__':
  conf = SparkConf()
  conf.set("spark.executor.memory", "4G")
  conf.set("spark.executor.instances", "8")

  spark = SparkSession.builder \
                      .appName('lsda-assignment2') \
                      .config(conf=conf) \
                      .getOrCreate()
  spark.sparkContext.setLogLevel('OFF')

  rs = spark.read.json("/datasets/yelp/review.json")
  bs = spark.read.json("/datasets/yelp/business.json")

  eda(rs, bs)
