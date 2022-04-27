# part2.py
# authenticity study
import pyspark as spark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import explode, col, split

def part2(rs, bs):
  # change col name of stars in review
  rs = rs.withColumnRenamed('stars', 'review_stars')
  
  # filter for restaurants reviews
  res = bs[bs.categories.contains('Restaurants')]
  res_bus = res.join(rs, "business_id")
  total_reviews = res_bus.count()


  auth = res_bus\
          .filter(res_bus.text.rlike('[Aa]uthentic[a-z]*'))

  auth_count = auth.count()
  auth_ratio = auth_count / total_reviews
  print(f"Reviews with a variant of authentic: {auth_count}")
  print(f"Ratio of reviews with a variant of authentic: {round(auth_ratio*100, 2)}%") 
  leg = res_bus\
          .filter(res_bus.text.rlike('legitimate'))
  leg_count = leg.count()
  leg_ratio = leg_count / total_reviews

  print(f"Reviews with a variant of legitimate: {leg_count}")
  print(f"Percentage of reviews with a variant of legitimate: {round(leg_ratio*100, 2)}%")

  # get review count per category
  res_bus_exploded = res_bus.withColumn('single_categories', 
      explode(split(col('categories'), ', ')))\
      .groupBy('single_categories').count()\
      .withColumnRenamed('count', 'review_count')

  # get auth review count per category
  auth_exploded = auth.withColumn('single_categories', 
      explode(split(col('categories'), ', ')))\
      .groupBy('single_categories').count()\
      .withColumnRenamed('count', 'authentic_review_count')

  # join and compute ratio of authentic reviews per category
  auth_ratio = res_bus_exploded.join(auth_exploded, on='single_categories')
  auth_ratio = auth_ratio.withColumn('auth_ratio', auth_ratio.authentic_review_count / auth_ratio.review_count)

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

  european = ['Modern European', 'Austrian', 'Italian', 'French', 'Eastern European', 'American (New)']

  non_european = ['Mexican', 'Turkish', 'Thai', 'Indian', 'Indian', 'Chinese', 'Indonesian', 'African', 'Mongolian', 'Taiwanese', 'Argentine', 'Japanese', 'Latin American', 'Filipino', 'Vietnamese', 'Pakistani', 'Burmese', 'New Mexican Cuisine', 'Persian/Iranian', 'Bangladeshi'] 

  res_bus = res_bus.withColumn('single_categories', explode(split(col('categories'), ', ')))

  europeans = res_bus.filter(res_bus.single_categories.isin(european))\
                          .groupBy('single_categories')\
                          .mean('review_stars')\
                          .sort('avg(review_stars)', ascending=False)

  non_europeans = res_bus.filter(res_bus.single_categories.isin(non_european))\
                          .groupBy('single_categories')\
                          .mean('review_stars')\
                          .sort('avg(review_stars)', ascending=False)

  europeans.show()
  non_europeans.show()

  print('Done')

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
