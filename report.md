# Yelp Review Authenticity Study
## Using PySpark for Large-Scale Data Analysis

*Author: Jonas-Mika Senghaas (jsen@itu.dk), Date: 14.04.2022*

## Project Description

In 2019,
a [study](https://ny.eater.com/2019/1/18/18183973/authenticity-yelp-reviews-white-supremacy-trap)
looked into the use of the word "authentic" in
Yelp reviews and how it was used to signal
different characteristics from cuisine to cuisine.
They found that, for example, in Chinese and
Mexican restaurants, the word was used to describe
typically negative things like "dirty", "kitsch"
or "cheap", where Italian and French restaurants
used to signal high quality and delicious details.
The author sees this tendency as harmful, trapping
non-white restaurant-owners in negative
stereotypes. The task was to test the study by
analysing large scale data (yelp reviews) using
Spark in a distributed cluster of computing and
storage nodes.

## Data

Yelp is an American online-platform, that hosts
crowd-sourced reviews about businesses, such as
restaurants. The data used within this project is
from the [Yelp Academic
Dataset](https://www.yelp.com/dataset/documentation/main)
and contains information of reviews given by Yelp
users to businesses registered on the platform.

The data is saved as JSON-files, which are served
on a distributed cluster of nodes through the
Hadoop Distributed File System. This project uses
Spark to read the files from the HDFS and convert
them into in-memory PySpark Dataframes. These were
used to perform the analyis on.

## Part 1: Simple Queries

### Query 1 - Total Number of Reviews

The `business` dataset contains detailed
information about each business registered on
Yelp. It contains the attribute `review_count`,
which stores the total number of reviews each
business has received. Summing over this column
gives the total number of reviews within the
dataset.

```python
bs.agg(func.sum('review_count')).show()
```

The above query returns the total number of
reviews in a DataFrame object. The total review
count is `645.990`.

### Query 2 - Filter Businesses

In this query, we are to filter all businesses,
which have an overall review 5 stars out of at
least 1000 reviews. The query involves filtering
for both conditions and then selecting the
relevant columns

```python
bs.filter(bs.stars==5)\
  .filter(bs.review_count >= 1000)\
  .select('name', 'stars', 'review_count').show()
```

Running `.show()` on this reveals the `name`, `star`
and `review_count` of only `2` businesses that
fulfill the criteria:

```
+---------------+-----+------------+
|           name|stars|review_count|
+---------------+-----+------------+
|Little Miss BBQ|  5.0|        1936|
|   Brew Tea Bar|  5.0|        1506|
+---------------+-----+------------+
```

### Query 3 - Find Influencers

An influencer is defined to be a Yelp user that
has written more than 1.000 reviews. We can easily
find those and store in a DataFrame containing
only their `user_id` like so:

```python
influencers = us.filter(us.review_count > 1000)\
        .select('user_id')
```

The first 5 columns are:

```
+--------------------+
|             user_id|
+--------------------+
|TEtzbpgA2BFBrC0y0...|
|zzpgpo54-_P-4rzzB...|
|eSlOI3GhroEtcbaD_...|
|d7D4dYzF6THtOx9im...|
|bURBDO2lgSrxnth2P...|
+--------------------+
```

### Query 4 - Businesses with many Influencer Reviews

This query asks to find the businesses that have
been reviewed more than five times by influencers.
To achieve this, we first perform an inner-join of
the DataFrame of `influencers` obtained from query
3 and the reviews DataFrame. This returns
a DataFrame only containing reviews of businesses
given by influencers users. We can then group on
this new DataFrame by the business id and count
the distinct user id, which in this case is simply
the influencer id. 
Finally, we filter for only those businesses that
have been reviewed by more than 5 influencer
users.

```python
inf_rev = rs.join(inf, ["user_id"], 'leftsemi')

# find the distinct no of influencer users per business_id
ans = inf_rev\
        .groupBy('business_id')\
        .agg(countDistinct('user_id')\
        .alias('inf_count'))

# filter for businesses having more than 5 reviews
ans.filter(ans.influencer_count > 5).show()
```

The query finds a total of `3771` such businesses.
The first 5 are being shown.

```
+--------------------+---------+
|         business_id|inf_count|
+--------------------+---------+
|_ixV2SWDy7w8jzEAH...|       24|
|2UgRg5a6KmpbD_SZf...|       24|
|JLbgvGM4FXh9zNP4O...|       18|
|Ch7NAhB_MWSDwcNbc...|        7|
|RtUvSWO_UZ8V3Wpj0...|       24|
+--------------------+---------+
```

### Query 5 - Ordered List of Users based on Average Review Star Count

The last query wants us to output all users
ordered by their average star count given. The
columns `user_id`, `name` and
`average_start_count` should be included in the
final returned DataFrame. The query involves to
join the `user` and `reviews` frames on the
`user_id` attribute. Grouping by the `user_id` and
`name` attribute allows us to aggregate all unique
users and store the average stars given in the
alias `average_rating`. We finally sort by the
`average_rating` columns in descending order. If
there are multiple users with the same
`average_rating`, they are sorting in ascending
order by their name.

```python
  us.join(rs, on='user_id')\
    .groupBy(['user_id', 'name'])\
    .agg(func.mean('stars').alias('average_rating'))\
    .sort(func.desc('average_rating'), func.asc('name'))\
    .show()
```

The first 5 rows look as follows:

```
+--------------------+----------+--------------+
|             user_id|      name|average_rating|
+--------------------+----------+--------------+
|vA5vv-PY9grpqbCld...|   Bernard|           5.0|
|_TqcPWzbwk8csuUkG...|      Zach|           5.0|
|PAInF1ewtaoy-x65D...|   'Andrew|           5.0|
|ahf3roPfkqKqK9B-3...|      'Ark|           5.0|
|N_r0dV5FWuXGc7gfj...|    'Bobby|           5.0|
+--------------------+----------+--------------+
```

## Part 2: Authenticity Study

The mentioned 
[study](https://ny.eater.com/2019/1/18/18183973/authenticity-yelp-reviews-white-supremacy-trap)
finds a worrying relationship, that the word
"authentic" in Yelp reviews of restaurants 
has positive or negative connotations based on the
cuisine. The article mentions, that typically in
non-european cuisines, like Chinese and
Mexican restaurants, being "authentic" is
negatively connotated and associated with
"dirtiness", "kitsch" or the appearance of
a "cheap" restaurant, whereras in european
cuisines the authenticity is associated to high
quality food. Such a tendency is worrying for the
non-european restaurants, because they can only
ever be "authentic" and "bad" or "non-authentic"
and "good". 

The conclusion were drawn from manually inspecting
a couple of thousand reviews. This large-scale
data analysis project on over 6 millions reviews
is trying to verify or falsify the results. 


### Exploratory Data Analysis

Before the data analysis the data was filtered to
only include restaurant-type businesses, since the
article solely focuses on these. 

```python
restaurants = bs[bs.categories.contains('Restaurants')]
```

Although, there is some noise in this filter,
since even non-traditional restaurants are
sometimes have a category tag of restaurant, it
seems like a good enough estimator to exclude all
non-restaurant type businesses and continue the
analysis from there.

Then, the data frame of restaurants was joined on the
review data frame to only look at those reviews
that were written for restaurants.

```python
res_bus = restaurants.join(rs, "business_id")
```

**Proportion of *Authentic* Reviews**

The first challenge was to find the reviews
containing some variant of the word
`authentic`. This was done through a simple Regex
pattern, that was run over the Data Frame of
reviews left on the restaurant-type businesses.

```python
auth = res_bus\
        .filter(res_bus.text.rlike('[Aa]uthentic[a-z]*'))

auth_ratio = auth.count() / res_bus.count()
```

The percentage of reviews of restaurants that include the word
authentic is only `2.59%`. Since we are operating on
a sample size of still over 4 million, this
corresponds to `108.747`. This still seems like
a big enough sample to get meaningful results. 

We can do the exact same thing for the proportion
of reviews containing a variant of the word
`legitimate`. 

```python
leg = res_bus\
        .filter(res_bus.text.rlike('[Ll]egitimate'))

leg_ratio = leg.count() / res_bus.count()
```

There are 2250 reviews containing a variant of the
word legitimate. This only corresponds to `0.05%`
of all reviews of restaurants.
Due to the small sample size and also because the
word legitimate does not carry as clear of an 
semantic meaning as authentic, these 2250 reviews
were disregarded, and the following data analysis
solely focused on the reviews containing the word
`authentic`. 

**Proportion of *Authentic* Reviews by Cuisine**

The central argument of the article revolves
around the word authentic carrying positive
meaning in european cuisines and negative meaning
in non-european cuisine. To further explore this
statement, the reviews filtered for only
restaurants and containing a variant of the word
authentic were grouped by the cuisine type.

To achieve this, first the entire Data Frame was
"exploded" (`pyspark.sql.functions`), st. the list
of categories, which holds information about the
type/ cuisine of restaurant, is a column with
a single string, over which we can aggregate.
This is first done for all reviews of restaurant businesses 
and then only for authentic reviews. 
Joining the two data frames results in knowing the
total number of authentic reviews and total review
each type of restaurant has obtained.

```python
# get review count per category
res_bus_exploded = res_bus.withColumn('single_categories',
    explode(split(col('categories'), ', ')))
res_bus_exploded = res_bus_exploded.groupBy('single_categories').count()
res_bus_exploded = res_bus_exploded.withColumnRenamed('count', 'review_count')

# get auth review count per category
auth_exploded = auth.withColumn('single_categories', explode(split(col('categories'), ', ')))\
      .groupBy('single_categories')\
      .count()\
      .withColumnRenamed('count', 'authentic_review_count')


# join and compute ratio of authentic reviews per category
auth_ratio = res_bus_exploded.join(auth_exploded, on='single_categories')\
      .withColumn('auth_ratio', auth_ratio.authentic_review_count / auth_ratio.review_count)

# show
auth_ratio.sort('review_count', ascending=False).show()
auth_ratio.sort('auth_ratio', ascending=False).show()
```

The results are first shown sorted in descending
order of the total review count, giving inside
into the ratio of authentic reviews for the
biggest categories and then for sorted in
descending order of the authentic ratio, showing
the categories of restaurants that relatively have
the most reviews containing the word authentic.


```
+--------------------+------------+----------------------+--------------------+
|   single_categories|review_count|authentic_review_count|          auth_ratio|
+--------------------+------------+----------------------+--------------------+
|         Restaurants|     4201684|                108747|0.025881765501641722|
|                Food|     1133172|                 23557|0.020788547546180102|
|           Nightlife|     1009498|                 14792|0.014652827444928074|
|                Bars|      974747|                 14171|0.014538131433079558|
|American (Traditi...|      733103|                  3703|0.005051131969177592|
|      American (New)|      729264|                  3621|0.004965280063186994|
+--------------------+------------+----------------------+--------------------+
```

We can see, that the category attribute is not
one-to-one mapping cuisines of restaurants, but
contains additional information. This data frame
was therefore filtered for some of the cuisines
the article mentions. It seems that the
non-european cuisine types have relatively more
reviews that contain the word authentic. The next
task is going to look at the rating of these
reviews to check if there really is a geographical
difference in the sentiment of the word authentic.

```python
# show only relevant categories
categories = ['Mexican', 'Indian', 'Japanese', 'Italian', 'Chinese',
    'Mexican', 'French', 'Greek']

auth_ratio.filter(auth_ratio.single_categories.isin(categories))\
          .sort('auth_ratio', ascending=False).show()
```

```
+-----------------+------------+----------------------+--------------------+
|single_categories|review_count|authentic_review_count|          auth_ratio|
+-----------------+------------+----------------------+--------------------+
|           Indian|       79867|                  5684| 0.07116831732755706|
|          Mexican|      401693|                 25638| 0.06382486127465502|
|          Chinese|      261527|                 14371| 0.05495034929471909|
|            Greek|       75500|                  3115| 0.04125827814569536|
|         Japanese|      309510|                  9180|0.029659784821168943|
|          Italian|      392125|                 11524|  0.0293885878227606|
|           French|      103875|                  1870|0.018002406738868833|
+-----------------+------------+----------------------+--------------------+
```

**Geographic Differences in the use of Authenticity**

The final part of the EDA explores, whether there
are geographic differences in the way to word
authentic is being used in reviews of restaurants.
To achieve this the `cube()` function on pyspark
data frames is used to group on multiple
geographic attributes. It takes a collection of
columns and applies aggregate expressions to all
possible combinations of the grouping columns.

```python
res_bus = res_bus.withColumn("auth_lang",
      res_bus.text.rlike('[Aa]uthentic[a-z]*)'))

res_bus_cube = res_bus.cube("state", "city", "auth_lang")\
                      .count()\
                      .orderBy("count", ascending=False)
```

The query returns a table containing all possible
combinations of state, city and whether a review
contained authenticity language or not. It becomes
clear, that there indeed is great variation as to
how much reviews use the word authentic depending
on the region. However, the query is returning
absolute numbers, and therefore not accounting for
differences in population size and overall review
count. One could dig deeper into this, but I chose
not to, since it didn't seem to give a lot of
insight into the original question of challenging
the article's statement of a disparity in
sentiment of the word authentic.

## Hypothesis Testing

Finally, we would like to challenge the central
statement mentioned in the 
[article](https://ny.eater.com/2019/1/18/18183973/authenticity-yelp-reviews-white-supremacy-trap):

> Authenticity language is used to describe dif-
> ferent characteristics for different cuisines

Extending this argument would mean, that it is
hard for non-white restaurant owners to enter the
higher-end restaurant market, since they would
loose the authentic flare.

I am going to test this hypothesis by looking at
the average star rating from all reviews
containing authentic language in non-european and
european cuisines.

```python
# authentic reviews of european cuisine
auth_exploded.filter(auth_exploded.single_categories.isin(european))\
             .groupBy('single_categories')\
             .sort('avg(review_stars)', ascending=False).show()

auth_exploded.filter(auth_exploded.single_categories.isin(non_european))\
             .groupBy('single_categories')\
             .mean('review_stars')\
             .sort('avg(review_stars)', ascending=False).show()
```

The two queries outputted these average star
rating of reviews containing the a variant of the 
word authentic for the two types of cuisines
mentioned in the article.

```
EUROPEAN:
+-----------------+-----------------+
|single_categories|avg(review_stars)|
+-----------------+-----------------+
|  Modern European|4.356608478802992|
|           French| 4.28288770053476|
|          Italian|4.257636237417564|
|   American (New)|4.037006351836509|
|         Austrian|4.032258064516129|
| Eastern European|              3.4|
+-----------------+-----------------+
```

```
NON-EUROPEAN:
+-------------------+------------------+
|  single_categories| avg(review_stars)|
+-------------------+------------------+
|            Burmese| 4.327868852459017|
|     Latin American| 4.316130705394191|
|          Argentine|4.3088235294117645|
|            Turkish| 4.302832244008715|
|    Persian/Iranian| 4.278656126482214|
|            African| 4.228782287822878|
|New Mexican Cuisine|  4.19327731092437|
|        Bangladeshi|4.1911764705882355|
|            Mexican| 4.103752242764646|
|               Thai| 4.102100089365505|
|             Indian| 4.101688951442646|
|           Filipino| 4.087116564417178|
|          Taiwanese| 4.042675159235669|
|          Pakistani|4.0278035217794255|
|           Japanese| 4.026688453159041|
|            Chinese|3.9867093452091016|
|         Vietnamese|3.9404833233473138|
|         Indonesian|3.8626373626373627|
|          Mongolian|              3.72|
+-------------------+------------------+
```

## Conclusion

The statement of the article, that the word
authentic is differently connotated in european
and non-european cuisines, seems to be wrong. The
average rating of reviews containing some variant
of the word authentic has shown no major
disparities in the average ratings. Given this
data, the statement therefore does not hold.
However, it has to be said, that the methodology
used within this project is rather simplistic. The
entire analysis is based on the strong assumption
that the primary factor influencing the star
rating of a restaurant review is the word
authentic, which does not have to hold. A review
of a non-european cuisine might regard the
restaurant to be authentic (in a positively
connotated way), but still gives a low star
rating, because of i.e. bad service). This
argument obviously also holds in the opposite way.

A more thorough analysis of the reviews could shed
more light onto the true meaning of the word
authentic in reviews of restaurants all over the
world. This project, however, has not shown any
major indication for structural difference in the
use of the word authentic in reviews.

## Bonus Question: Parquet Files

Loading data from parquet is significantly faster. 
