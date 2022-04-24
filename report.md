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

```
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
