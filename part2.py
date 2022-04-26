# part2.py
# authenticity study

def eda(rs, bs):
  # filter for restaurants reviews
  restaurants = bs[bs.categories.contains('Restaurants')]
  restaurant_reviews = restaurants.join(rs, "business_id")

  auth = restaurant_reviews\
          .filter(rest_rs.text.rlike('[Aa]uthentic[a-z]*'))

  auth_ratio = auto.count() / restaurant_reviews.count()
  print("Ratio of reviews with a variant of authentic: {auth_ratio}")
  
  leg = restaurant_reviews\
          .filter(rest_rs.text.rlike('legitimate'))
  leg_ratio = auto.count() / restaurant_reviews.count()

  print("Percentage of reviews with a variant of legitimate: {auth_ratio}")
  
def ht(rs, bs):
  pass
