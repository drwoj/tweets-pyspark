from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

tweet_schema = StructType([
    StructField("user_name", StringType(), True),
    StructField("user_location", StringType(), True),
    StructField("user_description", StringType(), True),
    StructField("user_created", TimestampType(), True),
    StructField("user_followers", IntegerType(), True),
    StructField("user_friends", IntegerType(), True),
    StructField("user_favourites", IntegerType(), True),
    StructField("user_verified", BooleanType(), True),
    StructField("date", TimestampType(), True),
    StructField("text", StringType(), True),
    StructField("hashtags", StringType(), True),
    StructField("source", StringType(), True),
    StructField("is_retweet", BooleanType(), True)
])

financial_tweet_schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("timestamp", StringType(), True),  # later  parsed to TimestampType
    StructField("source", StringType(), True),
    StructField("symbols", StringType(), True),
    StructField("company_names", StringType(), True),
    StructField("url", StringType(), True),
    StructField("verified", BooleanType(), True)
])
