from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, split, col


class TweetsCleaner:
    @classmethod
    def clean_tweets(cls, tweets: DataFrame) -> DataFrame:
        tweets = (tweets.withColumn('hashtags', regexp_replace(col('hashtags'), "^\\[|\\]$", ''))
                  .withColumn('hashtags', regexp_replace(col('hashtags'), "'", ''))
                  .withColumn('hashtags', split(col('hashtags'), ', ')))

        return tweets
