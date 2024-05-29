from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, split, col
from src.utils.columns import Columns


class TweetsCleaner:
    @classmethod
    def clean_tweets(cls, tweets: DataFrame) -> DataFrame:
        tweets = (tweets.withColumn(Columns.HASHTAGS.value, regexp_replace(col(Columns.HASHTAGS.value), "^\\[|\\]$", ''))
                  .withColumn(Columns.HASHTAGS.value, regexp_replace(col(Columns.HASHTAGS.value), "'", ''))
                  .withColumn(Columns.HASHTAGS.value, split(col(Columns.HASHTAGS.value), ', ')))

        return tweets
