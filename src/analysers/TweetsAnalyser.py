from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, desc, col, avg, lower
from src.utils.columns import Columns


class TweetsAnalyser:
    @classmethod
    def get_hashtags_stats(cls, tweets: DataFrame) -> DataFrame:
        return tweets.withColumn(Columns.HASHTAGS.value, explode(Columns.HASHTAGS.value)) \
            .groupBy(Columns.HASHTAGS.value) \
            .count() \
            .orderBy(desc('count'))

    @classmethod
    def get_retweets_stats(cls, tweets: DataFrame) -> DataFrame:
        return tweets.filter(col(Columns.IS_RETWEET.value).isNotNull()) \
                .groupBy(lower(col(Columns.IS_RETWEET.value)).alias(Columns.IS_RETWEET.value)) \
                .count() \
                .orderBy(desc('count'))

    @classmethod
    def get_source_stats(cls, tweets: DataFrame) -> DataFrame:
        return tweets.groupBy(Columns.SOURCE.value).count().orderBy(desc('count'))

    @classmethod
    def get_avg_user_followers_per_location(cls, tweets: DataFrame) -> DataFrame:
        return tweets.filter(col(Columns.USER_LOCATION.value).isNotNull()) \
            .filter(col(Columns.USER_NAME.value).isNotNull()) \
            .dropDuplicates([Columns.USER_NAME.value]) \
            .groupBy(Columns.USER_LOCATION.value) \
            .agg(avg(Columns.USER_FOLLOWERS.value).alias(Columns.USER_FOLLOWERS.value)) \
            .orderBy(desc(Columns.USER_FOLLOWERS.value))
