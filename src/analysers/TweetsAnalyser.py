from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, desc, col, avg
from src.utils.columns import Columns


class TweetsAnalyser:
    @classmethod
    def get_hashtags_stats(cls, tweets: DataFrame) -> DataFrame:
        return tweets.select(explode(Columns.HASHTAGS.value).alias(Columns.HASHTAGS.value)) \
            .groupBy(Columns.HASHTAGS.value) \
            .count() \
            .orderBy(desc('count'))

    @classmethod
    def get_retweets_stats(cls, tweets: DataFrame) -> DataFrame:
        return tweets.groupBy(Columns.IS_RETWEET.value).count()

    @classmethod
    def get_source_stats(cls, tweets: DataFrame) -> DataFrame:
        return tweets.groupBy(Columns.SOURCE.value).count().orderBy(desc('count'))

    @classmethod
    def get_avg_user_followers_per_location(cls, tweets: DataFrame) -> DataFrame:
        return tweets.select(Columns.USER_FOLLOWERS.value, Columns.USER_LOCATION.value, Columns.USER_NAME.value) \
            .filter(col(Columns.USER_LOCATION.value).isNotNull()) \
            .filter(col(Columns.USER_NAME.value).isNotNull()) \
            .dropDuplicates([Columns.USER_NAME.value]) \
            .groupBy(Columns.USER_LOCATION.value) \
            .agg(avg(Columns.USER_FOLLOWERS.value).alias(Columns.USER_FOLLOWERS.value)) \
            .orderBy(desc(Columns.USER_FOLLOWERS.value))
