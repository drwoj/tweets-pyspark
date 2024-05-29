from pyspark.sql import DataFrame
from pyspark.sql.functions import  *
from src.utils.columns import Columns

class TweetsAnalyser:
    @classmethod
    def get_hashtags_stats(cls, tweets: DataFrame) -> DataFrame:
        return tweets.select(explode(Columns.HASHTAGS.value).alias('hashtag')) \
            .groupBy('hashtag') \
            .count() \
            .orderBy(desc('count'))
