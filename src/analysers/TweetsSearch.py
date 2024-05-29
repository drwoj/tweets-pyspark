from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import *
from src.utils.columns import Columns


class TweetsSearch:
    @classmethod
    def search_by_keyword(cls, tweets: DataFrame, keyword: str) -> DataFrame:
        return tweets.filter(lower(col(Columns.TEXT.value)).contains(keyword.lower())) \
            .select(Columns.TEXT.value, Columns.DATE.value, Columns.SOURCE.value) \
            .orderBy(desc(Columns.DATE.value))

    @classmethod
    def search_by_keywords(cls, tweets:DataFrame, keywords: list[str]) -> DataFrame:
        keywords: list[str] = [keyword.lower() for keyword in keywords]

        return tweets.filter(lower(col(Columns.TEXT.value)).rlike("|".join(keywords))) \
            .select(Columns.TEXT.value, Columns.DATE.value, Columns.SOURCE.value) \
            .orderBy(desc(Columns.DATE.value))

    @classmethod
    def search_by_any_hashtag(cls, tweets: DataFrame, hashtags: list[str]) -> DataFrame:
        keywords_col: Column = array(*[lit(hashtag.lower()) for hashtag in hashtags])

        return tweets.filter(arrays_overlap(col(Columns.HASHTAGS.value), keywords_col)) \
            .select(Columns.HASHTAGS.value, Columns.TEXT.value, Columns.DATE.value, Columns.SOURCE.value) \
            .orderBy(desc(Columns.DATE.value))

    @classmethod
    def search_by_all_hashtags(cls, tweets: DataFrame, hashtags: list[str]) -> DataFrame:
        keywords_col: Column = array(*[lit(hashtag.lower()) for hashtag in hashtags])

        return tweets.filter(size(array_intersect(col(Columns.HASHTAGS.value), keywords_col)) == size(keywords_col)) \
            .select(Columns.HASHTAGS.value, Columns.TEXT.value, Columns.DATE.value, Columns.SOURCE.value) \
            .orderBy(desc(Columns.DATE.value))

    @classmethod
    def search_by_location(cls, tweets: DataFrame, location: str) -> DataFrame:
        return tweets.filter(lower(col(Columns.USER_LOCATION.value)).contains(location.lower())) \
            .select(Columns.USER_LOCATION.value, Columns.TEXT.value, Columns.DATE.value, Columns.SOURCE.value) \
            .orderBy(desc(Columns.DATE.value))
