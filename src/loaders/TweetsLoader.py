from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_timestamp, col
from src.utils.schemas import tweet_schema, financial_tweet_schema
from src.utils.columns import Columns
import os


class TweetsLoader:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../..', 'data')

    def load_tweets(self) -> DataFrame:
        covid_tweets = self.load_covid_tweets()
        grammy_tweets = self.load_grammy_tweets()
        financial_tweets = (self.load_financial_tweets()
                            .withColumnRenamed('timestamp', Columns.DATE.value)
                            .withColumnRenamed('verified', Columns.USER_VERIFIED.value))

        return covid_tweets.unionByName(grammy_tweets, True).unionByName(financial_tweets, True)

    def load_covid_tweets(self) -> DataFrame:
        file_path = os.path.join(self.data_dir, 'covid19_tweets.csv')
        return (self.spark.read.csv(file_path, header=True, schema=tweet_schema)
                .na.drop())

    def load_grammy_tweets(self) -> DataFrame:
        file_path = os.path.join(self.data_dir, 'GRAMMYs_tweets.csv')
        return (self.spark.read.csv(file_path, header=True, schema=tweet_schema)
                .na.drop())

    def load_financial_tweets(self) -> DataFrame:
        file_path = os.path.join(self.data_dir, 'financial.csv')
        return (self.spark.read.csv(file_path, header=True, schema=financial_tweet_schema)
                .na.drop()
                .withColumn('timestamp', to_timestamp(col('timestamp'), "EEE MMM dd HH:mm:ss Z yyyy")))
