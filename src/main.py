from pyspark.sql import SparkSession
from src.loaders.TweetsLoader import TweetsLoader
from src.cleaners.TweetsCleaner import TweetsCleaner as Cleaner

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName('TweetsLoader')
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")  # to parse dates from financial.csv
             .getOrCreate())

    loader = TweetsLoader(spark)

    tweets = loader.load_tweets()

    tweets.printSchema()
    tweets.show(5, False)

    tweets = Cleaner.clean_tweets(tweets)
    tweets.show(5, False)
