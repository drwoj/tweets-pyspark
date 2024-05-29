from pyspark.sql import SparkSession, DataFrame
from src.loaders.TweetsLoader import TweetsLoader
from src.cleaners.TweetsCleaner import TweetsCleaner as Cleaner
from src.analysers.TweetsAnalyser import TweetsAnalyser as Analyser

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName('TweetsLoader')
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")  # to parse dates from financial.csv
             .getOrCreate())

    loader: TweetsLoader = TweetsLoader(spark)

    tweets: DataFrame = loader.load_tweets()
    tweets = Cleaner.clean_tweets(tweets)

    hashtags_stats: DataFrame = Analyser.get_hashtags_stats(tweets)
    hashtags_stats.show()

    retweets_stats: DataFrame = Analyser.get_retweets_stats(tweets)
    retweets_stats.show()

    source_stats: DataFrame = Analyser.get_source_stats(tweets)
    source_stats.show()

    avg_user_followers_per_location: DataFrame = Analyser.get_avg_user_followers_per_location(tweets)
    avg_user_followers_per_location.show()
