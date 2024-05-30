from pyspark.sql import SparkSession, DataFrame
from src.loaders.TweetsLoader import TweetsLoader
from src.cleaners.TweetsCleaner import TweetsCleaner as Cleaner
from src.analysers.TweetsAnalyser import TweetsAnalyser as Analyser
from src.analysers.TweetsSearch import TweetsSearch as Search

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName('Twitter Posts Analysis')
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")  # to parse dates from financial.csv
             .getOrCreate())

    loader: TweetsLoader = TweetsLoader(spark)

    tweets: DataFrame = loader.load_tweets().cache()
    tweets = Cleaner.clean_tweets(tweets)

    hashtags_stats: DataFrame = Analyser.get_hashtags_stats(tweets)
    hashtags_stats.show()

    retweets_stats: DataFrame = Analyser.get_retweets_stats(tweets)
    retweets_stats.show()

    source_stats: DataFrame = Analyser.get_source_stats(tweets)
    source_stats.show()

    avg_user_followers_per_location: DataFrame = Analyser.get_avg_user_followers_per_location(tweets)
    avg_user_followers_per_location.show()

    search: str = 'Adele'
    search_tweets: DataFrame = Search.search_by_keyword(tweets, search)
    print(search_tweets.count())
    search_tweets.show()

    search_keywords: list[str] = ['Adele', 'Grammys']
    search_tweets: DataFrame = Search.search_by_keywords(tweets, search_keywords)
    print(search_tweets.count())
    search_tweets.show()

    search_hashtags: list[str] = ['Football','Messi']
    search_tweets: DataFrame = Search.search_by_any_hashtag(tweets, search_hashtags)
    print(search_tweets.count())
    search_tweets.show()

    search_hashtags: list[str] = ['breaking', 'grammys', 'award', 'grammy']
    search_location: str = 'London'
    search_tweets: DataFrame = tweets.transform(Search.search_by_any_hashtag, search_hashtags) \
        .transform(Search.search_by_location, search_location)

    print(search_tweets.count())
    search_tweets.show()

    spark.stop()
