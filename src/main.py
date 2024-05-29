from pyspark.sql import SparkSession
from src.loaders.TweetsLoader import TweetsLoader

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName('TweetsLoader')
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")  # to parse dates from financial.csv
             .getOrCreate())

    tl = TweetsLoader(spark)

    tweets = tl.load_tweets()

    tweets.printSchema()
    tweets.show(1, False)
