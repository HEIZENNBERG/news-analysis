from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from textblob import TextBlob

# =======================
# 1. Spark session with MongoDB connector + Java driver
# =======================
spark = SparkSession.builder \
    .appName("SentimentAnalysisNews") \
    .config("spark.jars", "/opt/bitnami/spark/jars/mongo-spark-connector_2.12-3.0.1.jar," \
                          "/opt/bitnami/spark/jars/mongodb-driver-sync-4.3.4.jar," \
                          "/opt/bitnami/spark/jars/mongodb-driver-core-4.3.4.jar," \
                          "/opt/bitnami/spark/jars/bson-4.3.4.jar") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/newsdb.news_collection") \
    .getOrCreate()


# =======================
# 2. Read cleaned CSV from HDFS
# =======================
input_path = "hdfs://namenode:9000/user/onajem/raw-data/cleaned-data.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# =======================
# 3. Sentiment analysis function
# =======================
def get_sentiment(text):
    if not text or text.strip() == "":
        return "neutral"
    analysis = TextBlob(text)
    polarity = analysis.sentiment.polarity
    if polarity > 0:
        return "positive"
    elif polarity < 0:
        return "negative"
    else:
        return "neutral"

sentiment_udf = udf(get_sentiment, StringType())

# =======================
# 4. Add sentiment column
# =======================
df_with_sentiment = df.withColumn("sentiment", sentiment_udf(df["content"]))

# =======================
# 5. Write results to MongoDB
# =======================
df_with_sentiment.write \
    .format("mongo") \
    .mode("overwrite") \
    .save()

spark.stop()
