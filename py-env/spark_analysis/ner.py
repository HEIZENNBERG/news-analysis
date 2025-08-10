from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
import warnings
from time import time

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# ==== CONFIGURATION ====
MONGO_URI = "mongodb://mongodb:27017"
INPUT_DB = "newsdb"
INPUT_COLLECTION = "news_collection"
OUTPUT_COLLECTION = "news_collection_ner"
CONTENT_COL = "content"
NUM_PARTITIONS = 10  # Adjust based on cluster resources

# ==== SPARK SESSION ====
spark = SparkSession.builder \
    .appName("MongoDB_NER_Analysis") \
    .config("spark.mongodb.input.uri", f"{MONGO_URI}/{INPUT_DB}.{INPUT_COLLECTION}") \
    .config("spark.mongodb.output.uri", f"{MONGO_URI}/{INPUT_DB}.{OUTPUT_COLLECTION}") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.default.parallelism", "20") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.network.timeout", "3600000") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.mongodb.read.readPreference.name", "primary") \
    .config("spark.mongodb.read.cursorTimeoutMillis", "3600000") \
    .config("spark.mongodb.read.noCursorTimeout", "false") \
    .getOrCreate()

# ==== SPACY LOADER ====
nlp = None
def load_spacy_model():
    global nlp
    if nlp is None:
        import spacy
        nlp = spacy.load("en_core_web_sm", disable=["parser", "tagger", "lemmatizer"])
        nlp.max_length = 2_000_000
    return nlp

# ==== NER SCHEMA ====
ner_schema = ArrayType(
    StructType([
        StructField("text", StringType(), True),
        StructField("label", StringType(), True)
    ])
)

# ==== NER UDF ====
@udf(ner_schema)
def extract_entities(text):
    if not text or not text.strip():
        return []
    try:
        model = load_spacy_model()
        doc = model(text[:1_000_000])  # Limit to 1M chars
        return [(ent.text, ent.label_) for ent in doc.ents]
    except Exception as e:
        print(f"Error processing text: {str(e)}")
        return []

# ==== MAIN PIPELINE ====
def main():
    # Read from MongoDB
    df = spark.read \
        .format("mongo") \
        .option("partitioner", "MongoSamplePartitioner") \
        .option("partitionerOptions.partitionSizeMB", "64") \
        .load()

    # Filter out empty content
    df = df.filter(col(CONTENT_COL).isNotNull())

    # Repartition for parallelism
    df = df.repartition(NUM_PARTITIONS)

    start_time = time()

    # Perform NER in parallel
    df_with_ner = df.withColumn("entities", extract_entities(col(CONTENT_COL)))

    # Write results back to Mongo
    df_with_ner.write \
        .format("mongo") \
        .mode("append") \
        .option("collection", OUTPUT_COLLECTION) \
        .option("maxBatchSize", "1000") \
        .save()

    duration = time() - start_time
    print(f"NER processing completed in {duration:.2f} seconds")

    # Show sample results
    sample_df = spark.read \
        .format("mongo") \
        .option("collection", OUTPUT_COLLECTION) \
        .load() \
        .limit(5)
    
    print("\nSample results:")
    sample_df.select(CONTENT_COL, "entities").show(5, truncate=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        raise
    finally:
        spark.stop()
        print("Spark session stopped")
