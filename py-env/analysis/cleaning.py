from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.types import StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Clean News Dataset") \
    .getOrCreate()

# Load CSV from HDFS
input_path = "hdfs://namenode:9000/user/onajem/raw-data/raw-data.csv"
df = spark.read.option("header", "true").csv(input_path)

# Drop duplicates
df = df.dropDuplicates()

# Drop rows with null in important fields
important_fields = ['title', 'description', 'content', 'published_at']
df = df.dropna(subset=important_fields)

# Remove special characters from text fields
text_columns = ['title', 'description', 'content', 'author', 'source_name']

for col_name in text_columns:
    df = df.withColumn(col_name, regexp_replace(col(col_name), r"[^a-zA-Z0-9\s.,!?'\"]", ""))
    df = df.withColumn(col_name, trim(col(col_name)))

# Drop rows that became empty after cleaning
df = df.filter(col("content").rlike(r".*[a-zA-Z0-9].*"))  # Keep rows where content has alphanum

# Optional: Normalize case (e.g. lowercase titles)
df = df.withColumn("title", col("title").cast(StringType()))
df = df.withColumn("title", trim(regexp_replace(col("title"), r"\s+", " ")))

# Save cleaned CSV back to HDFS
output_path = "hdfs://namenode:9000/user/onajem/raw-data/cleaned-data.csv"
df.write.mode("overwrite").option("header", "true").csv(output_path)

print("✅ Data cleaned and stored at:", output_path)

spark.stop()
