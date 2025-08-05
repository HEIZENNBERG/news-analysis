# consumer.py
import os
import json
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

TOPIC_NAME = "twitter-ai"

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870')

HDFS_DIR = "/user/onajem/twitter-data/"

LOCAL_DIR = "data"

os.makedirs(LOCAL_DIR, exist_ok=True)
hdfs_client = InsecureClient(HDFS_URL, user="onajem")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='tweet-consumer-group'
)

print(f"👂 Listening to topic '{TOPIC_NAME}'...")

for message in consumer:
    tweet = message.value
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"tweet_{timestamp}.json"
    local_path = os.path.join(LOCAL_DIR, filename)
    hdfs_path = os.path.join(HDFS_DIR, filename)

    # Save locally
    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(tweet, f, indent=2, ensure_ascii=False)

    # Upload to HDFS
    try:
        hdfs_client.upload(hdfs_path, local_path)
        print(f"✅ Tweet saved to HDFS at {hdfs_path}")
    except Exception as e:
        print(f"❌ HDFS upload error: {e}")
