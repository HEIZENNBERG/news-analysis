import requests
import json
from datetime import datetime
import os
from kafka import KafkaProducer
from hdfs import InsecureClient





TOPIC_NAME = "twitter-ai"

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

HDFS_URL = os.getenv('HDFS_URL', 'http://localhost:9870') 
         

HDFS_DIR = "/user/onajem/twitter-data/"

LOCAL_DIR = "data"

# Create local directory
os.makedirs(LOCAL_DIR, exist_ok=True)

# HDFS client
hdfs_client = InsecureClient(HDFS_URL, user="onajem")


def fetch_tweets():
    print("📡 Fetching tweets from API...")
    try:
        api_key = "8870bd51e8aa4ed1ac41f1682cf4676b"
        url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
        querystring = {"queryType":"Latest","query":"AI"}
        headers = {"X-API-Key": api_key}

        response = requests.get(url, headers=headers, params=querystring)

        if response.status_code != 200:
            print(f"❌ API returned status code {response.status_code}")
            return None

        if not response.content.strip():
            print("❌ Empty response from API.")
            return None

        data = response.json()
        if "tweets" not in data or not data["tweets"]:
            print("⚠️ No tweets found in API response.")
            return None

        return data
    except Exception as e:
        print(f"❌ Error fetching tweets: {e}")
        return None


def send_to_kafka(data):
    print("🚀 Sending tweets to Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        for tweet in data["tweets"]:
            producer.send(TOPIC_NAME, value=tweet)
        producer.flush()
        print(f"✅ Sent {len(data['tweets'])} tweets to Kafka topic '{TOPIC_NAME}'")
    except Exception as e:
        print(f"❌ Kafka error: {e}")


def save_to_hdfs(data):
    print("📤 Saving data to HDFS...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"tweets_{timestamp}.json"
    local_path = os.path.join(LOCAL_DIR, filename)

    # Save locally
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Upload to HDFS
    hdfs_path = HDFS_DIR + filename
    try:
        hdfs_client.upload(hdfs_path, local_path)
        print(f"✅ Uploaded to HDFS at {hdfs_path}")
    except Exception as e:
        print(f"❌ Error uploading to HDFS: {e}")


def main():
    data = fetch_tweets()
    if not data:
        print("⚠️ No data fetched. Exiting.")
        return

    send_to_kafka(data)
    save_to_hdfs(data)


if __name__ == "__main__":
    main()
