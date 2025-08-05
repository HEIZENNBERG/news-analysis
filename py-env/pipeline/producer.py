# producer.py
import os
import json
import requests
from kafka import KafkaProducer
import time


TOPIC_NAME = "twitter-ai"
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

def fetch_tweets():
    print("📡 Fetching tweets from API...")
    api_key = "9e6ebdff54d748249f3da05f3a3b60db"
    url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
    headers = {"X-API-Key": api_key}
    params = {"queryType": "Latest", "query": "NEWS"}

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"❌ API error: {response.status_code}")
            return []

        data = response.json()
        return data.get("tweets", [])
    except Exception as e:
        print(f"❌ Error fetching tweets: {e}")
        return []

def send_to_kafka(tweets):
    print("🚀 Sending tweets to Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        for tweet in tweets:
            producer.send(TOPIC_NAME, value=tweet)

        producer.flush()
        print(f"✅ Sent {len(tweets)} tweets to Kafka")
    except Exception as e:
        print(f"❌ Kafka error: {e}")

def main():
    tweets = fetch_tweets()
    if tweets:
        send_to_kafka(tweets)
    else:
        print("⚠️ No tweets fetched.")


# def main():
#     while True:
#         tweets = fetch_tweets()
#         if tweets:
#             send_to_kafka(tweets)
#         else:
#             print("⚠️ No tweets fetched.")
#         print("⏳ Sleeping for 5 seconds...\n")
#         time.sleep(5)

if __name__ == "__main__":
    main()
