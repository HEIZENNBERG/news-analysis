import os
import json
import requests
from kafka import KafkaProducer
from datetime import datetime
import time

# Kafka configuration
TOPIC_NAME = "news-ai"
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

# NewsAPI configuration
NEWS_API_KEY = "NEWS_API_key"
NEWS_API_URL = "https://newsapi.org/v2/everything"
QUERY = "AI OR climate OR sports OR politics OR economy OR health OR entertainment OR education OR startup OR travel OR technology OR war OR migration"
LANGUAGE = "en"
PAGE_SIZE = 20  # limit results to avoid hitting quota

def fetch_news():
    print(" Fetching news articles from NewsAPI...")
    headers = {"Authorization": NEWS_API_KEY}
    params = {
        "q": QUERY,
        "language": LANGUAGE,
        "pageSize": PAGE_SIZE,
        "sortBy": "publishedAt"
    }

    try:
        response = requests.get(NEWS_API_URL, headers=headers, params=params)
        if response.status_code != 200:
            print(f" API error: {response.status_code} - {response.text}")
            return []

        data = response.json()
        return data.get("articles", [])
    except Exception as e:
        print(f" Error fetching news: {e}")
        return []

def send_to_kafka(articles):
    print(" Sending news articles to Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        for article in articles:
            producer.send(TOPIC_NAME, value=article)

        producer.flush()
        print(f" Sent {len(articles)} articles to Kafka")
    except Exception as e:
        print(f" Kafka error: {e}")

def save_to_json(articles):
    try:
        #  Create directory if it doesn't exist
        os.makedirs("news_data", exist_ok=True)
        
        #  Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"news_data/news_data_{timestamp}.json"
        
        #  Save JSON
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(articles, f, indent=4, ensure_ascii=False)
        print(f" Saved {len(articles)} articles to {filename}")
    except Exception as e:
        print(f" Error saving to JSON: {e}")

def main():
    i = 0
    while i < 50:
        print(f" Fetching news article [{i}]...")
        articles = fetch_news()
        if articles:
            save_to_json(articles)     # Save locally first
            send_to_kafka(articles)    # Then send to Kafka
        else:
            print("No articles fetched.")
        i += 1
        time.sleep(120)  # Wait for 10 minutes
        

if __name__ == "__main__":
    main()
