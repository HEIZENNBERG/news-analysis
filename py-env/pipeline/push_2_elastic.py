from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers
from datetime import datetime

# -------------------------------
# Connect to MongoDB
# -------------------------------
mongo_client = MongoClient("mongodb://mongodb:27017")
db = mongo_client["newsdb"]
collection = db["news_collection_with_ner"]

# -------------------------------
# Connect to Elasticsearch
# -------------------------------
es = Elasticsearch("http://elasticsearch:9200")

# -------------------------------
# Prepare bulk actions
# -------------------------------
actions = []

for doc in collection.find():
    # Safely parse published_at
    pub_date_str = doc.get("published_at")
    if pub_date_str:
        try:
            pub_date = datetime.strptime(pub_date_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            pub_date = None
    else:
        pub_date = None

    # Flatten entities for easier Kibana aggregation
    entities = doc.get("entities", [])
    entity_texts = [e.get("text", "") for e in entities]
    entity_labels = [e.get("label", "") for e in entities]

    es_doc = {
        "_index": "news",
        "_id": str(doc.get("_id")),
        "_source": {
            "article_id": doc.get("article_id", ""),
            "source": doc.get("source_name", ""),
            "title": doc.get("title", ""),
            "description": doc.get("description", ""),
            "url": doc.get("url", ""),
            "category": doc.get("category", ""),
            "sentiment": doc.get("sentiment", ""),
            "published_at": pub_date,
            "entity_texts": entity_texts,
            "entity_labels": entity_labels
        }
    }
    actions.append(es_doc)

# -------------------------------
# Bulk insert into Elasticsearch
# -------------------------------
if actions:
    helpers.bulk(es, actions)
    print(f"✅ Successfully pushed {len(actions)} documents to Elasticsearch")
else:
    print("⚠️ No documents found to push")
