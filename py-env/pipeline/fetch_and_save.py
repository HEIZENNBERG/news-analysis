import requests
import json
from datetime import datetime
import os

def fetch_data():
    api_key = "8870bd51e8aa4ed1ac41f1682cf4676b"
    url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
    querystring = {"queryType":"Latest","query":"AI"}
    headers = {"X-API-Key": api_key}

    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data")
        return None

def save_to_json(data):
    if not os.path.exists("tweets"):
        os.makedirs("tweets")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"tweets/tweets_{timestamp}.json"
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved to {filename}")
    return filename

if __name__ == "__main__":
    data = fetch_data()
    if data:
        save_to_json(data)
