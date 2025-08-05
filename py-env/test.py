import requests
import json
from datetime import datetime

# API request setup
url = "https://instagram-statistics-api.p.rapidapi.com/posts"
querystring = {
    "cid": "INST:17841400005463628",
    "from": "01.11.2023",
    "to": "30.11.2023",
    "type": "posts",
    "sort": "date"
}
headers = {
    "x-rapidapi-key": "9a8a0dc49cmsh95bf0e1d896f7eep1fa1aejsn62fcb2e4f98f",
    "x-rapidapi-host": "instagram-statistics-api.p.rapidapi.com"
}

# Make the request
response = requests.get(url, headers=headers, params=querystring)
data = response.json()

# Create filename with current timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"rapid_tweet_{timestamp}.json"

# Save JSON to file
with open(filename, "w", encoding='utf-8') as f:
    json.dump(data, f, indent=4, ensure_ascii=False)

print(f"Saved data to {filename}")
