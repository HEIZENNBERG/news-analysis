import requests
import json

# Your API key and request setup here
api_key = "8870bd51e8aa4ed1ac41f1682cf4676b"
url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
querystring = {"queryType":"Latest","query":"AI"}
headers = {"X-API-Key": api_key}

response = requests.get(url, headers=headers, params=querystring)

if response.status_code == 200:
    # response.content is bytes, decode to string
    json_str = response.content.decode('utf-8')

    # parse JSON string to Python dict
    data = json.loads(json_str)

    # write the parsed JSON to a file
    with open('tweets.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print("Saved tweets.json successfully!")

else:
    print(f"Error {response.status_code}: {response.text}")
