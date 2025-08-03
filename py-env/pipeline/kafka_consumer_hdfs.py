from kafka import KafkaConsumer
import json
import requests
from datetime import datetime

consumer = KafkaConsumer(
    'twitter-ai',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='tweet-consumer-group'
)

def write_to_hdfs(data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    hdfs_path = f"/tweets/tweets_{timestamp}.json"
    
    # WebHDFS must be enabled on HDFS, adjust host and port as needed
    hdfs_url = f"http://localhost:9870/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true"
    response = requests.put(
        hdfs_url,
        data=json.dumps(data),
        headers={'Content-Type': 'application/octet-stream'},
        allow_redirects=False
    )

    if response.status_code == 307:
        redirect_url = response.headers['Location']
        put_response = requests.put(redirect_url, data=json.dumps(data))
        if put_response.status_code in [200, 201]:
            print(f"Saved to HDFS: {hdfs_path}")
        else:
            print(f"Failed to save to HDFS: {put_response.text}")
    else:
        print(f"Initial PUT failed: {response.text}")

if __name__ == "__main__":
    print("Waiting for messages...")
    for message in consumer:
        write_to_hdfs(message.value)
