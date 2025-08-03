from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import os

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'twitter-ai'

# 1. Create topic if it doesn't exist
def create_topic_if_not_exists(topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)

    try:
        topic_list = admin_client.list_topics()
        if topic_name in topic_list:
            print(f"Topic '{topic_name}' already exists.")
        else:
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics([topic])
            print(f"Topic '{topic_name}' created.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    finally:
        admin_client.close()

# 2. Send file content to Kafka
def send_file_to_kafka(filepath, topic=TOPIC_NAME):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    with open(filepath, 'r', encoding='utf-8') as f:
        content = json.load(f)
        producer.send(topic, content)
        producer.flush()
        print(f"Sent {filepath} to Kafka topic '{topic}'")

    producer.close()

# 3. Main execution
if __name__ == "__main__":
    create_topic_if_not_exists(TOPIC_NAME)

    files = sorted(os.listdir('tweets'), reverse=True)
    if files:
        send_file_to_kafka(os.path.join('tweets', files[0]))
        print("Sent the latest JSON file to Kafka successfuly.")
    else:
        print("No JSON files to send.")
    