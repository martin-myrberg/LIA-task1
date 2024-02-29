import os
from google.cloud import pubsub_v1
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\martin.myrberg\Desktop\keys\clan-bi-analytics-test-bcdf-d48acbf74f5f.json"

project_id = "clan-bi-analytics-test-bcdf"
topic_id = "lia-martin"

def publish_message(data, project_id, topic_id):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    data_str = json.dumps(data)
    data_bytes = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data=data_bytes)
    return future.result()

def load_json_schema(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

schema_file_path = r"C:\Users\martin.myrberg\Desktop\Task1\task1.2\schemas\bigquery_schema.json"
data_schema = load_json_schema(schema_file_path)

message_id = publish_message(data_schema, project_id, topic_id)
print(f"Publish message ID: {message_id}")