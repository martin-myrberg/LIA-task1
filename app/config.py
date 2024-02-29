from google.cloud import pubsub_v1, bigquery

# Initialize Google Cloud clients
# The subscriber client is used for interacting with Pub/Sub to consume messages.
# The BigQuery client is used for data storage and analytics operations.
subscriber = pubsub_v1.SubscriberClient()
bigquery_client = bigquery.Client()

# Configuration for Pub/Sub and BigQuery resources
# These variables hold the identifiers for the Pub/Sub subscription and the BigQuery dataset and table.
# These identifiers are used to construct the full paths to these resources, which are required for API calls.
project_id = 'clan-bi-analytics-test-bcdf'
subscription_id = 'lia-martin-sub'
dataset_name = 'lia_martin'
table_name = 'task1'

# Construct resource paths
# subscription_path is used to specify the Pub/Sub subscription from which messages will be pulled.
# table_path is used to specify the BigQuery table where data will be stored or queried.
subscription_path = subscriber.subscription_path(project_id, subscription_id)
table_path = f"{bigquery_client.project}.{dataset_name}.{table_name}"
