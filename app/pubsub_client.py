from google.cloud import pubsub_v1
from config import subscription_path
from logger import logger
from google.api_core import retry
import json

# Function to pull messages from a Google Cloud Pub/Sub subscription
def pull_messages_from_pubsub():

    # Initialize a Pub/Sub subscriber client
    subscriber = pubsub_v1.SubscriberClient()

    # Pull messages from the subscription, with a maximum of 10 messages per pull
    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 10},
        retry=retry.Retry(deadline=300), # Set a retry deadline of 300 seconds
    )

    # Initialize an empty list to hold the decoded messages
    messages = []
    
    # Iterate through each received message in the pull response
    for received_message in response.received_messages:

        # Extract the acknowledgement ID (ack_id) from the received message
        ack_id = received_message.ack_id

        # Acknowledge the message using its ack_id to prevent it from being redelivered
        subscriber.acknowledge(subscription=subscription_path, ack_ids=[ack_id])

        # Log the acknowledgement of the message using its ack_id
        logger.info(f"Message with ack_id {ack_id} acknowledged.")

        # Decode the message data from bytes to a string, then parse the JSON into a Python dictionary
        message_data = json.loads(received_message.message.data.decode("utf-8"))

        # Append the decoded message data to the messages list
        messages.append(message_data)

    # Log the total number of messages successfully pulled and processed
    logger.info(f"Successfullt pulled {len(messages)} messages from Pub/Sub.")

    # Close the subscriber client to free up resources
    subscriber.close()

    # Return the list of decoded messages
    return messages
