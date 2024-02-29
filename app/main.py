import asyncio
import os
import json
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError
from models import MessageModel
from pubsub_client import pull_messages_from_pubsub
from bigquery_client import batch_load_to_bigquery, insert_rows_to_bigquery
from logger import logger

# Set the Google Cloud credentials environment variable ==> access to service account
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"clan-bi-analytics-test-bcdf-d48acbf74f5f.json"

# Initialize FastAPI app
app = FastAPI()

# Initialize an empty buffer for batch message processing
message_buffer = []

# Define the threshold for batch processing
BUFFER_THRESHOLD = 5

# Initialize an asyncio lock for thread-safe operations on the buffer
buffer_lock = asyncio.Lock()


# Define an endpoint for streaming messages directly to BigQuery
@app.get("/stream_messages")
async def stream_messages_endpoint():

     # Attempt to pull messages from Pub/Sub
    try:
        messages = pull_messages_from_pubsub()
    except Exception as e:
        # Log and raise an exception if message pulling fails
        logger.error(f"Failed to pull messages: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to pull messages from Pub/Sub")

    # Initialize a list to store validated messages for insertion
    rows_to_insert = []

    # Validate and append each message to the list for insertion
    for message_data in messages:
        try:
            validated_data = MessageModel(**message_data).dict()
            rows_to_insert.append(validated_data)
        except (json.JSONDecodeError, ValidationError) as e:
            # Log a warning for any invalid messages and skip them
            logger.warning(f"Skipping invalid message: {str(e)}")

    # Attempt to insert the validated rows into BigQuery
    try:
        insert_rows_to_bigquery(rows_to_insert)
    except Exception as e:
        # Log and raise an exception if the BigQuery insert fails
        logger.error(f"Exception during BigQuery insert: {str(e)}")
        raise HTTPException(status_code=500, detail="Exception during BigQuery insert")

    # Return the success response with the count of messages pulled and inserted
    return {"success": True, "pulled_messages": len(rows_to_insert)}


# Define an endpoint for batch loading messages into BigQuery
@app.post("/batch_messages")
async def batch_messages_endpoint():

    # Reference the global message buffer
    global message_buffer

    # Initialize counter for processed messages
    processed_messages = 0

    # Use the asyncio lock to ensure thread-safe operations on the buffer
    async with buffer_lock:
        
        # If buffer doesn't meet the threshold, attempt to pull new messages
        if len(message_buffer) < BUFFER_THRESHOLD:
            try:
                messages = pull_messages_from_pubsub()
                for message_data in messages:
                    try:
                        validated_data = MessageModel(**message_data).dict()
                        message_buffer.append(validated_data) # Append validated messages to the buffer
                    except (json.JSONDecodeError, ValidationError) as e:
                        # Log a warning for any invalid messages and skip them
                        logger.warning(f"Skipping invalid message: {str(e)}")
            except Exception as e:
                 # Log and raise an exception if message pulling fails
                logger.error(f"Exeption during message pulling: {str(e)}")
                raise HTTPException(status_code=500, detail="Exception during message pulling")

        # If the buffer meets the threshold, process the messages
        if len(message_buffer) >= BUFFER_THRESHOLD:
            try:
                # Batch load messages to BigQuery and clear the buffer
                await asyncio.to_thread(batch_load_to_bigquery, message_buffer)
                processed_messages = len(message_buffer) # Update the count of processed messages
                message_buffer.clear() # Clear the buffer after processing
            except Exception as e:
                # Log and raise an exception if batch processing fails
                logger.error(f"Exception during BigQuery batch insert: {str(e)}")
                raise HTTPException(status_code=500, detail="Exception during batch processing")

    # Return the response including the count of processed/inserted messages to bigquery and the current buffer size
    return {"success": True, "inserted_messages": processed_messages, "current_buffer_size": len(message_buffer)}
