from google.cloud import bigquery
from config import table_path
from logger import logger
from io import BytesIO
import json

# Function for streaming individual rows into BigQuery
def insert_rows_to_bigquery(rows_to_insert):

    # Initialize a BigQuery client
    bigquery_client = bigquery.Client()

    # Check if there are any rows to insert
    if rows_to_insert:
        # Attempt to insert rows and capture any errors
        errors = bigquery_client.insert_rows_json(table_path, rows_to_insert)
        if errors:
            # Log an error if the insert operation fails
            logger.error(f"Failed to insert rows into BigQuery: {errors}")
        else:
            # Log a success message with the count of inserted rows
            logger.info(f"Successfully inserted {len(rows_to_insert)} rows into BigQuery")

# Function for batch loading rows into BigQuery
def batch_load_to_bigquery(rows_to_insert):

    # Initialize a BigQuery client
    bigquery_client = bigquery.Client()

    # Serialize the list of rows to newline-delimited JSON for batch loading
    serialized_data = '\n'.join([json.dumps(msg) for msg in rows_to_insert])

    # Create an in-memory file-like object from the serialized data
    in_memory_file = BytesIO(serialized_data.encode())

    # Configure the job for loading newline-delimited JSON data
    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, # Set format to newline-delimited JSON
        autodetect = False, # Disable schema autodetection
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND # Append data to the existing table
    )

    # Load the data from the in-memory file to the specified BigQuery table
    load_job = bigquery_client.load_table_from_file(
        in_memory_file,
        table_path, # The path to the BigQuery table
        job_config=job_config # The load job configuration
    )

    # Wait for the load job to complete
    load_job.result()

    # Check for errors in the load job, and log accordingly
    if load_job.errors:
        # Log an error if the load job encounters issues
        logger.error(f"Errors during BigQuery load job: {load_job.errors}")
    else:
        # Log a success message with the count of loaded messages and the job ID
        logger.info(f"Successfully batch-loaded {len(rows_to_insert)} messages to BigQuery. Job ID: {load_job.job_id}")
