import json
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import pubsub_v1

def process_gcs_file(event, context):
    """
    Triggered by a change to a GCS bucket. Processes the uploaded file,
    detects the schema (including RECORDs and repeated RECORDs),
    dynamically determines the table_id, and inserts data into the table.
    Publishes a notification to a Pub/Sub topic upon successful completion.
    """
    bucket_name = event['bucket']
    file_name = event['name']
    dataset_id = "test_3"  # BigQuery dataset
    base_table_id = "test_devoteam"  # Base table name
    pubsub_topic = "projects/devoteam-interview-pav-dhillon/topics/upload_complete_topic"  # Pub/Sub topic name

    try:
        print(f"Processing file: gs://{bucket_name}/{file_name}")

        # Read the JSON file from GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        file_contents = blob.download_as_string().decode("utf-8")

        # Try loading as normal JSON (array of objects)
        try:
            json_data = json.loads(file_contents)
            if not isinstance(json_data, list):
                raise ValueError("JSON file must contain an array of objects")
        except json.JSONDecodeError:
            # Handle line-delimited JSON
            print("Detected line-delimited JSON format.")
            json_data = [json.loads(line) for line in file_contents.splitlines() if line.strip()]

        # Detect schema from JSON
        schema = detect_schema(json_data)
        print("Detected Schema:", schema)

        # Dynamically determine the table_id
        table_id = get_or_create_table(dataset_id, base_table_id, schema)

        # Insert data into the table
        insert_into_bigquery(dataset_id, table_id, json_data)

        print(f"Data successfully inserted into BigQuery table: {table_id}")

        # Publish a notification to Pub/Sub
        notify_job_completion(pubsub_topic, file_name, table_id)

    except Exception as e:
        print(f"Error processing file: {e}")
        raise

def detect_schema(data):
    """Detect BigQuery schema from JSON data."""
    schema = []
    sample = data[0]  # Use the first object to infer schema

    for key, value in sample.items():
        schema.append(detect_field_schema(key, value))

    return schema

def detect_field_schema(key, value):
    """
    Detect the schema for a single field, including support for RECORD and repeated RECORD types.
    """
    field = {"name": key, "mode": "NULLABLE"}  # Default mode

    if isinstance(value, str):
        field["type"] = "STRING"
    elif isinstance(value, int):
        field["type"] = "INTEGER"
    elif isinstance(value, float):
        field["type"] = "FLOAT"
    elif isinstance(value, bool):
        field["type"] = "BOOLEAN"
    elif isinstance(value, list):
        # Check if list contains objects (indicating a repeated RECORD)
        if all(isinstance(item, dict) for item in value):
            field["type"] = "RECORD"
            field["mode"] = "REPEATED"
            field["fields"] = detect_schema(value)
        else:
            # If not objects, assume list of primitive types
            field["type"] = "STRING"  # Default to STRING for non-object arrays
            field["mode"] = "REPEATED"
    elif isinstance(value, dict):
        # If it's a JSON object, it's a non-repeated RECORD
        field["type"] = "RECORD"
        field["fields"] = detect_schema([value])  # Wrap in a list for consistency
    else:
        field["type"] = "STRING"  # Fallback to STRING for unknown types

    return field

def get_or_create_table(dataset_id, base_table_id, schema):
    """
    Check if a table exists in the dataset. If it doesn't, create it with the given schema.
    """
    bigquery_client = bigquery.Client()
    table_id = f"{base_table_id}_{schema_hash(schema)}"  # Dynamically include schema hash

    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    try:
        # Check if table exists
        bigquery_client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except Exception:
        # Table does not exist, create it
        create_bigquery_table(dataset_id, table_id, schema)

    return table_id

def schema_hash(schema):
    """
    Generate a simple hash from the schema to create a unique table ID for different schemas.
    """
    return abs(hash(json.dumps(schema)))

def create_bigquery_table(dataset_id, table_id, schema):
    """Create a BigQuery table with the detected schema."""
    bigquery_client = bigquery.Client()

    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)

    bigquery_client.create_table(table)
    print(f"Created table {table_id} in dataset {dataset_id}")

def insert_into_bigquery(dataset_id, table_id, rows):
    """Insert rows into BigQuery table."""
    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    # Insert rows into BigQuery
    errors = bigquery_client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"Errors occurred while inserting rows: {errors}")
    print("Data insertion complete.")

def notify_job_completion(topic, file_name, table_id):
    """
    Publishes a message to a Pub/Sub topic to notify about job completion.
    """
    publisher = pubsub_v1.PublisherClient()
    message = {
        "file_name": file_name,
        "table_id": table_id,
        "status": "COMPLETED"
    }
    message_json = json.dumps(message).encode("utf-8")

    try:
        publisher.publish(topic, message_json)
        print(f"Notification sent to Pub/Sub topic: {topic}")
    except Exception as e:
        print(f"Failed to publish message: {e}")
        raise
