

# main.py: GCS File Processing and BigQuery Integration

## Overview

This script is designed to process files uploaded to a Google Cloud Storage (GCS) bucket. It detects the schema of the JSON data, dynamically determines the appropriate BigQuery table, and inserts the data into the table. Upon successful completion, it publishes a notification to a Google Cloud Pub/Sub topic.

## Process Flow

```mermaid
flowchart TD
    A("Start: File Upload to GCS") --> B["process_gcs_file(event, context)"]
    B --> C["Read JSON from GCS"]
    C --> D{"Is JSON Line-delimited?"}
    D -->|No| E["Load JSON as Array of Objects"]
    D -->|Yes| F["Load Line-delimited JSON"]
    E --> G["Detect Schema"]
    F --> G
    G --> H["Determine Table ID"]
    H --> I["Insert Data into BigQuery"]
    I --> J["Publish Notification to Pub/Sub"]
    J --> K("End: Process Complete")
```
# Documentation of the file main.py

## Introduction
This file, `main.py`, is designed to process files uploaded to a Google Cloud Storage (GCS) bucket. It detects the schema of the uploaded JSON file, dynamically determines the appropriate BigQuery table, inserts the data into the table, and publishes a notification to a Google Cloud Pub/Sub topic upon successful completion.

## Description
The script is triggered by changes to a GCS bucket. It processes the uploaded JSON file, detects its schema, and dynamically determines the BigQuery table ID. The data is then inserted into the table. Upon successful data insertion, a notification is published to a specified Pub/Sub topic. The script handles both standard JSON and line-delimited JSON formats.

## Structure
The file is structured to include functions for processing the GCS file, detecting the schema, managing BigQuery tables, inserting data, and notifying job completion via Pub/Sub.

## Dependencies
- `google.cloud.bigquery`: For interacting with BigQuery.
- `google.cloud.storage`: For accessing and downloading files from GCS.
- `google.cloud.pubsub_v1`: For publishing messages to Pub/Sub topics.
- `json`: For parsing JSON data.

## Imports
```python
import json
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import pubsub_v1
```

## Variables
- `bucket_name`: The name of the GCS bucket where the file is stored.
- `file_name`: The name of the file in the GCS bucket.
- `dataset_id`: The ID of the BigQuery dataset.
- `base_table_id`: The base name for the BigQuery table.
- `pubsub_topic`: The name of the Pub/Sub topic for notifications.

## Methods

### `process_gcs_file(event, context)`
Triggered by a change to a GCS bucket. It processes the uploaded file, detects the schema, determines the table ID, inserts data into the table, and publishes a notification to a Pub/Sub topic.

### `detect_schema(data)`
Detects the BigQuery schema from JSON data. It uses the first object in the JSON array to infer the schema.

### `detect_field_schema(key, value)`
Detects the schema for a single field, including support for RECORD and repeated RECORD types.

### `get_or_create_table(dataset_id, base_table_id, schema)`
Checks if a table exists in the dataset. If it doesn't, it creates the table with the given schema.

### `schema_hash(schema)`
Generates a simple hash from the schema to create a unique table ID for different schemas.

### `create_bigquery_table(dataset_id, table_id, schema)`
Creates a BigQuery table with the detected schema.

### `insert_into_bigquery(dataset_id, table_id, rows)`
Inserts rows into a BigQuery table.

### `notify_job_completion(topic, file_name, table_id)`
Publishes a message to a Pub/Sub topic to notify about job completion.

## Example
To use this file, deploy it as a Cloud Function that is triggered by changes to a specific GCS bucket. Ensure that the necessary Google Cloud services (BigQuery, Pub/Sub) are properly configured and that the appropriate permissions are set.

## Notes
- Ensure that the Google Cloud services (BigQuery, GCS, Pub/Sub) are properly configured and that the necessary permissions are granted.
- The script assumes that the JSON file contains an array of objects or is line-delimited.

## Vulnerabilities
- The script does not handle all possible JSON formats, which may lead to errors if the input JSON is not in the expected format.
- There is no explicit error handling for network-related issues when interacting with Google Cloud services.

## Insights

- The script handles both standard JSON arrays and line-delimited JSON formats.
- It dynamically creates a BigQuery table if it does not exist, using a schema derived from the JSON data.
- A unique table ID is generated using a hash of the schema to accommodate different data structures.
- Notifications of successful data processing are sent to a Pub/Sub topic, facilitating integration with other systems.

## Dependencies

```mermaid
flowchart LR
    main_py --- |"Uses"| google_cloud_bigquery
    main_py --- |"Uses"| google_cloud_storage
    main_py --- |"Uses"| google_cloud_pubsub_v1
```

- `google.cloud.bigquery`: Used for interacting with BigQuery to create tables and insert data.
- `google.cloud.storage`: Used for accessing and reading files from Google Cloud Storage.
- `google.cloud.pubsub_v1`: Used for publishing messages to a Pub/Sub topic to notify about job completion.


# requirements.txt: Google Cloud Services Dependencies

## Overview

This document outlines the dependencies required for integrating with various Google Cloud services. These dependencies are essential for applications that interact with Google Cloud BigQuery, Google Cloud Storage, and Google Cloud Pub/Sub.

## Process Flow

```mermaid
flowchart TD
    A[Google Cloud Services] --> B[google-cloud-bigquery]
    A --> C[google-cloud-storage]
    A --> D[google-cloud-pubsub]
```

## Insights

- The application relies on Google Cloud services for data storage, processing, and messaging.
- `google-cloud-bigquery` is used for data warehousing and analytics.
- `google-cloud-storage` is utilized for storing and retrieving large datasets.
- `google-cloud-pubsub` is employed for asynchronous messaging and event-driven systems.

## Dependencies

```mermaid
flowchart LR
    requirements_txt --- |"Includes"| google_cloud_bigquery
    requirements_txt --- |"Includes"| google_cloud_storage
    requirements_txt --- |"Includes"| google_cloud_pubsub
```

- `google-cloud-bigquery`: Provides tools for interacting with Google BigQuery, a fully-managed data warehouse.
- `google-cloud-storage`: Offers capabilities for storing and accessing data on Google Cloud Storage.
- `google-cloud-pubsub`: Facilitates message-oriented middleware for sending and receiving messages between independent applications.





# Steps for Deployment from IDE/terminal
Follow the below steps to successfully deploy the above workflow to Google Cloud Platform

## STEP 1
Clone the repo locally:

```
git clone https://github.com/pavdhillon/take-home-test.git
```

## STEP 2
Authenticate to GCP via commandline:

```
gcloud auth login
```

## STEP 3
Set the project you wish to deploy in:

```
gcloud config set project devoteam-interview-pav-dhillon
```

## STEP 4
Create a BigQuery Dataset. This will be the dataset the Cloud Function deploys the tables in. 
Edit he parameter as needed - [DATASET_NAME]: The name of the dataset to create. Dataset names must be unique within a project.

```
gcloud bigquery datasets create [DATASET_NAME]
```

## STEP 5
Create a GCS bucket: 
Edit the parameters as needed - [BUCKET_NAME]: The name of the bucket. It must be unique across all GCP buckets.
--location [LOCATION]: The location where the bucket resides. Examples: US, EU, asia-south1.
--storage-class [STORAGE_CLASS]: (Optional) The type of storage class. Examples: STANDARD, NEARLINE, COLDLINE, ARCHIVE. Defaults to STANDARD.
```
gcloud storage buckets create [BUCKET_NAME] \
    --location [LOCATION] \
    --storage-class [STORAGE_CLASS]
```

## STEP 6
Create Pub/Sub Topic:
Edit the parameters as needed - Parameters [TOPIC_NAME]: The name of the topic. Topic names must follow GCP naming conventions.

```
gcloud pubsub topics create [TOPIC_NAME]
```

## STEP 7
Edit the `main.py` file to include the parameters from steps 4,5 & 6.

## STEP 8
Deploy the Cloud Function:

```
gcloud functions deploy process_gcs_file \
    --runtime python310 \
    --trigger-resource test-3-pd-cf \
    --trigger-event google.storage.object.finalize \
    --region us-central1 \
    --entry-point process_gcs_file
```

## STEP 9
Test.
In order to test the workflow, navigate to the `json_test_files` folder and upload each file to the GCS bucket. This will invoke the cloud function to create a table in BigQuery and then publish a message to Pub/Sub when the upload has completed. To also verify the steps each Cloud Function is taking, look at the logs for the Cloud Function.



