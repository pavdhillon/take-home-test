

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





# take-home-test
Steps for deployment from terminal

STEP 1
git clone repo locally

STEP 2
gcloud auth login

STEP 3
gcloud config set project devoteam-interview-pav-dhillon

STEP 4
bq mk test_3

STEP 5
gcloud functions deploy process_gcs_file \
    --runtime python310 \
    --trigger-resource test-3-pd-cf \
    --trigger-event google.storage.object.finalize \
    --region us-central1 \
    --entry-point process_gcs_file

STEP 6
Upload file to GCS bucket to test
