#!/usr/bin/env python3
"""
BigQuery Setup Script (Python version)

This script sets up BigQuery tables for the lorchestra event pipeline.
Use this if you don't have gcloud CLI installed.

Prerequisites:
- google-cloud-bigquery Python package installed
- GOOGLE_APPLICATION_CREDENTIALS environment variable set
- Service account with BigQuery Admin permissions

Usage:
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
    export GCP_PROJECT=local-orchestration
    export DATASET_NAME=events_dev
    python3 scripts/setup_bigquery.py
"""

import os
import sys
from google.cloud import bigquery
from google.cloud.exceptions import Conflict, NotFound

# ANSI color codes
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
NC = '\033[0m'  # No Color


def error(msg):
    """Print error message and exit"""
    print(f"{RED}ERROR: {msg}{NC}", file=sys.stderr)
    sys.exit(1)


def success(msg):
    """Print success message"""
    print(f"{GREEN}✓ {msg}{NC}")


def info(msg):
    """Print info message"""
    print(f"{YELLOW}→ {msg}{NC}")


def main():
    # Get configuration from environment
    project_id = os.environ.get('GCP_PROJECT')
    dataset_name = os.environ.get('DATASET_NAME', 'events_dev')
    location = os.environ.get('BQ_LOCATION', 'US')

    if not project_id:
        error("GCP_PROJECT environment variable not set")

    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not credentials_path:
        error("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")

    if not os.path.exists(credentials_path):
        error(f"Credentials file not found: {credentials_path}")

    print("")
    print("=" * 50)
    print("BigQuery Setup for lorchestra (Python)")
    print("=" * 50)
    print(f"Project: {project_id}")
    print(f"Dataset: {dataset_name}")
    print(f"Location: {location}")
    print(f"Credentials: {credentials_path}")
    print("=" * 50)
    print("")

    # Initialize BigQuery client
    info("Initializing BigQuery client...")
    try:
        client = bigquery.Client(project=project_id)
        success("BigQuery client initialized")
    except Exception as e:
        error(f"Failed to initialize BigQuery client: {e}")

    # Step 1: Create dataset
    info(f"Creating dataset: {dataset_name}")
    dataset_id = f"{project_id}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    dataset.description = "Event pipeline storage for lorchestra"

    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        success(f"Dataset created: {dataset_id}")
    except Exception as e:
        error(f"Failed to create dataset: {e}")

    # Step 2: Create event_log table
    info("Creating event_log table...")
    event_log_schema = [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_system", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("object_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("idem_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("correlation_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("subject_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
    ]

    event_log_table_id = f"{dataset_id}.event_log"
    event_log_table = bigquery.Table(event_log_table_id, schema=event_log_schema)

    # Configure partitioning and clustering
    event_log_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at"
    )
    event_log_table.clustering_fields = ["source_system", "object_type", "event_type"]
    event_log_table.description = "Event audit trail - one row per emit() call, no payload"

    try:
        event_log_table = client.create_table(event_log_table, exists_ok=True)
        success(f"event_log table created: {event_log_table_id}")
    except Exception as e:
        error(f"Failed to create event_log table: {e}")

    # Step 3: Create raw_objects table
    info("Creating raw_objects table...")
    raw_objects_schema = [
        bigquery.SchemaField("idem_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_system", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("object_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("external_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("first_seen", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("last_seen", "TIMESTAMP", mode="REQUIRED"),
    ]

    raw_objects_table_id = f"{dataset_id}.raw_objects"
    raw_objects_table = bigquery.Table(raw_objects_table_id, schema=raw_objects_schema)

    # Configure clustering
    raw_objects_table.clustering_fields = ["source_system", "object_type"]
    raw_objects_table.description = "Deduped object store - one row per idem_key, with payload"

    try:
        raw_objects_table = client.create_table(raw_objects_table, exists_ok=True)
        success(f"raw_objects table created: {raw_objects_table_id}")
    except Exception as e:
        error(f"Failed to create raw_objects table: {e}")

    # Step 4: Verify tables
    print("")
    info("Verifying table creation...")

    try:
        event_log_table = client.get_table(event_log_table_id)
        success(f"event_log verified ({event_log_table.num_rows} rows)")
    except NotFound:
        error(f"event_log table not found: {event_log_table_id}")

    try:
        raw_objects_table = client.get_table(raw_objects_table_id)
        success(f"raw_objects verified ({raw_objects_table.num_rows} rows)")
    except NotFound:
        error(f"raw_objects table not found: {raw_objects_table_id}")

    # Print next steps
    print("")
    print("=" * 50)
    print("✓ BigQuery Setup Complete!")
    print("=" * 50)
    print("")
    print("Tables created:")
    print(f"  - {event_log_table_id}")
    print(f"  - {raw_objects_table_id}")
    print("")
    print("Next steps:")
    print("1. Create .env file with these settings:")
    print("")
    print(f"   GCP_PROJECT={project_id}")
    print(f"   EVENTS_BQ_DATASET={dataset_name}")
    print("   EVENT_LOG_TABLE=event_log")
    print("   RAW_OBJECTS_TABLE=raw_objects")
    print(f"   GOOGLE_APPLICATION_CREDENTIALS={credentials_path}")
    print("")
    print("2. Test ingestion:")
    print("")
    print("   lorchestra run-job lorchestra gmail_ingest_acct1")
    print("")


if __name__ == "__main__":
    main()
