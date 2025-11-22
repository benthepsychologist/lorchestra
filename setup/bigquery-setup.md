# BigQuery Setup Guide

This guide walks you through setting up BigQuery tables and credentials for the lorchestra event pipeline.

## Overview

The lorchestra event pipeline uses BigQuery for event storage with a two-table pattern:

1. **event_log** - Audit trail (metadata only, no payload)
2. **raw_objects** - Deduped object store (with payload)

## Prerequisites

- GCP project (we'll use `lorchestra` as the project ID)
- gcloud CLI installed and authenticated
- BigQuery API enabled in your project
- Sufficient permissions to create datasets, tables, and service accounts

## Installation

### 1. Install Google Cloud SDK

If you don't have gcloud installed:

```bash
# macOS
brew install --cask google-cloud-sdk

# Linux
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Verify installation
gcloud --version
```

### 2. Authenticate and Set Project

```bash
# Authenticate with Google Cloud
gcloud auth login

# Create or set your project
gcloud projects create lorchestra --name="lorchestra Event Pipeline"

# Set as active project
gcloud config set project lorchestra

# Enable BigQuery API
gcloud services enable bigquery.googleapis.com
```

## Automated Setup

The easiest way to set up BigQuery is to use the automated setup script:

```bash
# Set environment variables
export GCP_PROJECT=lorchestra
export DATASET_NAME=events_dev  # or events_prod

# Run setup script
bash scripts/setup_bigquery.sh
```

This script will:
1. Create the BigQuery dataset
2. Create the `event_log` table
3. Create the `raw_objects` table
4. Create a service account (`lorchestra-events`)
5. Grant necessary permissions
6. Generate and download credentials to `~/lorchestra-events-key.json`

## Manual Setup

If you prefer to set up manually or the automated script fails:

### Step 1: Create Dataset

```bash
bq mk --dataset --location=US lorchestra:events_dev
```

### Step 2: Create Tables

Replace `YOUR_PROJECT` and `YOUR_DATASET` in the SQL files:

```bash
# Edit the SQL files
sed -i 's/YOUR_PROJECT/lorchestra/g' scripts/sql/*.sql
sed -i 's/YOUR_DATASET/events_dev/g' scripts/sql/*.sql

# Create event_log table
bq query --use_legacy_sql=false < scripts/sql/create_event_log.sql

# Create raw_objects table
bq query --use_legacy_sql=false < scripts/sql/create_raw_objects.sql
```

### Step 3: Create Service Account

```bash
# Create service account
gcloud iam service-accounts create lorchestra-events \
  --display-name="lorchestra Event Emitter" \
  --project=lorchestra

# Grant permissions
gcloud projects add-iam-policy-binding lorchestra \
  --member="serviceAccount:lorchestra-events@lorchestra.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding lorchestra \
  --member="serviceAccount:lorchestra-events@lorchestra.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Create and download key
gcloud iam service-accounts keys create ~/lorchestra-events-key.json \
  --iam-account=lorchestra-events@lorchestra.iam.gserviceaccount.com
```

## Configuration

### 1. Create .env File

Copy the example environment file and update with your values:

```bash
cp .env.example .env
```

Edit `.env`:

```bash
GCP_PROJECT=lorchestra
EVENTS_BQ_DATASET=events_dev
EVENT_LOG_TABLE=event_log
RAW_OBJECTS_TABLE=raw_objects
GOOGLE_APPLICATION_CREDENTIALS=/workspace/lorchestra/lorchestra-events-key.json
```

### 2. Test Connection

Verify your BigQuery connection:

```bash
# Load environment variables
source .env

# Test Python BigQuery client
python3 -c "
from google.cloud import bigquery
client = bigquery.Client()
print('✓ BigQuery client initialized')
print(f'Project: {client.project}')
"
```

Expected output:
```
✓ BigQuery client initialized
Project: lorchestra
```

## Verify Tables

Check that your tables were created correctly:

```bash
# List datasets
bq ls

# List tables in events_dev
bq ls events_dev

# Show event_log schema
bq show --schema events_dev.event_log

# Show raw_objects schema
bq show --schema events_dev.raw_objects
```

## Table Schemas

### event_log

```sql
event_id         STRING      -- UUID per emit() call
event_type       STRING      -- "gmail.email.received"
source_system    STRING      -- "tap-gmail--acct1-personal"
object_type      STRING      -- "email"
idem_key         STRING      -- References raw_objects
correlation_id   STRING      -- run_id for tracing
subject_id       STRING      -- PHI subject identifier
created_at       TIMESTAMP   -- Event timestamp
status           STRING      -- "ok" | "error"
error_message    STRING      -- Error details if failed
```

Partitioned by `DATE(created_at)`, clustered by `source_system, object_type, event_type`.

### raw_objects

```sql
idem_key         STRING      -- Primary key (NOT ENFORCED)
source_system    STRING      -- "tap-gmail--acct1-personal"
object_type      STRING      -- "email"
external_id      STRING      -- Gmail message_id, etc.
payload          JSON        -- Full object data
first_seen       TIMESTAMP   -- First ingestion
last_seen        TIMESTAMP   -- Last ingestion
```

Clustered by `source_system, object_type`.

## Next Steps

After BigQuery is set up:

1. **Test ingestion**: Run your first job
   ```bash
   lorchestra run-job lorchestra gmail_ingest_acct1
   ```

2. **Query events**: Check that events were written
   ```bash
   bq query "SELECT COUNT(*) FROM events_dev.event_log"
   bq query "SELECT COUNT(*) FROM events_dev.raw_objects"
   ```

3. **Test idempotency**: Re-run the same job
   ```bash
   lorchestra run-job lorchestra gmail_ingest_acct1
   ```

   Verify that:
   - `event_log` has new rows (audit trail)
   - `raw_objects` has same count (deduped)

## Troubleshooting

### "Permission denied" errors

Make sure your service account has the correct roles:
- `roles/bigquery.dataEditor`
- `roles/bigquery.jobUser`

### "Table not found" errors

Verify the table exists:
```bash
bq show lorchestra:events_dev.event_log
```

### "Invalid JSON key file" errors

Check that `GOOGLE_APPLICATION_CREDENTIALS` points to a valid JSON key file:
```bash
cat $GOOGLE_APPLICATION_CREDENTIALS | jq .
```

## Security Notes

- The service account key file contains sensitive credentials
- Store it securely (e.g., `~/.config/gcloud/lorchestra-events-key.json`)
- Never commit it to git (it's in `.gitignore`)
- Rotate keys periodically for production use
- Consider using Workload Identity for GKE deployments instead of service account keys
