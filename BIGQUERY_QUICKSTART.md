# BigQuery Quick Start

## What We Just Created

✅ **Automated Setup Script**: `scripts/setup_bigquery.sh`
✅ **SQL DDL Files**: `scripts/sql/create_event_log.sql` and `create_raw_objects.sql`
✅ **Documentation**: `docs/bigquery-setup.md` (comprehensive guide)
✅ **Environment Template**: `.env.example`
✅ **Updated `.gitignore`**: Excludes credentials and .env files

## Quick Start (5 Steps)

### 1. Install Google Cloud SDK (if not already installed)

```bash
# macOS
brew install --cask google-cloud-sdk

# Linux
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
```

### 2. Create and Configure GCP Project

```bash
# Authenticate
gcloud auth login

# Create project (or use existing)
gcloud projects create lorchestra --name="lorchestra Event Pipeline"

# Set as active project
gcloud config set project lorchestra

# Enable BigQuery API
gcloud services enable bigquery.googleapis.com
```

### 3. Run Automated Setup

```bash
export GCP_PROJECT=lorchestra
export DATASET_NAME=events_dev

bash scripts/setup_bigquery.sh
```

This will:
- ✓ Create BigQuery dataset
- ✓ Create `event_log` table (partitioned, clustered)
- ✓ Create `raw_objects` table (clustered)
- ✓ Create service account with permissions
- ✓ Generate credentials at `~/lorchestra-events-key.json`

### 4. Configure Environment

```bash
# Copy template
cp .env.example .env

# Edit with your actual path
nano .env
```

Update `.env`:
```bash
GCP_PROJECT=lorchestra
EVENTS_BQ_DATASET=events_dev
EVENT_LOG_TABLE=event_log
RAW_OBJECTS_TABLE=raw_objects
GOOGLE_APPLICATION_CREDENTIALS=/home/user/lorchestra-events-key.json
```

### 5. Test Connection

```bash
# Load environment
source .env

# Test BigQuery connection
python3 -c "
from google.cloud import bigquery
client = bigquery.Client()
print('✓ Connected to project:', client.project)
"
```

Expected output:
```
✓ Connected to project: lorchestra
```

## Next Steps

Once BigQuery is set up and tested:

### Run Your First Ingestion Job

```bash
# Make sure environment is loaded
source .env

# Run Gmail ingestion (requires Meltano taps configured)
lorchestra run-job lorchestra gmail_ingest_acct1
```

### Verify Data

```bash
# Check event_log
bq query "SELECT COUNT(*) as event_count FROM events_dev.event_log"

# Check raw_objects
bq query "SELECT COUNT(*) as object_count FROM events_dev.raw_objects"

# View recent events
bq query "
SELECT
  event_type,
  source_system,
  object_type,
  created_at
FROM events_dev.event_log
ORDER BY created_at DESC
LIMIT 10
"
```

### Test Idempotency

```bash
# Run the same job again
lorchestra run-job lorchestra gmail_ingest_acct1

# Verify deduplication
bq query "
SELECT
  COUNT(*) as total_events,
  COUNT(DISTINCT idem_key) as unique_objects
FROM events_dev.event_log
"
```

Expected: `total_events` increases (audit trail), `unique_objects` stays same (deduped).

## Troubleshooting

### Permission Errors

```bash
# Verify service account permissions
gcloud projects get-iam-policy lorchestra \
  --flatten="bindings[].members" \
  --filter="bindings.members:lorchestra-events@lorchestra.iam.gserviceaccount.com"
```

### Table Not Found

```bash
# List datasets
bq ls

# List tables
bq ls events_dev

# Show table schema
bq show --schema events_dev.event_log
```

### Invalid Credentials

```bash
# Verify key file exists
ls -la ~/lorchestra-events-key.json

# Verify JSON is valid
cat ~/lorchestra-events-key.json | jq .

# Check environment variable
echo $GOOGLE_APPLICATION_CREDENTIALS
```

## Architecture Reminder

```
┌─────────────────────────────────────────┐
│  lorc jobs                              │
│  - Reads JSONL from ingestor            │
│  - Calls event_client.emit()            │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│  event_client                           │
│  - INSERT into event_log (audit)        │
│  - MERGE into raw_objects (dedup)       │
└──────────────────┬──────────────────────┘
                   │
                   ▼
         ┌─────────────────────┐
         │ BigQuery            │
         │ - event_log         │
         │ - raw_objects       │
         └─────────────────────┘
```

## Files Reference

- **Setup Script**: `scripts/setup_bigquery.sh`
- **SQL DDL**: `scripts/sql/create_event_log.sql`, `scripts/sql/create_raw_objects.sql`
- **Documentation**: `docs/bigquery-setup.md`
- **Environment Template**: `.env.example`
- **Your Environment**: `.env` (created by you, gitignored)

---

**For detailed information**, see `docs/bigquery-setup.md`
