# BigQuery Setup Instructions (Manual)

Since `gcloud` CLI is not installed, follow these steps to set up BigQuery manually using the GCP Console and Python script.

## Step 1: Create GCP Project (via Console)

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Click "Select a Project" → "New Project"
3. Enter:
   - **Project name**: `lorchestra`
   - **Project ID**: `local-orchestration`
4. Click "Create"

## Step 2: Enable BigQuery API

1. In the GCP Console, go to [APIs & Services](https://console.cloud.google.com/apis/dashboard)
2. Click "+ ENABLE APIS AND SERVICES"
3. Search for "BigQuery API"
4. Click "Enable"

## Step 3: Create Service Account

1. Go to [IAM & Admin → Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts)
2. Make sure project `local-orchestration` is selected
3. Click "+ CREATE SERVICE ACCOUNT"
4. Enter:
   - **Service account name**: `lorchestra-events`
   - **Service account ID**: `lorchestra-events` (auto-filled)
   - **Description**: `Service account for lorchestra event pipeline`
5. Click "CREATE AND CONTINUE"
6. Grant roles:
   - Click "Select a role"
   - Add: `BigQuery Data Editor`
   - Click "+ ADD ANOTHER ROLE"
   - Add: `BigQuery Job User`
7. Click "CONTINUE" → "DONE"

## Step 4: Create Service Account Key

1. In the Service Accounts list, find `lorchestra-events`
2. Click the three dots (⋮) → "Manage keys"
3. Click "ADD KEY" → "Create new key"
4. Select "JSON"
5. Click "CREATE"
6. Save the downloaded JSON file as:
   ```
   ~/lorchestra-events-key.json
   ```
   Or any secure location of your choice

## Step 5: Set Environment Variables

```bash
export GCP_PROJECT=local-orchestration
export DATASET_NAME=events_dev
export GOOGLE_APPLICATION_CREDENTIALS=~/lorchestra-events-key.json
```

**Important**: Replace `~/lorchestra-events-key.json` with the actual path where you saved the key.

## Step 6: Run Python Setup Script

```bash
cd /workspace/lorchestra
python3 scripts/setup_bigquery.py
```

This will:
- ✓ Create dataset `events_dev`
- ✓ Create `event_log` table (partitioned, clustered)
- ✓ Create `raw_objects` table (clustered)
- ✓ Verify tables exist

Expected output:
```
==================================================
BigQuery Setup for lorchestra (Python)
==================================================
Project: local-orchestration
Dataset: events_dev
Location: US
Credentials: /path/to/lorchestra-events-key.json
==================================================

→ Initializing BigQuery client...
✓ BigQuery client initialized
→ Creating dataset: events_dev
✓ Dataset created: local-orchestration.events_dev
→ Creating event_log table...
✓ event_log table created: local-orchestration.events_dev.event_log
→ Creating raw_objects table...
✓ raw_objects table created: local-orchestration.events_dev.raw_objects
→ Verifying table creation...
✓ event_log verified (0 rows)
✓ raw_objects verified (0 rows)

==================================================
✓ BigQuery Setup Complete!
==================================================
```

## Step 7: Create .env File

```bash
cd /workspace/lorchestra
cp .env.example .env
```

Edit `.env` with your actual credentials path:

```bash
GCP_PROJECT=local-orchestration
EVENTS_BQ_DATASET=events_dev
EVENT_LOG_TABLE=event_log
RAW_OBJECTS_TABLE=raw_objects
GOOGLE_APPLICATION_CREDENTIALS=/path/to/lorchestra-events-key.json
```

## Step 8: Test Connection

```bash
source .env

python3 -c "
from google.cloud import bigquery
client = bigquery.Client()
print('✓ Connected to project:', client.project)

# List datasets
datasets = list(client.list_datasets())
print('✓ Datasets:', [d.dataset_id for d in datasets])
"
```

Expected output:
```
✓ Connected to project: local-orchestration
✓ Datasets: ['events_dev']
```

## Step 9: Verify Tables

```bash
python3 -c "
from google.cloud import bigquery
import os

client = bigquery.Client()
dataset_id = os.environ['EVENTS_BQ_DATASET']
project = os.environ['GCP_PROJECT']

# List tables
tables = list(client.list_tables(f'{project}.{dataset_id}'))
print('✓ Tables in', dataset_id)
for table in tables:
    print(f'  - {table.table_id}')
"
```

Expected output:
```
✓ Tables in events_dev
  - event_log
  - raw_objects
```

## Troubleshooting

### "Project not found"
Make sure you're using the correct project ID: `local-orchestration` (not `lorchestra`)

### "Permission denied"
Verify your service account has both roles:
1. Go to [IAM](https://console.cloud.google.com/iam-admin/iam)
2. Find `lorchestra-events@local-orchestration.iam.gserviceaccount.com`
3. Verify it has:
   - BigQuery Data Editor
   - BigQuery Job User

### "Invalid JSON key"
Check that your credentials file is valid JSON:
```bash
cat ~/lorchestra-events-key.json | python3 -m json.tool
```

### "Could not automatically determine credentials"
Make sure `GOOGLE_APPLICATION_CREDENTIALS` is set:
```bash
echo $GOOGLE_APPLICATION_CREDENTIALS
ls -la $GOOGLE_APPLICATION_CREDENTIALS
```

## Next Steps

After setup is complete, you're ready to run your first ingestion job:

```bash
source .env
lorchestra run-job lorchestra gmail_ingest_acct1
```

---

**For automated setup** (if you have gcloud CLI), see `BIGQUERY_QUICKSTART.md`
