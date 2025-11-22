# BigQuery Setup - Do This Now

Quick guide to get BigQuery running. Follow these 5 steps:

## Step 1: Create GCP Project (2 minutes)

1. Open: https://console.cloud.google.com/projectcreate
2. Fill in:
   - **Project name**: `lorchestra`
   - **Project ID**: `local-orchestration` ← **IMPORTANT: Use this exact ID**
3. Click "CREATE"
4. Wait for project creation to complete

## Step 2: Enable BigQuery API (1 minute)

1. Make sure `local-orchestration` is selected (top dropdown)
2. Open: https://console.cloud.google.com/apis/library/bigquery.googleapis.com
3. Click "ENABLE"

## Step 3: Create Service Account (3 minutes)

1. Open: https://console.cloud.google.com/iam-admin/serviceaccounts?project=local-orchestration
2. Click "+ CREATE SERVICE ACCOUNT"
3. Fill in:
   - **Service account name**: `lorchestra-events`
   - **Service account ID**: `lorchestra-events` (auto-filled)
   - **Description**: `Service account for lorchestra event pipeline`
4. Click "CREATE AND CONTINUE"
5. **Grant roles** - Add these 2 roles:
   - Click "Select a role" → Search "BigQuery Data Editor" → Select
   - Click "+ ADD ANOTHER ROLE" → Search "BigQuery Job User" → Select
6. Click "CONTINUE"
7. Click "DONE"

## Step 4: Download Credentials (1 minute)

1. In the Service Accounts list, find `lorchestra-events@local-orchestration.iam.gserviceaccount.com`
2. Click the three dots (⋮) on the right
3. Click "Manage keys"
4. Click "ADD KEY" → "Create new key"
5. Select "JSON"
6. Click "CREATE"
7. **Save the file** as `lorchestra-events-key.json`
   - Suggested location: `~/lorchestra-events-key.json`
   - Remember the path - you'll need it next!

## Step 5: Run Setup Script (2 minutes)

Now run the Python setup script:

```bash
# Set environment variables
export GCP_PROJECT=local-orchestration
export DATASET_NAME=events_dev
export GOOGLE_APPLICATION_CREDENTIALS=~/lorchestra-events-key.json

# Run setup
cd /workspace/lorchestra
python3 scripts/setup_bigquery.py
```

Expected output:
```
==================================================
BigQuery Setup for lorchestra (Python)
==================================================
Project: local-orchestration
Dataset: events_dev
...
✓ BigQuery client initialized
✓ Dataset created: local-orchestration.events_dev
✓ event_log table created
✓ raw_objects table created
✓ event_log verified (0 rows)
✓ raw_objects verified (0 rows)

==================================================
✓ BigQuery Setup Complete!
==================================================
```

## Step 6: Create .env File (1 minute)

```bash
cd /workspace/lorchestra

# Copy template
cp .env.example .env

# Edit with your credentials path
nano .env
```

Update `.env` with your actual path:
```bash
GCP_PROJECT=local-orchestration
EVENTS_BQ_DATASET=events_dev
EVENT_LOG_TABLE=event_log
RAW_OBJECTS_TABLE=raw_objects
GOOGLE_APPLICATION_CREDENTIALS=/home/user/lorchestra-events-key.json  # ← YOUR ACTUAL PATH
```

## Step 7: Verify Everything Works

```bash
# Load environment
source .env

# Run verification
python3 scripts/verify_setup.py
```

Expected output:
```
==================================================
BigQuery Setup Verification
==================================================

Environment Variables:
------------------------------------------------------------
✓ GCP_PROJECT=local-orchestration
✓ EVENTS_BQ_DATASET=events_dev
✓ GOOGLE_APPLICATION_CREDENTIALS=/home/user/lorchestra-events-key.json

Credentials File:
------------------------------------------------------------
✓ Valid service account key: lorchestra-events@local-orchestration.iam.gserviceaccount.com

BigQuery Connection:
------------------------------------------------------------
✓ Connected to project: local-orchestration

Dataset:
------------------------------------------------------------
✓ Dataset exists: local-orchestration.events_dev
✓   Location: US
✓   Created: 2025-11-22 ...

Tables:
------------------------------------------------------------
✓ event_log table exists
✓   Rows: 0
✓   Size: 0.00 MB
✓   Partitioned by: created_at
✓   Clustered by: source_system, object_type, event_type
✓ raw_objects table exists
✓   Rows: 0
✓   Size: 0.00 MB
✓   Clustered by: source_system, object_type
✓   Payload field: JSON type ✓

Permissions:
------------------------------------------------------------
✓ Query permission: OK
✓ Service account has necessary permissions

==================================================
✓ All checks passed!
==================================================

Your BigQuery setup is ready to use.
```

## 🎉 Done! What's Next?

Once verification passes, you're ready to ingest data:

### Option 1: Configure Meltano Taps
If you have Meltano taps configured in `/workspace/ingestor`:
```bash
lorchestra run-job lorchestra gmail_ingest_acct1
```

### Option 2: Test with Mock Data
Create a simple test script to verify the pipeline works.

### Check Your Data
```bash
python3 scripts/query_events.py
```

## Troubleshooting

### "Permission denied"
- Check service account has both roles in Step 3
- Verify project ID is `local-orchestration` (not `lorchestra`)

### "Credentials file not found"
- Check the path in your `.env` file
- Verify file exists: `ls -la ~/lorchestra-events-key.json`

### "Dataset not found"
- Make sure Step 5 (setup script) completed successfully
- Re-run: `python3 scripts/setup_bigquery.py`

### Need Help?
- Detailed guide: See `SETUP_INSTRUCTIONS.md`
- Optimization docs: See `docs/bigquery-json-optimization.md`
- Quick reference: See `BIGQUERY_QUICKSTART.md`

---

**Total Time: ~10 minutes**
