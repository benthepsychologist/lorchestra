# BigQuery Setup

One-time setup for BigQuery event storage.

## Quick Setup (5 minutes)

Already authenticated with gcloud? Just run:

```bash
export GCP_PROJECT=local-orchestration
export DATASET_NAME=events_dev
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

python3 setup/setup_bigquery.py
```

This creates:
- Dataset: `events_dev`
- Table: `event_log` (partitioned by date, clustered)
- Table: `raw_objects` (clustered, with JSON payload)

## Verify Setup

```bash
# Set environment variables (or use .env file)
export GCP_PROJECT=local-orchestration
export EVENTS_BQ_DATASET=events_dev
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

# Run verification
python3 setup/verify_setup.py
```

Expected output:
```
✓ All checks passed!
```

## Files

- **setup_bigquery.py** - Creates BigQuery dataset and tables
- **verify_setup.py** - Verifies setup is correct
- **sql/** - SQL DDL files for reference
  - `create_event_log.sql` - Event log table schema
  - `create_raw_objects.sql` - Raw objects table schema
  - `add_search_index.sql` - Optional search index for full-text search
  - `create_materialized_view_emails.sql` - Optional materialized view for emails
- **bigquery-setup.md** - Detailed setup guide
- **bigquery-json-optimization.md** - JSON performance optimization guide

## Architecture

### event_log (Audit Trail)
- Stores event metadata (no payload)
- Partitioned by `created_at` date
- Clustered by `source_system`, `object_type`, `event_type`
- One row per `emit()` call

### raw_objects (Deduped Object Store)
- Stores full object payloads as JSON
- Clustered by `source_system`, `object_type`
- One row per `idem_key` (deduplicated)
- MERGE query provides idempotency

## Next Steps

After setup is complete:

1. **Load environment**:
   ```bash
   source .env
   ```

2. **Run ingestion**:
   ```bash
   lorchestra run-job lorchestra gmail_ingest_acct1
   ```

3. **View data**:
   ```bash
   python3 scripts/query_events.py
   ```

## Optimization

See `bigquery-json-optimization.md` for:
- Search indexes (full-text search)
- Materialized views (pre-computed queries)
- Storage Write API (50% cost reduction at scale)
- Extracted columns (for high-frequency queries)
