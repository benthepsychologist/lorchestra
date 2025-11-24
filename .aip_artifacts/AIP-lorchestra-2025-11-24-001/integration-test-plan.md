# Integration Test Plan

## Prerequisites

1. **Apply BigQuery schema changes:**
   ```bash
   # Replace YOUR_PROJECT and YOUR_DATASET with actual values
   export GCP_PROJECT=$(gcloud config get-value project)
   export DATASET_NAME=events_dev

   # Apply schema alterations
   sed "s/YOUR_PROJECT.YOUR_DATASET/${GCP_PROJECT}.${DATASET_NAME}/g" \
       setup/sql/alter_event_log.sql | \
       bq query --use_legacy_sql=false
   ```

2. **Verify schema changes:**
   ```bash
   bq show --schema ${DATASET_NAME}.event_log
   # Should show:
   # - idem_key: NULLABLE (was REQUIRED)
   # - payload: JSON, NULLABLE (new)
   # - trace_id: STRING, NULLABLE (new)
   ```

## Test Execution

### Step 1: Run a Gmail Job

```bash
source .env
lorchestra run gmail_ingest_acct1 --since "2025-11-20"
```

Expected output:
- Job starts successfully
- Logs show batch upsert operation
- Job completes with success message

### Step 2: Verify Event Log

Query event_log for job events:

```sql
SELECT
  event_type,
  object_type,
  correlation_id,
  status,
  payload,
  idem_key,
  trace_id
FROM `${DATASET_NAME}.event_log`
WHERE correlation_id LIKE 'gmail_ingest_acct1-%'
ORDER BY created_at DESC
LIMIT 10;
```

**Expected results:**
1. `job.started` event:
   - object_type = "job_run"
   - idem_key = NULL
   - payload contains: job_name, package_name, parameters

2. `ingestion.completed` event:
   - idem_key = NULL
   - payload contains: records_extracted, duration_seconds, date_filter

3. `job.completed` event:
   - object_type = "job_run"
   - idem_key = NULL
   - payload contains: job_name, package_name, duration_seconds

**Critical assertions:**
- ✓ All events have small telemetry payloads (NOT full email objects)
- ✓ idem_key is NULL for all telemetry events
- ✓ NO per-object events (no "email.received" events)

### Step 3: Verify Raw Objects

Query raw_objects for ingested emails:

```sql
SELECT
  COUNT(*) as email_count,
  source_system,
  object_type
FROM `${DATASET_NAME}.raw_objects`
WHERE source_system = 'tap-gmail--acct1-personal'
  AND object_type = 'email'
GROUP BY source_system, object_type;
```

**Expected results:**
- Email count > 0 (emails were ingested)
- Full email payloads stored in raw_objects

### Step 4: Critical Assertion - No Per-Object Events

```sql
-- This query should return 0 rows!
SELECT COUNT(*) as per_object_events
FROM `${DATASET_NAME}.event_log`
WHERE event_type = 'email.received'
  AND correlation_id LIKE 'gmail_ingest_acct1-%';
```

**Expected result:** 0 rows (no per-object events)

This is the most important verification - we should have moved from "100 events + 100 merges" to "1 event + 1 batch merge".

## Success Criteria

All of the following must be true:

- [ ] Unit tests pass (20/20)
- [ ] Gmail job completes successfully
- [ ] event_log has job.started, ingestion.completed, job.completed events
- [ ] event_log rows have small telemetry payloads (NOT full objects)
- [ ] raw_objects has full email payloads
- [ ] event_log.idem_key is NULL for telemetry events
- [ ] **NO per-object events were written** (CRITICAL)
- [ ] BigQuery schema supports nullable idem_key, payload, and trace_id

## Rollback Plan

If integration test fails:

1. Restore previous event_client.py from git
2. Restore previous ingest_gmail.py from git
3. Revert BigQuery schema changes (if needed)
