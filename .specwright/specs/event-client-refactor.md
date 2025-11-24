---
version: "0.1"
tier: C
title: Event Client Refactor + Job Logging
owner: benthepsychologist
goal: Refactor event_client to separate event logging from object storage, and add job execution tracking
labels: [refactor, architecture-fix, observability]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-11-24T00:39:58.858257+00:00
updated: 2025-11-24T01:45:00.000000+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/event-client-refactor"
---

# Event Client Refactor + Job Logging

## Objective

Refactor `event_client` to separate concerns and add job execution tracking.

### Background

**Current state (WRONG):**
```python
# Emits ONE event per email (100 emails = 100 INSERT + 100 MERGE operations)
for record in iter_jsonl_records(jsonl_path):
    emit(
        event_type="email.received",  # Per-object event
        payload=record,  # Full 73KB email body
        source=source_system,
        object_type="email",
        bq_client=bq_client,
        correlation_id=run_id
    )
```

**Problems:**
1. ❌ Upsert is an ACTION, not an EVENT
2. ❌ event_log has no payload column but we need to store small telemetry
3. ❌ No way to emit telemetry without triggering object upsert
4. ❌ No batch operations (N emails = N separate MERGE statements)
5. ❌ Confusing API: one function does two unrelated things
6. ❌ NO tracking of job execution itself (when jobs start/complete, parameters used, success/failure)

**Solution:**
Split `emit()` into two explicit functions:
- `log_event()` - Records facts to event_log (telemetry/audit) with small payload
- `upsert_objects()` - Batch-merges objects into raw_objects (side-effecting action)

Then use `log_event()` to track job execution lifecycle.

### Acceptance Criteria

**Event Client Refactor:**
- `log_event()` function writes event envelopes + small telemetry payload to event_log (no object storage)
- `upsert_objects()` function batch-merges objects into raw_objects
- event_log.idem_key is nullable (not required for telemetry events)
- event_log.payload is nullable JSON for small telemetry (job params, counts, duration)
- trace_id supported (nullable) for future cross-system tracing
- Gmail job refactored to use batch pattern (1 log_event + 1 batch upsert per run)
- NO per-object events written for ingested data

**Job Logging:**
- Job start emits `job.started` event with parameters
- Job completion emits `job.completed` event with metrics (duration)
- Job failure emits `job.failed` event with error message
- All job events use `object_type="job_run"` and same `correlation_id`
- Job events use ONLY log_event() (no raw_objects writes)

**Testing & CI:**
- Tests verify new API works correctly
- Tests verify job events are emitted correctly
- Tests verify NO per-object events written during ingestion
- CI green (lint + unit)

### Constraints

- NO backward compatibility needed - we are not in production
- MUST support batch operations (list of objects)
- MUST keep two-table pattern (event_log + raw_objects)
- Schema changes: make idem_key nullable, add payload JSON nullable, add trace_id STRING nullable
- Job events use ONLY log_event() - no raw_objects writes for job telemetry
- NO assumption that all objects have 'id' field (idem_key_fn is required and explicit)

### Event Naming Vocabulary

Establish clear event taxonomy to keep queries sane:
- **job.*** = orchestrator lifecycle (lorchestra CLI)
- **ingestion.*** = extractor lifecycle (tap wrapper jobs)
- Future: **canonize.***, **score.***, **report.*** for other pipeline stages

### Correlation ID Hierarchy

Lock down the correlation model:
- **correlation_id** is always the job run_id (e.g., `gmail_ingest_acct1-20251124143000`)
- All events from a job run (job.started, ingestion.completed, job.completed) share the same correlation_id
- Ingestion/canonizer/etc. events are children under that correlation_id
- If sub-correlation is ever needed, add step_id in payload (not as separate ID column)

## Plan

### Step 1: Update BigQuery Schema

**Prompt:**

Update the event_log schema to support the refactored event architecture:
1. Make idem_key nullable (telemetry events don't have idem_keys)
2. Add payload column (JSON, nullable) for small telemetry data
3. Add trace_id field (STRING, nullable) for future cross-system tracing

Do NOT modify raw_objects - it already works correctly.

**Commands:**

```bash
# Apply schema changes
bq query --use_legacy_sql=false < lorchestra/setup/sql/alter_event_log.sql

# Verify schema
bq show --schema --format=prettyjson local-orchestration:events_dev.event_log | grep -E "(idem_key|payload|trace_id)"
```

**Validation:**

Before proceeding, verify:
- ✓ idem_key column is nullable
- ✓ payload column exists (JSON, nullable)
- ✓ trace_id column exists (STRING, nullable)
- ✓ No errors in schema alteration

**Outputs:**

- `lorchestra/setup/sql/alter_event_log.sql` (new)

---

### Step 2: Implement New Event Client API

**Prompt:**

Implement the refactored event client with two explicit functions in `lorchestra/stack_clients/event_client/__init__.py`:

1. **log_event()** - Write event envelopes to event_log:
   - Parameters: event_type, source_system, correlation_id, trace_id (optional), object_type (optional), status, error_message (optional), idem_key (optional), payload (optional dict), bq_client
   - Payload should be small telemetry only (job params, counts, duration) - NOT full objects
   - idem_key defaults to NULL for telemetry events; can be set to link to a specific object if needed
   - Insert to event_log using insert_rows_json() - payload inserted as native JSON dict (not stringified)
   - error_message: human-readable summary for status="failed"
   - payload: machine-parsable metadata (error_type, stack_hint, retryable, job params, etc.)

2. **upsert_objects()** - Batch MERGE objects into raw_objects:
   - Parameters: objects (list or iterator), source_system, object_type, correlation_id, trace_id (optional), idem_key_fn (callable, required), batch_size (optional, default 5000), bq_client
   - idem_key_fn: function that computes idempotency key from object payload (e.g., `gmail_idem_key(obj)`)
   - idem_key_fn is REQUIRED - forces every tap to think about identity explicitly
   - batch_size: tunable for large payload sources; default 5000; lower if hitting BQ load limits or timeouts
   - Process in batches (don't materialize entire dataset)
   - Load batch via load_table_from_json() to temp table (named `temp_objects_{run_id}_{batch_idx}`)
   - MERGE from temp table to raw_objects
   - Cleanup temp table in finally block
   - Support both list and iterator inputs

3. **Remove emit()** entirely - no deprecation wrapper needed (not in production)

**Commands:**

```bash
# Run linter
ruff check lorchestra/stack_clients/event_client/

# Run tests
pytest tests/test_event_client.py -v
```

**Validation:**

Before proceeding, verify:
- ✓ log_event() writes to event_log with small payload
- ✓ upsert_objects() performs batch MERGE correctly
- ✓ upsert_objects() handles custom idem_key_fn
- ✓ upsert_objects() processes in batches (not full materialization)
- ✓ upsert_objects() respects batch_size parameter
- ✓ emit() has been removed entirely
- ✓ All tests pass
- ✓ No linting errors

**Outputs:**

- `lorchestra/stack_clients/event_client/__init__.py` (updated)
- `lorchestra/idem_keys.py` (new - registry of idem_key functions)
- `tests/test_event_client.py` (updated with new tests)

---

### Step 3: Refactor Gmail Job

**Prompt:**

Update Gmail ingestion job to use the new event client pattern in `lorchestra/jobs/ingest_gmail.py`:

1. Import log_event and upsert_objects
2. Import gmail_idem_key from lorchestra.idem_keys (or define inline if idem_keys.py doesn't exist yet)
3. In _ingest_gmail():
   - Stream JSONL records into batches (don't call list() to materialize everything)
   - Log ingestion.completed event with telemetry payload: {records_extracted, duration_seconds, date_filter}
   - Upsert objects in batches: `upsert_objects(objects=iter_jsonl_records(jsonl_path), idem_key_fn=gmail_idem_key, ...)`
   - On failure, log ingestion.failed event with error details

This changes from "100 events + 100 merges" to "1 event + 1 batch merge operation".

**Recommended pattern**: Create `lorchestra/idem_keys.py` with a registry of idem_key functions to avoid drift:
```python
def gmail_idem_key(source_system: str):
    """Gmail uses 'id' field for messageId."""
    return lambda obj: f"{source_system}:email:{obj['id']}"

def stripe_charge_idem_key(source_system: str):
    """Stripe charges use 'id' field."""
    return lambda obj: f"{source_system}:charge:{obj['id']}"

# Add more as needed for other taps
```

This prevents accidentally changing identity rules later.

**Commands:**

```bash
# Run linter
ruff check lorchestra/jobs/ingest_gmail.py

# Run job tests
pytest tests/test_cli_jobs.py -k gmail -v
```

**Validation:**

Before proceeding, verify:
- ✓ Gmail job uses log_event() + upsert_objects()
- ✓ No more per-record emit() calls
- ✓ Batch operation pattern implemented (no full materialization)
- ✓ Uses ingestion.completed (not email.received)
- ✓ Tests pass
- ✓ No linting errors

**Outputs:**

- `lorchestra/jobs/ingest_gmail.py` (updated)

---

### Step 4: Add Job Logging to CLI

**Prompt:**

Add job execution tracking to `lorchestra/cli.py` in the `_run_job_impl()` function:

1. Import log_event from event_client
2. Generate run_id: `f"{job_name}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"`
3. Before execute_job():
   - Log job.started event with payload: {job_name, package_name, parameters}
4. After execute_job() succeeds:
   - Log job.completed event with payload: {job_name, package_name, duration_seconds}
5. In exception handler:
   - Log job.failed event with status="failed", error_message (human summary), payload: {job_name, package_name, error_type}

All job events use object_type="job_run" and ONLY log_event() (no upsert_objects).

Note: error_message contains human-readable summary, payload contains machine-parsable fields (error_type, etc.).

**Commands:**

```bash
# Run linter
ruff check lorchestra/cli.py

# Run CLI tests
pytest tests/test_cli_jobs.py -v
```

**Validation:**

Before proceeding, verify:
- ✓ job.started event emitted before execution
- ✓ job.completed event emitted on success
- ✓ job.failed event emitted on exception
- ✓ All events use object_type="job_run"
- ✓ Tests pass
- ✓ No linting errors

**Outputs:**

- `lorchestra/cli.py` (updated)
- `tests/test_cli_jobs.py` (updated with job event tests)

---

### Step 5: Integration Testing

**Prompt:**

Run end-to-end integration test to verify the refactored system:

1. Run a Gmail job: `lorchestra run gmail_ingest_acct1 --since "2025-11-20"`
2. Query BigQuery to verify:
   - event_log has job.started, ingestion.completed, job.completed events
   - event_log rows have small telemetry payloads (NOT full objects)
   - raw_objects has full email payloads
   - event_log.idem_key is NULL for telemetry events
   - **NO per-object events were written** (critical assertion)

**Commands:**

```bash
# Set environment
source .venv/bin/activate
cd /workspace/lorchestra
set -a && source .env && set +a

# Run Gmail job
lorchestra run gmail_ingest_acct1 --since "2025-11-20"

# Save run_id for queries (extract from CLI output or generate)
RUN_ID="gmail_ingest_acct1-20251124..."

# Query event_log for job events
bq query --use_legacy_sql=false "
SELECT event_type, source_system, object_type, created_at,
       payload
FROM events_dev.event_log
WHERE object_type = 'job_run'
ORDER BY created_at DESC
LIMIT 10
"

# Query event_log for ingestion events
bq query --use_legacy_sql=false "
SELECT event_type, source_system, object_type, created_at,
       JSON_EXTRACT_SCALAR(payload, '$.records_extracted') as count
FROM events_dev.event_log
WHERE object_type = 'email' AND event_type LIKE 'ingestion.%'
ORDER BY created_at DESC
LIMIT 5
"

# CRITICAL: Verify NO per-object events were written
bq query --use_legacy_sql=false --parameter=run_id:STRING:$RUN_ID "
SELECT COUNT(*) as per_object_events
FROM events_dev.event_log
WHERE correlation_id = @run_id
  AND event_type NOT IN ('job.started','ingestion.completed','job.completed','job.failed')
"
# Should return 0

# Verify raw_objects has emails
bq query --use_legacy_sql=false "
SELECT object_type, source_system, COUNT(*) as count
FROM events_dev.raw_objects
WHERE object_type = 'email'
GROUP BY object_type, source_system
"

# Idempotency smoke test: run same job twice, verify count unchanged
BEFORE_COUNT=$(bq query --use_legacy_sql=false --format=csv "SELECT COUNT(*) FROM events_dev.raw_objects WHERE object_type='email' AND source_system='tap-gmail--acct1-personal'" | tail -n 1)

lorchestra run gmail_ingest_acct1 --since "2025-11-20"

AFTER_COUNT=$(bq query --use_legacy_sql=false --format=csv "SELECT COUNT(*) FROM events_dev.raw_objects WHERE object_type='email' AND source_system='tap-gmail--acct1-personal'" | tail -n 1)

echo "Before: $BEFORE_COUNT, After: $AFTER_COUNT (should be equal)"

# Verify last_seen advanced
bq query --use_legacy_sql=false "
SELECT MAX(last_seen) as latest_last_seen
FROM events_dev.raw_objects
WHERE object_type='email' AND source_system='tap-gmail--acct1-personal'
"

# Run full test suite
pytest tests/ -v
```

**Validation:**

Before proceeding, verify:
- ✓ Gmail job completes successfully
- ✓ event_log has telemetry events (not full objects)
- ✓ raw_objects has full email objects
- ✓ Job events visible in event_log
- ✓ idem_key is NULL for telemetry events
- ✓ **per_object_events query returns 0** (critical)
- ✓ **Idempotency: BEFORE_COUNT == AFTER_COUNT** (critical)
- ✓ **last_seen advanced on rerun**
- ✓ All tests pass

**Outputs:**

- Integration test results
- BigQuery query outputs showing correct data
- Screenshot/log showing per_object_events = 0

---

## Orchestrator

**State Machine:** Standard (pending → running → awaiting_human → succeeded/failed)

**Tools:** bash, pytest, ruff, bq (BigQuery CLI)

**Models:** (to be filled by defaults)

## Repository

**Branch:** `feat/event-client-refactor`

**Merge Strategy:** squash

## Migration Path

1. **Phase 1** (This spec): Add new functions + update schema + remove emit()
2. **Phase 2** (This spec): Update Gmail job to use new pattern
3. **Phase 3** (This spec): Add job logging to CLI
4. **Phase 4** (Future): Update all other jobs (Exchange, Dataverse, Stripe, etc.)

## Benefits

✅ Clear separation of concerns (events vs. actions)
✅ Batch operations (100 emails = 1 MERGE instead of 100)
✅ Smaller event_log (telemetry only, not full objects)
✅ Simpler to reason about (each function does one thing)
✅ Enables pure telemetry events (job logging) without object storage
✅ trace_id supported for future cross-system correlation
✅ Job execution observability (track when jobs run, how long, failures)
✅ Flexible idem_key computation (no hard 'id' field requirement)
✅ Streaming/batching support (no full materialization required)

## Queries Enabled

**Latest job runs:**
```sql
SELECT *
FROM `events_dev.event_log`
WHERE object_type = 'job_run'
ORDER BY created_at DESC
```

**Job execution history:**
```sql
SELECT
  event_type,
  created_at,
  JSON_EXTRACT_SCALAR(payload, '$.job_name') as job_name,
  JSON_EXTRACT_SCALAR(payload, '$.duration_seconds') as duration
FROM `events_dev.event_log`
WHERE object_type = 'job_run'
ORDER BY created_at DESC
```

**Failed jobs:**
```sql
SELECT *
FROM `events_dev.event_log`
WHERE event_type = 'job.failed'
  AND DATE(created_at) >= CURRENT_DATE() - 7
```

**Latest object state (emails, contacts, etc):**
```sql
SELECT *
FROM `events_dev.raw_objects`
WHERE object_type = 'email'
ORDER BY last_seen DESC
```

**Verify no per-object events (should be 0):**
```sql
SELECT COUNT(*) as per_object_events
FROM `events_dev.event_log`
WHERE correlation_id = @run_id
  AND event_type NOT IN ('job.started','ingestion.completed','job.completed','job.failed')
```
