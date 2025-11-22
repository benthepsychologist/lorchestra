---
version: "0.1"
tier: C
title: Clean ingestor → lorc → event_client pipeline
owner: benthepsychologist
goal: Implement clean three-layer ingestion architecture with event_log + raw_objects
labels: [ingestion, meltano, bigquery, jobs, architecture]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-11-19T21:40:35.577860+00:00
updated: 2025-11-19T22:30:00.000000+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/ingestion-to-bq"
---

# Clean Ingestor → Lorc → Event_Client Pipeline

## Objective

Implement a clean three-layer ingestion architecture where:
1. **ingestor** - Thin Meltano wrapper, returns JSONL files (NO event knowledge)
2. **lorc jobs** - Event boundary, reads JSONL and calls event_client
3. **event_client** - Writes to event_log + raw_objects (idempotent via idem_key)

This establishes the foundational pattern for all 15+ API ingestions in LifeOS.

## Acceptance Criteria

- [x] event_client implements two-table pattern (event_log + raw_objects) ✓
- [ ] ingestor package wraps Meltano with clean API (returns JSONL paths)
- [ ] lorc jobs call ingestor and emit events via event_client
- [ ] Custom targets deleted (target-event-emitter, target-jsonl-chunked)
- [ ] Standard target-jsonl used for all Meltano runs
- [ ] End-to-end test passes (Meltano → JSONL → events → BQ)
- [ ] Idempotency verified (re-run produces no duplicate objects)
- [ ] All tests passing

## Context

### Background

We have a Meltano project at `/workspace/ingestor` with 11+ taps (Gmail, Exchange, Dataverse, Stripe, etc.) and custom Singer targets (target-event-emitter, target-jsonl-chunked) that directly emit to BigQuery.

**Problem:** This violates separation of concerns:
- Meltano/Singer shouldn't know about events or BigQuery
- Event emission should happen at the orchestrator layer (lorc)
- We need clean boundaries for 15+ API sources

**Solution:** Three-layer architecture with clear contracts:

```
┌─────────────────────────────────────────────────┐
│  lorc (orchestrator + event boundary)          │
│  - Defines jobs (job_ingest_gmail, etc.)        │
│  - Calls ingestor to get JSONL                  │
│  - Reads JSONL, computes idem_key               │
│  - Calls event_client.emit()                    │
└──────────────────┬──────────────────────────────┘
                   │ (calls)
┌──────────────────▼──────────────────────────────┐
│  ingestor (thin Meltano wrapper)                │
│  - Runs: meltano run tap-x target-jsonl         │
│  - Returns: Path to JSONL file                  │
│  - NO event knowledge, NO BigQuery              │
└──────────────────┬──────────────────────────────┘
                   │ (raw JSONL)
                   ▼
          /tmp/phi-vault/jsonl-tmp/{run_id}.jsonl
                   │
                   │ (read by lorc)
                   ▼
┌─────────────────────────────────────────────────┐
│  event_client (event spine gateway)             │
│  - INSERT into event_log (audit trail)          │
│  - MERGE into raw_objects (dedup by idem_key)   │
│  - Lives in lorc/stack_clients/                 │
└─────────────────────────────────────────────────┘
```

### Architecture Contract

**Three distinct layers:**

1. **lorchestra (lorc)** - Orchestrator and event boundary
   - Owns job definitions
   - Calls domain packages (ingestor) to get raw data
   - Wraps raw data into events and passes to event_client
   - **ONLY place that calls event_client**

2. **ingestor package** - Meltano wrapper
   - Wraps `meltano run tap-x target-jsonl`
   - Returns iterator/path to raw JSONL records
   - **Does NOT emit events**
   - **Does NOT know about BigQuery**
   - Job: "give lorc raw JSON from external APIs"

3. **event_client** - Event spine gateway
   - Single gateway into BigQuery
   - Only used from lorc, never from ingestor
   - Writes to event_log (append-only audit)
   - Writes to raw_objects (MERGE/dedup by idem_key)

**Storage Model (BigQuery):**

```sql
-- event_log: append-only audit trail (NO payload)
CREATE TABLE event_log (
  event_id STRING,           -- UUID per emit() call
  event_type STRING,         -- "gmail.email.received"
  source_system STRING,      -- "tap-gmail--acct1-personal"
  object_type STRING,        -- "email", "contact", etc.
  idem_key STRING,           -- References raw_objects
  correlation_id STRING,     -- run_id for tracing
  created_at TIMESTAMP,
  status STRING,             -- "ok" | "error"
  error_message STRING
);

-- raw_objects: deduped object store (one row per idem_key)
CREATE TABLE raw_objects (
  idem_key STRING PRIMARY KEY,  -- Stable identity
  source_system STRING,
  object_type STRING,
  external_id STRING,           -- Gmail message_id, etc.
  payload JSON,                 -- Full raw data
  first_seen TIMESTAMP,
  last_seen TIMESTAMP
);
```

**Idempotency:**
- Same object ingested twice → one row in raw_objects (last_seen updated)
- Every ingestion creates new event_log row (audit trail)
- idem_key = stable identity based on content

### Constraints

- No custom Meltano targets (use standard target-jsonl only)
- All event emission must happen in lorc, never in ingestor
- ingestor must be domain-agnostic (just Meltano → JSONL)
- event_client is the single gateway to BigQuery

## Plan

### Step 1: Create ingestor Package (Meltano Wrapper) [G0: Plan Approval]

**Objective:** Build thin wrapper around Meltano that returns JSONL file paths

**Task 1.1: Create package structure**

Files to create:
- `/workspace/ingestor/ingestor/__init__.py`
- `/workspace/ingestor/ingestor/extractors.py`
- `/workspace/ingestor/pyproject.toml`

**Task 1.2: Implement extractors.py**

```python
"""Thin wrapper around Meltano for data extraction.

This module provides a clean Python API for running Meltano taps.
It knows NOTHING about events or BigQuery - just Meltano → JSONL.
"""

import subprocess
from pathlib import Path
from typing import Optional
import os

def extract_to_jsonl(
    tap_name: str,
    run_id: str,
    output_dir: str = "/tmp/phi-vault/jsonl-tmp"
) -> Path:
    """
    Run Meltano tap and return path to JSONL output file.

    This is a generic wrapper that works for any tap configured in meltano.yml.
    It uses the standard Singer target-jsonl to write records to a temp file.

    Args:
        tap_name: Tap name from meltano.yml (e.g., "tap-gmail--acct1-personal")
        run_id: Unique run identifier for this extraction
        output_dir: Directory for JSONL output (default: /tmp/phi-vault/jsonl-tmp)

    Returns:
        Path to JSONL file containing extracted records

    Raises:
        RuntimeError: If Meltano execution fails

    Example:
        >>> jsonl_path = extract_to_jsonl("tap-gmail--acct1-personal", "run-123")
        >>> with open(jsonl_path) as f:
        ...     for line in f:
        ...         record = json.loads(line)
        ...         # Process record
    """
    # Create output directory if needed
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # JSONL file path
    jsonl_file = output_path / f"{run_id}.jsonl"

    # Get ingestor directory (where meltano.yml lives)
    ingestor_dir = Path(__file__).parent.parent

    # Set environment for Meltano
    env = os.environ.copy()
    env["RUN_ID"] = run_id
    env["JSONL_DESTINATION_PATH"] = str(jsonl_file)

    # Run Meltano: tap → target-jsonl
    # NOTE: We use standard target-jsonl, NOT custom targets
    cmd = ["meltano", "run", tap_name, "target-jsonl"]

    result = subprocess.run(
        cmd,
        cwd=ingestor_dir,
        env=env,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Meltano extraction failed for {tap_name}: {result.stderr}"
        )

    if not jsonl_file.exists():
        raise RuntimeError(
            f"Expected JSONL file not created: {jsonl_file}"
        )

    return jsonl_file


def iter_jsonl_records(jsonl_path: Path):
    """
    Iterate over records in a JSONL file.

    Args:
        jsonl_path: Path to JSONL file

    Yields:
        Dict records from JSONL file
    """
    import json

    with open(jsonl_path) as f:
        for line in f:
            if line.strip():  # Skip empty lines
                yield json.loads(line)
```

**Task 1.3: Create pyproject.toml**

```toml
[project]
name = "ingestor"
version = "0.1.0"
description = "Thin Meltano wrapper for LifeOS data extraction"
requires-python = ">=3.11"
dependencies = []

# NO job entrypoints - ingestor doesn't expose jobs to lorc
# Jobs live in lorc and call ingestor functions
```

**Commands:**

```bash
cd /workspace/ingestor

# Test imports
python -c "from ingestor.extractors import extract_to_jsonl; print('✓ ingestor imports')"
```

**Outputs:**
- `ingestor/__init__.py` created
- `ingestor/extractors.py` created
- `pyproject.toml` created
- No import errors

---

### Step 2: Create lorc Ingestion Jobs [G1: Code Readiness]

**Objective:** Create jobs in lorc that use ingestor and event_client

**Task 2.1: Create jobs module structure**

Files to create:
- `/workspace/lorchestra/lorchestra/jobs/__init__.py`
- `/workspace/lorchestra/lorchestra/jobs/ingest_gmail.py`

**Task 2.2: Implement ingest_gmail.py**

```python
"""Gmail ingestion job for LifeOS.

This job:
1. Calls ingestor.extract_to_jsonl() to run Meltano
2. Reads JSONL records
3. Computes idem_key for each record
4. Calls event_client.emit() to write to event_log + raw_objects

This is the ONLY place event_client is called for Gmail ingestion.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


def job_ingest_gmail_acct1(bq_client):
    """
    Ingest Gmail messages from acct1-personal.

    Args:
        bq_client: BigQuery client for event emission

    This job demonstrates the clean three-layer pattern:
    - Calls ingestor (Meltano wrapper)
    - Reads JSONL
    - Emits events via event_client
    """
    # Import here to avoid circular dependencies
    from ingestor.extractors import extract_to_jsonl, iter_jsonl_records
    from lorchestra.stack_clients.event_client import emit, generate_idem_key

    # Configuration
    tap_name = "tap-gmail--acct1-personal"
    source_system = tap_name
    object_type = "email"
    run_id = f"gmail-acct1-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    logger.info(f"Starting Gmail ingestion: {tap_name}, run_id={run_id}")

    try:
        # Step 1: Get JSONL from ingestor (Meltano wrapper)
        jsonl_path = extract_to_jsonl(tap_name, run_id)
        logger.info(f"JSONL extracted to: {jsonl_path}")

        # Step 2: Read records and emit events
        record_count = 0
        for record in iter_jsonl_records(jsonl_path):
            # Compute stable idem_key
            idem_key = generate_idem_key(
                source=source_system,
                object_type=object_type,
                payload=record
            )

            # Emit event (writes to event_log + raw_objects)
            emit(
                event_type="gmail.email.received",
                payload=record,
                source=source_system,
                object_type=object_type,
                bq_client=bq_client,
                correlation_id=run_id
            )

            record_count += 1

        logger.info(
            f"Gmail ingestion complete: {record_count} records, run_id={run_id}"
        )

    except Exception as e:
        logger.error(f"Gmail ingestion failed: {e}", exc_info=True)
        raise


def job_ingest_gmail_acct2(bq_client):
    """Ingest Gmail messages from acct2-business1."""
    # Similar pattern, different tap_name
    pass


def job_ingest_gmail_acct3(bq_client):
    """Ingest Gmail messages from acct3-bfarmstrong."""
    # Similar pattern, different tap_name
    pass
```

**Task 2.3: Register jobs in lorchestra**

Update `/workspace/lorchestra/pyproject.toml`:

```toml
[project.entry-points."lorchestra.jobs.gmail"]
ingest_acct1 = "lorchestra.jobs.ingest_gmail:job_ingest_gmail_acct1"
ingest_acct2 = "lorchestra.jobs.ingest_gmail:job_ingest_gmail_acct2"
ingest_acct3 = "lorchestra.jobs.ingest_gmail:job_ingest_gmail_acct3"
```

**Commands:**

```bash
cd /workspace/lorchestra

# Verify job discovery
lorchestra jobs list gmail

# Should show:
# gmail:
#   ingest_acct1
#   ingest_acct2
#   ingest_acct3
```

**Outputs:**
- `lorchestra/jobs/__init__.py` created
- `lorchestra/jobs/ingest_gmail.py` created
- Jobs registered in pyproject.toml
- Jobs appear in `lorchestra jobs list`

---

### Step 3: Configure Meltano for Standard target-jsonl [G2: Pre-Release]

**Objective:** Configure Meltano to use standard target-jsonl instead of custom targets

**Task 3.1: Update meltano.yml**

Add target-jsonl configuration:

```yaml
loaders:
  - name: target-jsonl
    variant: andyh1203
    pip_url: target-jsonl
    config:
      destination_path: ${JSONL_DESTINATION_PATH}
      do_timestamp_file: false
```

**Task 3.2: Delete custom targets**

```bash
cd /workspace/ingestor

# Delete target-event-emitter (ENTIRE directory)
rm -rf targets/target-event-emitter

# Delete any other custom targets
rm -rf targets/target-jsonl-chunked  # If exists
```

**Commands:**

```bash
cd /workspace/ingestor

# Test Meltano with standard target
export JSONL_DESTINATION_PATH=/tmp/test.jsonl
meltano run tap-gmail--acct1-personal target-jsonl

# Verify JSONL created
ls -la /tmp/test.jsonl
```

**Outputs:**
- target-event-emitter directory deleted
- meltano.yml updated with target-jsonl
- Test run produces JSONL file

---

### Step 4: BigQuery Setup (Tables and Credentials) [G3: Pre-Release]

**Objective:** Create BigQuery tables and configure credentials for event storage

**Task 4.1: Create BigQuery Dataset**

```bash
# Create dataset for events
bq mk --dataset --location=US ${GCP_PROJECT}:events_dev

# Or for production
bq mk --dataset --location=US ${GCP_PROJECT}:events_prod
```

**Task 4.2: Create event_log table**

Create the event_log table (audit trail - no payload):

```sql
CREATE TABLE `events_dev.event_log` (
  event_id STRING NOT NULL,
  event_type STRING NOT NULL,
  source_system STRING NOT NULL,
  object_type STRING NOT NULL,
  idem_key STRING NOT NULL,
  correlation_id STRING,
  subject_id STRING,
  created_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  error_message STRING
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, object_type, event_type
OPTIONS(
  description="Event audit trail - one row per emit() call, no payload"
);
```

**Task 4.3: Create raw_objects table**

Create the raw_objects table (deduped object store):

```sql
CREATE TABLE `events_dev.raw_objects` (
  idem_key STRING NOT NULL,
  source_system STRING NOT NULL,
  object_type STRING NOT NULL,
  external_id STRING,
  payload JSON NOT NULL,
  first_seen TIMESTAMP NOT NULL,
  last_seen TIMESTAMP NOT NULL
)
CLUSTER BY source_system, object_type
OPTIONS(
  description="Deduped object store - one row per idem_key, with payload"
);

-- Add primary key constraint (enforced at query time, not insert time)
ALTER TABLE `events_dev.raw_objects`
ADD PRIMARY KEY (idem_key) NOT ENFORCED;
```

**Task 4.4: Configure Service Account Credentials**

1. Create service account (if not exists):
```bash
gcloud iam service-accounts create lorchestra-events \
  --display-name="lorchestra Event Emitter" \
  --project=${GCP_PROJECT}
```

2. Grant BigQuery permissions:
```bash
# Grant BigQuery Data Editor role
gcloud projects add-iam-policy-binding ${GCP_PROJECT} \
  --member="serviceAccount:lorchestra-events@${GCP_PROJECT}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

# Grant BigQuery Job User role
gcloud projects add-iam-policy-binding ${GCP_PROJECT} \
  --member="serviceAccount:lorchestra-events@${GCP_PROJECT}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
```

3. Create and download key:
```bash
gcloud iam service-accounts keys create ~/lorchestra-events-key.json \
  --iam-account=lorchestra-events@${GCP_PROJECT}.iam.gserviceaccount.com
```

4. Set environment variable:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/lorchestra-events-key.json
export EVENTS_BQ_DATASET=events_dev
export EVENT_LOG_TABLE=event_log
export RAW_OBJECTS_TABLE=raw_objects
```

**Task 4.5: Add to .env file**

Create or update `/workspace/lorchestra/.env`:

```bash
# BigQuery Configuration
GOOGLE_APPLICATION_CREDENTIALS=/path/to/lorchestra-events-key.json
EVENTS_BQ_DATASET=events_dev
EVENT_LOG_TABLE=event_log
RAW_OBJECTS_TABLE=raw_objects

# GCP Project
GCP_PROJECT=your-project-id
```

**Commands:**

```bash
# Verify tables exist
bq ls events_dev

# Verify schema
bq show --schema events_dev.event_log
bq show --schema events_dev.raw_objects

# Test credentials
python3 -c "from google.cloud import bigquery; client = bigquery.Client(); print('✓ BigQuery client initialized')"
```

**Outputs:**
- ✓ event_log table created in BigQuery
- ✓ raw_objects table created in BigQuery
- ✓ Service account created with proper permissions
- ✓ Credentials configured in environment
- ✓ .env file created with BigQuery settings

---

### Step 5: Testing & Validation [G4: Pre-Release]

**Objective:** Test end-to-end ingestion flow

**Task 5.1: Unit tests for ingestor**

Create `/workspace/ingestor/tests/test_extractors.py`:

```python
import pytest
from unittest.mock import Mock, patch
from ingestor.extractors import extract_to_jsonl

def test_extract_to_jsonl():
    """Test Meltano wrapper."""
    with patch('ingestor.extractors.subprocess.run') as mock_run:
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")

        # Mock file creation
        with patch('ingestor.extractors.Path.exists', return_value=True):
            result = extract_to_jsonl("tap-test", "run-123")

            assert mock_run.called
            cmd = mock_run.call_args[0][0]
            assert "meltano" in cmd
            assert "tap-test" in cmd
            assert "target-jsonl" in cmd
```

**Task 5.2: Integration test**

```bash
# Set up environment
export EVENTS_BQ_DATASET=events_test
export BQ_EVENTS_DATASET=events_test
export EVENT_LOG_TABLE=event_log
export RAW_OBJECTS_TABLE=raw_objects

# Run full ingestion
cd /workspace/lorchestra
lorchestra run-job gmail ingest_acct1

# Verify results
# 1. JSONL file created
ls -la /tmp/phi-vault/jsonl-tmp/

# 2. Events in event_log
bq query "SELECT COUNT(*) FROM events_test.event_log WHERE event_type='gmail.email.received'"

# 3. Objects in raw_objects
bq query "SELECT COUNT(*) FROM events_test.raw_objects WHERE object_type='email'"

# 4. Test idempotency (re-run)
lorchestra run-job gmail ingest_acct1

# 5. Verify no duplicate objects (only last_seen updated)
bq query "SELECT COUNT(*) FROM events_test.raw_objects WHERE object_type='email'"
# Should be same count as before
```

**Task 5.3: Verify clean boundaries**

```bash
# Verify ingestor has NO event imports
cd /workspace/ingestor
grep -r "event_client" ingestor/
# Should return nothing

# Verify event_client only called from lorc
cd /workspace/lorchestra
grep -r "event_client.emit" lorchestra/
# Should only show calls in lorchestra/jobs/
```

**Outputs:**
- Unit tests passing
- Integration test produces events in BQ
- Idempotency verified (re-run safe)
- Clean boundaries confirmed

---

### Step 6: Documentation & Finalization [G5: Final Approval]

**Task 6.1: Update architecture docs**

Update `/workspace/lorchestra/ARCH-GOAL-MINIMAL-EVENT-PIPELINE.md`:

- Replace references to "raw_events table" with "event_log + raw_objects"
- Update Section 3.2 (event_client) to describe two-table pattern
- Update Section 4.1 (Raw Event Emission) to show ingestor → lorc → event_client flow
- Update Section 5.1 (Storage Strategy) with event_log + raw_objects tables

**Task 6.2: Update ingestor README**

Update `/workspace/ingestor/README.md`:

```markdown
## Architecture

ingestor is a thin wrapper around Meltano that provides a clean Python API
for data extraction. It knows NOTHING about events or BigQuery.

### Usage

```python
from ingestor.extractors import extract_to_jsonl, iter_jsonl_records

# Extract data
jsonl_path = extract_to_jsonl("tap-gmail--acct1-personal", run_id="run-123")

# Iterate records
for record in iter_jsonl_records(jsonl_path):
    # Process record (e.g., emit events from lorc)
    pass
```

### Boundary Contract

ingestor responsibilities:
- Run Meltano taps via subprocess
- Write to JSONL using standard target-jsonl
- Return file paths or record iterators

ingestor does NOT:
- Emit events or call event_client
- Know about BigQuery tables
- Handle deduplication or idempotency

Those concerns belong in the orchestrator layer (lorc).
```

**Task 6.3: Commit changes**

```bash
cd /workspace/lorchestra
git add -A
git commit -m "feat: implement clean ingestor → lorc → event_client pipeline

Establishes three-layer architecture for ingestion:

1. ingestor package - Thin Meltano wrapper
   - Wraps meltano run commands
   - Returns JSONL file paths
   - NO event knowledge

2. lorc jobs - Event boundary
   - Calls ingestor.extract_to_jsonl()
   - Reads JSONL records
   - Computes idem_key
   - Calls event_client.emit()

3. event_client - BigQuery gateway
   - Writes to event_log (audit trail)
   - Writes to raw_objects (dedup by idem_key)

Changes:
- Created ingestor package with extractors.py
- Created lorc/jobs/ingest_gmail.py
- Updated event_client to two-table pattern (event_log + raw_objects)
- Deleted custom targets (target-event-emitter)
- Configured Meltano to use standard target-jsonl
- Added comprehensive tests

Benefits:
- Clean separation of concerns
- Idempotent ingestion via idem_key
- Single event boundary (lorc only)
- Scalable to 15+ API sources

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
"
```

**Outputs:**
- ARCH-GOAL updated with event_log + raw_objects
- ingestor README updated with boundary contract
- Changes committed

---

## Success Criteria

✅ Clean three-layer architecture implemented
✅ ingestor has NO event_client imports (grep verification passes)
✅ lorc jobs are the ONLY place calling event_client
✅ Custom targets deleted (target-event-emitter, etc.)
✅ Standard target-jsonl used for all extractions
✅ event_log + raw_objects tables written correctly
✅ Idempotency works (re-run produces no duplicate objects)
✅ End-to-end test passes (Meltano → JSONL → events → BQ)
✅ All tests passing
✅ Documentation updated

## Next Steps

After this spec is complete:

1. **Replicate pattern for other taps** (Exchange, Dataverse, Stripe)
2. **Create canonization jobs** (read raw_objects, emit canonical events)
3. **Set up job scheduling** (Cloud Scheduler or cron)
4. **Add monitoring** (query event_log for job status)

## Repository

**Branch:** `feat/ingestion-to-bq`
**Merge Strategy:** squash
**Target:** `main`
