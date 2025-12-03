# Lorchestra Jobs Guide

## Overview

Lorchestra is a lightweight job orchestrator that discovers and runs jobs from installed packages via Python entrypoints. Jobs are self-contained units of work that emit events to BigQuery for tracking and observability.

## Architecture

### Job Discovery

Jobs are discovered via Python entrypoint groups:
- `lorchestra.jobs.<package_name>` - Jobs for a specific package

Example `pyproject.toml`:
```toml
[project.entry-points."lorchestra.jobs.ingester"]
extract_gmail = "my_package.jobs:extract_gmail"
extract_slack = "my_package.jobs:extract_slack"
```

### Job Interface

Jobs must follow this interface:

```python
def my_job(bq_client, **kwargs):
    """
    Job description.

    Args:
        bq_client: BigQuery client for event emission
        **kwargs: Optional args (account, since, until)
    """
    # Your job logic here
    pass
```

**Required parameter:**
- `bq_client` - BigQuery client instance (passed by lorchestra)

**Optional parameters:**
- `account` - Account identifier (e.g., "acct1-personal")
- `since` - Start time (ISO format or relative like "-7d")
- `until` - End time (ISO format)
- Any custom parameters your job needs

## Writing Jobs

### Basic Job Example

```python
# my_package/jobs.py
import logging

logger = logging.getLogger(__name__)

def extract_gmail(bq_client, account=None, since=None, until=None):
    """Extract emails from Gmail."""
    logger.info(f"Extracting Gmail for account={account}, since={since}, until={until}")

    # Your extraction logic here
    emails = fetch_emails(account, since, until)

    # Process emails
    for email in emails:
        process_email(email)

    logger.info(f"Extracted {len(emails)} emails")
```

### Registering Jobs

Add to your package's `pyproject.toml`:

```toml
[project.entry-points."lorchestra.jobs.my_package"]
extract_gmail = "my_package.jobs:extract_gmail"
extract_calendar = "my_package.jobs:extract_calendar"
```

### Event Emission

Jobs emit events to BigQuery for tracking and observability. The `event_log` table is the single source of truth for pipeline status.

#### Event Type Naming

Use full words for event types following this pattern:
- `{stage}.started` - Emitted before work begins
- `{stage}.completed` - Emitted after successful completion
- `{stage}.failed` - Emitted on failure

Standard stages: `ingestion`, `validation`, `canonization`, `finalization`

#### Status Values

Status is strictly `success` or `failed` (no synonyms like `ok`, `done`, `error`).

#### Standard Payload Fields

All `*.completed` events should include:
- `job_id` - The job identifier
- `object_type` - The data type being processed (e.g., "email", "message")
- `duration_seconds` - Time for this stage (not total job time)

**Ingestion events:**
```json
{
  "job_id": "ingest_gmail_acct1",
  "object_type": "email",
  "records_extracted": 123,
  "records_inserted": 100,
  "records_updated": 23,
  "window_since": "2025-01-15T00:00:00Z",
  "window_until": "2025-01-16T00:00:00Z",
  "duration_seconds": 4.21
}
```

**Validation events:**
```json
{
  "job_id": "validate_gmail_source",
  "object_type": "email",
  "records_checked": 500,
  "records_pass": 495,
  "records_fail": 5,
  "schema_ref": "iglu:raw/email_gmail/jsonschema/1-0-0",
  "duration_seconds": 2.33
}
```

**Canonization events:**
```json
{
  "job_id": "canonize_gmail_jmap",
  "object_type": "email",
  "records_processed": 495,
  "records_inserted": 400,
  "records_updated": 95,
  "schema_ref": "iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0",
  "duration_seconds": 5.67
}
```

#### Example Event Emission

```python
from lorchestra.stack_clients.event_client import log_event

def my_job(bq_client, **kwargs):
    # Started event (before work begins)
    log_event(
        event_type="ingestion.started",
        source_system="my_source",
        target_object_type="email",
        correlation_id=run_id,
        status="success",
        payload={"job_id": job_id},
    )

    try:
        # Job logic
        result = do_work()

        # Completed event
        log_event(
            event_type="ingestion.completed",
            source_system="my_source",
            target_object_type="email",
            correlation_id=run_id,
            status="success",
            payload={
                "job_id": job_id,
                "object_type": "email",
                "records_extracted": result.extracted,
                "records_inserted": result.inserted,
                "records_updated": result.updated,
                "duration_seconds": duration,
            },
        )
    except Exception as e:
        # Failed event
        log_event(
            event_type="ingestion.failed",
            source_system="my_source",
            target_object_type="email",
            correlation_id=run_id,
            status="failed",
            error_message=str(e),
            payload={
                "job_id": job_id,
                "object_type": "email",
                "error_type": type(e).__name__,
                "duration_seconds": duration,
            },
        )
        raise
```

## CLI Commands

### Run a Job

```bash
# Basic usage
lorchestra run-job <package> <job>

# With options
lorchestra run-job ingester extract_gmail --account acct1-personal
lorchestra run-job ingester extract_gmail --since 2024-01-01 --until 2024-01-31
lorchestra run-job ingester extract_gmail --account acct1 --since -7d
```

### List Jobs

```bash
# List all jobs
lorchestra jobs list

# List jobs for specific package
lorchestra jobs list ingester
```

Output:
```
ingester:
  extract_gmail
  extract_slack
canonizer:
  canonicalize_email
  canonicalize_message
```

### Show Job Details

```bash
lorchestra jobs show <package> <job>
```

Example:
```bash
$ lorchestra jobs show ingester extract_gmail
ingester/extract_gmail
Location: /path/to/my_package/jobs.py
Signature: (bq_client, account=None, since=None, until=None)

Extract emails from Gmail.
```

### Pipeline Stats

Get quick statistics from the event log:

```bash
# Show event counts by type
lorchestra stats

# Filter by event type pattern
lorchestra stats --type "ingestion.*"

# Limit results
lorchestra stats --limit 20
```

### Pipeline Queries

Run pre-defined SQL queries for pipeline observability:

```bash
# List available queries
lorchestra query --list

# Run a named query
lorchestra query pipeline-status
lorchestra query streams-health

# Output as JSON
lorchestra query pipeline-status --format json
```

Available queries:
- `pipeline-status` - Last activity per stream (30-day window)
- `streams-health` - Hours since last success per stream

### Ad-hoc SQL

Run read-only SQL queries against BigQuery:

```bash
# Interactive SQL
lorchestra sql "SELECT event_type, COUNT(*) FROM event_log GROUP BY 1"

# Output as JSON
lorchestra sql "SELECT * FROM event_log LIMIT 10" --format json

# From a file
lorchestra sql --file my-query.sql
```

Note: Only read-only queries (SELECT) are allowed. DML statements are rejected.

## Best Practices

### 1. Logging

Use structured logging for observability:

```python
import logging

logger = logging.getLogger(__name__)

def my_job(bq_client, **kwargs):
    logger.info(f"Job started: {kwargs}")
    # ... work ...
    logger.info(f"Job completed: {result}")
```

### 2. Error Handling

Jobs should handle errors gracefully and emit failure events:

```python
def my_job(bq_client, **kwargs):
    try:
        # Work
        result = do_work()
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        emit(bq_client, "job.failed", "my_package/my_job", {"error": str(e)})
        raise  # Re-raise for lorchestra to catch
```

### 3. Idempotency

Jobs should be idempotent when possible:

```python
def extract_data(bq_client, account=None, since=None, **kwargs):
    # Check if already processed
    if already_extracted(account, since):
        logger.info("Already extracted, skipping")
        return

    # Extract
    data = fetch_data(account, since)
    save_data(data)
```

### 4. Parameter Validation

Validate required parameters:

```python
def my_job(bq_client, account=None, **kwargs):
    if not account:
        raise ValueError("account parameter is required")

    # Continue with job
```

## Environment Variables

### BigQuery Event Emission

Required environment variables for event emission:

- `BQ_EVENTS_DATASET` - BigQuery dataset name (e.g., "events")
- `BQ_EVENTS_TABLE` - BigQuery table name (e.g., "job_events")

These are used by the `event_client` module for emitting job events.

## Examples

### Complete Job Package

**Directory structure:**
```
my_ingester/
├── pyproject.toml
├── my_ingester/
│   ├── __init__.py
│   └── jobs.py
```

**pyproject.toml:**
```toml
[project]
name = "my-ingester"
version = "0.1.0"
dependencies = [
    "lorchestra",
    "google-cloud-bigquery>=3.0",
]

[project.entry-points."lorchestra.jobs.my_ingester"]
extract_source = "my_ingester.jobs:extract_source"
```

**jobs.py:**
```python
import logging
import time
from lorchestra.stack_clients.event_client import log_event

logger = logging.getLogger(__name__)

def extract_source(bq_client, account=None, since=None, until=None):
    """Extract data from source."""
    job_id = f"extract_source_{account}"
    run_id = f"run-{time.time()}"

    logger.info(f"Starting extraction for account={account}")

    # Started event (before work begins)
    log_event(
        event_type="ingestion.started",
        source_system="my_source",
        target_object_type="record",
        correlation_id=run_id,
        status="success",
        payload={"job_id": job_id},
    )

    start_time = time.time()
    try:
        # Extraction logic
        records = fetch_records(account, since, until)
        result = save_records(records)

        duration = time.time() - start_time
        log_event(
            event_type="ingestion.completed",
            source_system="my_source",
            target_object_type="record",
            correlation_id=run_id,
            status="success",
            payload={
                "job_id": job_id,
                "object_type": "record",
                "records_extracted": len(records),
                "records_inserted": result.inserted,
                "records_updated": result.updated,
                "duration_seconds": round(duration, 2),
            },
        )

        logger.info(f"Extracted {len(records)} records")

    except Exception as e:
        duration = time.time() - start_time
        log_event(
            event_type="ingestion.failed",
            source_system="my_source",
            target_object_type="record",
            correlation_id=run_id,
            status="failed",
            error_message=str(e),
            payload={
                "job_id": job_id,
                "object_type": "record",
                "error_type": type(e).__name__,
                "duration_seconds": round(duration, 2),
            },
        )
        logger.error(f"Extraction failed: {e}", exc_info=True)
        raise
```

## Testing Jobs

### Unit Testing

```python
# tests/test_jobs.py
from unittest.mock import Mock
from my_ingester.jobs import extract_source

def test_extract_source():
    bq_client = Mock()

    extract_source(
        bq_client=bq_client,
        account="test-account",
        since="2024-01-01"
    )

    # Assert job behavior
    assert bq_client.insert_rows.called
```

### Integration Testing

```python
import pytest
from google.cloud import bigquery

@pytest.fixture
def bq_client():
    return bigquery.Client()

def test_extract_integration(bq_client):
    from my_ingester.jobs import extract_source

    # Run job
    extract_source(
        bq_client=bq_client,
        account="test",
        since="-1d"
    )

    # Verify results
    # ...
```

## Migration from Old Pipeline

If you're migrating from the old lorchestra pipeline architecture:

1. **Extract Stage** → Create jobs under `lorchestra.jobs.<package_name>`
2. **Canonize Stage** → Create canonizer jobs
3. **Index Stage** → Create indexer jobs
4. **Update imports** - Remove references to `lorchestra.stages.*`
5. **Update CLI calls** - Use `lorchestra run-job` instead of `lorchestra run`
6. **Add event emission** - Use `event_client.emit()` for observability
