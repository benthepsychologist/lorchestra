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

Jobs should emit events to BigQuery for tracking:

```python
from lorchestra.stack_clients.event_client import emit

def my_job(bq_client, **kwargs):
    # Start event
    emit(
        bq_client=bq_client,
        event_type="job.started",
        source="my_package/my_job",
        data={"account": kwargs.get("account")}
    )

    try:
        # Job logic
        result = do_work()

        # Success event
        emit(
            bq_client=bq_client,
            event_type="job.completed",
            source="my_package/my_job",
            data={"records_processed": result.count}
        )
    except Exception as e:
        # Error event
        emit(
            bq_client=bq_client,
            event_type="job.failed",
            source="my_package/my_job",
            data={"error": str(e)}
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
from lorchestra.stack_clients.event_client import emit

logger = logging.getLogger(__name__)

def extract_source(bq_client, account=None, since=None, until=None):
    """Extract data from source."""
    logger.info(f"Starting extraction for account={account}")

    emit(
        bq_client=bq_client,
        event_type="extraction.started",
        source="my_ingester/extract_source",
        data={"account": account, "since": since, "until": until}
    )

    try:
        # Extraction logic
        records = fetch_records(account, since, until)
        save_records(records)

        emit(
            bq_client=bq_client,
            event_type="extraction.completed",
            source="my_ingester/extract_source",
            data={"records_count": len(records)}
        )

        logger.info(f"Extracted {len(records)} records")

    except Exception as e:
        emit(
            bq_client=bq_client,
            event_type="extraction.failed",
            source="my_ingester/extract_source",
            data={"error": str(e)}
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
