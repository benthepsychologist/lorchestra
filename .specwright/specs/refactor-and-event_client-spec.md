---
version: "0.1"
tier: C
title: Minimal event_client Implementation
owner: benthepsychologist
goal: Create minimal event_client.emit() that writes event envelopes to BigQuery
labels: [event-client, bigquery, minimal-viable]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-11-18T17:28:20.603189+00:00
updated: 2025-11-18T20:45:00.000000+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/minimal-event-client"
---

# Minimal event_client Implementation

## Objective

Create a minimal `event_client` INSIDE the lorchestra repo with a single responsibility: write correctly-shaped event envelope rows into BigQuery.

**Key principle:** No schema discovery, no policy engine, no auto-gov integration. Callers are responsible for choosing `event_type`, `payload`, and (optionally) `schema_ref` and metadata. `event_client`'s ONLY job is to normalize into an envelope and write it to the configured BQ table.

## Acceptance Criteria

- [ ] `lorc.stack_clients.event_client` module created with `emit()` function
- [ ] `emit()` writes event envelopes to BigQuery events table
- [ ] Envelope shape includes: event_id, event_type, source, schema_ref, created_at, correlation_id, subject_id, payload
- [ ] Environment variables EVENTS_BQ_DATASET and EVENTS_BQ_TABLE are used for table config
- [ ] All tests pass (pytest)
- [ ] Code passes linting (ruff check)
- [ ] 80%+ test coverage for event_client module
- [ ] No JSONL backup (deferred to future work)
- [ ] No existing jobs refactored (only creates new event_client)

## Context

### Background

Per ARCH-GOAL-MINIMAL-EVENT-PIPELINE.md, lorchestra needs an `event_client` to write events to BigQuery as the primary WAL (write-ahead log). This is the foundation for the three working lanes:

1. Sales pipeline via email
2. Measurement pipeline via questionnaires
3. Automated reporting

For v0, `event_client.emit()` does ONE thing: take an envelope-shaped dict and write it to the events table in BigQuery. Full stop. All schema validation, policy enforcement, and cleverness lives elsewhere or later.

### Constraints

- No schema lookup or validation (callers provide schema_ref as optional string)
- No policy engine integration
- No JSONL backup (will be added later)
- No auto-gov integration
- Must use google-cloud-bigquery client (passed in by caller)
- Environment-based configuration only (EVENTS_BQ_DATASET, EVENTS_BQ_TABLE)
- No existing job refactoring in this task (event_client only)

## Plan

### Step 1: File Structure & Envelope Design

**What to build:**

Create new internal package structure:
```
lorchestra/
├── stack_clients/
│   ├── __init__.py
│   └── event_client.py
└── ...

tests/
├── test_event_client.py
└── ...
```

**Envelope shape (exact keys):**
```python
{
    "event_id": str,           # UUID4
    "event_type": str,         # e.g. "email.received", "questionnaire_response"
    "source": str,             # e.g. "gmail_ingester", "canonizer_email"
    "schema_ref": Optional[str],  # e.g. "email.v1", None
    "created_at": str,         # UTC ISO 8601 timestamp
    "correlation_id": Optional[str],  # For tracing related events
    "subject_id": Optional[str],      # PHI subject identifier
    "payload": Dict[str, Any]  # Event-specific data
}
```

**Files to create:**
1. `lorchestra/stack_clients/__init__.py`
2. `lorchestra/stack_clients/event_client.py`
3. `tests/test_event_client.py`

**Files to touch:** 3 total (all new, no modifications to existing files)

### Step 2: Implementation Details

#### 2.1 Envelope Builder (`event_client.py`)

Implement `build_envelope()` helper:

```python
from typing import Any, Dict, Optional
from datetime import datetime, timezone
import uuid

def build_envelope(
    *,
    event_type: str,
    payload: Dict[str, Any],
    source: str,
    schema_ref: Optional[str] = None,
    correlation_id: Optional[str] = None,
    subject_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a standard event envelope.

    Args:
        event_type: Type of event (e.g. "email.received")
        payload: Event-specific data
        source: Source system/component (e.g. "gmail_ingester")
        schema_ref: Optional schema reference (e.g. "email.v1")
        correlation_id: Optional correlation ID for tracing
        subject_id: Optional subject identifier (PHI)

    Returns:
        Event envelope dict with all required fields

    Raises:
        ValueError: If event_type or source are empty
    """
```

**Behavior:**
- Validate `event_type` and `source` are non-empty strings
- Generate `event_id` as `str(uuid.uuid4())`
- Generate `created_at` as `datetime.now(timezone.utc).isoformat()`
- Return dict with all envelope fields

#### 2.2 BigQuery Configuration (`event_client.py`)

Implement environment-based table reference:

```python
import os

def _get_bq_table_ref(bq_client) -> str:
    """
    Get fully-qualified BigQuery table reference from environment.

    Reads EVENTS_BQ_DATASET and EVENTS_BQ_TABLE environment variables.

    Args:
        bq_client: BigQuery client (used for project context if needed)

    Returns:
        Table reference string: "dataset.table"

    Raises:
        RuntimeError: If required environment variables are missing
    """
    dataset = os.environ.get("EVENTS_BQ_DATASET")
    table = os.environ.get("EVENTS_BQ_TABLE")

    if not dataset or not table:
        raise RuntimeError(
            "Missing required environment variables: "
            "EVENTS_BQ_DATASET and/or EVENTS_BQ_TABLE"
        )

    return f"{dataset}.{table}"
```

#### 2.3 BigQuery Write (`event_client.py`)

Implement single-row insert:

```python
def _write_to_bq(envelope: Dict[str, Any], bq_client) -> None:
    """
    Write event envelope to BigQuery events table.

    Args:
        envelope: Event envelope dict from build_envelope()
        bq_client: google.cloud.bigquery.Client instance

    Raises:
        RuntimeError: If BigQuery write fails
    """
    table_ref = _get_bq_table_ref(bq_client)

    # Use insert_rows_json for single row insert
    errors = bq_client.insert_rows_json(table_ref, [envelope])

    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")
```

**Assumed BQ table schema:**
```sql
CREATE TABLE `project.dataset.events` (
    event_id STRING NOT NULL,
    event_type STRING NOT NULL,
    source STRING NOT NULL,
    schema_ref STRING,
    created_at STRING NOT NULL,
    correlation_id STRING,
    subject_id STRING,
    payload JSON NOT NULL
)
PARTITION BY DATE(_PARTITIONTIME)
CLUSTER BY event_type, source;
```

**Critical design decisions:**

1. **`created_at` is STRING (ISO 8601 UTC) for v0.**
   - Stored as: `"2025-11-18T20:45:00.123456+00:00"`
   - Rationale: No timezone conversion bugs, easy string comparison in queries, can migrate to TIMESTAMP later
   - Migration path: Future ALTER TABLE to change type and add proper partitioning by `DATE(created_at)`

2. **`payload` MUST be JSON column, NOT STRING.**
   - Pass the payload dict directly to `insert_rows_json()`
   - Let the BigQuery client handle JSON serialization
   - **DO NOT** call `json.dumps()` on the payload before inserting
   - Rationale: BQ JSON type allows native querying with JSON functions

3. **Partitioning uses `_PARTITIONTIME` (ingestion time) for now.**
   - Simple, works immediately
   - Future: Migrate to `PARTITION BY DATE(created_at)` when we convert to TIMESTAMP

#### 2.4 Public API (`event_client.py`)

Implement the single public function:

```python
def emit(
    event_type: str,
    payload: Dict[str, Any],
    *,
    source: str,
    bq_client,
    schema_ref: Optional[str] = None,
    correlation_id: Optional[str] = None,
    subject_id: Optional[str] = None,
) -> None:
    """
    Emit an event to BigQuery.

    This is the primary interface for writing events to the event store.
    Callers are responsible for choosing appropriate event_type and payload.

    Args:
        event_type: Type of event (e.g. "email.received")
        payload: Event-specific data as a dict
        source: Source system/component (e.g. "gmail_ingester")
        bq_client: google.cloud.bigquery.Client instance
        schema_ref: Optional schema reference (e.g. "email.v1")
        correlation_id: Optional correlation ID for tracing
        subject_id: Optional subject identifier (PHI)

    Raises:
        ValueError: If event_type or source are invalid
        RuntimeError: If BigQuery write fails or env vars missing

    Example:
        >>> from google.cloud import bigquery
        >>> client = bigquery.Client()
        >>> emit(
        ...     event_type="email.received",
        ...     payload={"subject": "Hello", "from": "test@example.com"},
        ...     source="gmail_ingester",
        ...     bq_client=client
        ... )
    """
    envelope = build_envelope(
        event_type=event_type,
        payload=payload,
        source=source,
        schema_ref=schema_ref,
        correlation_id=correlation_id,
        subject_id=subject_id,
    )

    _write_to_bq(envelope, bq_client)
```

#### 2.5 Module Docstring (`event_client.py`)

Add clear module-level documentation:

```python
"""
Minimal event client for writing events to BigQuery.

This module provides a single public function, emit(), which writes
event envelopes to a BigQuery events table.

Key principles:
- Callers choose event_type and payload
- event_client does NOT know about domain-specific schemas
- schema_ref is optional and purely informational
- No schema validation, policy enforcement, or auto-gov integration
- BigQuery is the only storage target (no JSONL backup in v0)

Configuration via environment variables:
- EVENTS_BQ_DATASET: BigQuery dataset name
- EVENTS_BQ_TABLE: BigQuery table name

Envelope format:
- event_id: UUID4 string
- event_type: Caller-provided (e.g., "email.received")
- source: Caller-provided (e.g., "ingester/gmail/acct1")
- schema_ref: Optional schema reference
- created_at: ISO 8601 UTC string (e.g., "2025-11-18T20:45:00.123456+00:00")
- correlation_id: Optional correlation ID for tracing
- subject_id: Optional subject identifier (PHI)
- payload: JSON object (dict)

Important:
- Pass payload as a dict, NOT a JSON string
- created_at is stored as STRING in BQ, not TIMESTAMP (for v0 simplicity)
- payload must be a JSON column in BQ, not STRING

Usage:
    from google.cloud import bigquery
    from lorchestra.stack_clients.event_client import emit

    client = bigquery.Client()
    emit(
        event_type="email.received",
        payload={"subject": "Test", "from": "user@example.com"},
        source="ingester/gmail/acct1-personal",
        bq_client=client
    )
"""
```

### Step 3: Test Implementation (`tests/test_event_client.py`)

**Test coverage requirements:**
- 80%+ coverage for event_client module
- Focus on critical paths and error handling
- Use mocks for BQ client and environment variables

**Required test coverage:**

#### 3.1 Envelope Building
- build_envelope with required fields only
- build_envelope with all optional fields
- Validation errors for missing/empty event_type or source
- event_id is a valid UUID (simple check: `uuid.UUID(envelope['event_id'])`)
- created_at is valid ISO 8601 (simple check: `datetime.fromisoformat(envelope['created_at'])`)

#### 3.2 BigQuery Integration
- emit() calls insert_rows_json with correct envelope
- Missing EVENTS_BQ_DATASET raises RuntimeError
- Missing EVENTS_BQ_TABLE raises RuntimeError
- BQ insert errors are raised as RuntimeError

#### 3.3 Test Fixtures

```python
import pytest
from unittest.mock import MagicMock
from datetime import datetime
import uuid

@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client."""
    client = MagicMock()
    client.insert_rows_json.return_value = []  # No errors
    return client

@pytest.fixture
def env_vars(monkeypatch):
    """Set required environment variables."""
    monkeypatch.setenv("EVENTS_BQ_DATASET", "test_dataset")
    monkeypatch.setenv("EVENTS_BQ_TABLE", "test_table")

def test_build_envelope_basic():
    """Test envelope creation with required fields."""
    from lorchestra.stack_clients.event_client import build_envelope

    envelope = build_envelope(
        event_type="test.event",
        payload={"key": "value"},
        source="test_source"
    )

    assert envelope["event_type"] == "test.event"
    assert envelope["source"] == "test_source"
    assert envelope["payload"] == {"key": "value"}
    assert uuid.UUID(envelope["event_id"])  # Valid UUID
    assert datetime.fromisoformat(envelope["created_at"])  # Valid ISO 8601

def test_emit_calls_insert(mock_bq_client, env_vars):
    """Test emit() calls BQ insert_rows_json."""
    from lorchestra.stack_clients.event_client import emit

    emit(
        event_type="test.event",
        payload={"key": "value"},
        source="test_source",
        bq_client=mock_bq_client
    )

    mock_bq_client.insert_rows_json.assert_called_once()
    args = mock_bq_client.insert_rows_json.call_args
    assert args[0][0] == "test_dataset.test_table"
    assert len(args[0][1]) == 1  # One row
    assert args[0][1][0]["event_type"] == "test.event"

# ... additional tests for error cases
```

**Note:** Don't over-specify test names. Focus on coverage requirements and critical paths.

### Step 4: Validation & Testing

**Commands to run:**

```bash
# Install dependencies (if needed)
pip install google-cloud-bigquery pytest

# Lint check
ruff check lorchestra/stack_clients/

# Run tests with coverage
pytest tests/test_event_client.py -v --cov=lorchestra.stack_clients.event_client --cov-report=term-missing

# Ensure 80%+ coverage
pytest tests/test_event_client.py --cov=lorchestra.stack_clients.event_client --cov-fail-under=80
```

**Expected output:**
- All tests pass
- Coverage >= 80%
- No linting errors
- No type errors (if using mypy)

### Step 5: Integration Verification (Manual)

Optional manual verification that the structure is correct:

```python
# In a Python REPL or test script
from lorchestra.stack_clients.event_client import emit, build_envelope
from google.cloud import bigquery
import os

# Set env vars
os.environ["EVENTS_BQ_DATASET"] = "test_dataset"
os.environ["EVENTS_BQ_TABLE"] = "test_table"

# Build an envelope (no BQ call)
envelope = build_envelope(
    event_type="test.event",
    payload={"message": "Hello, world!"},
    source="manual_test"
)

print(envelope)
# Should show all required fields

# To actually test BQ write (requires real BQ credentials):
# client = bigquery.Client()
# emit(
#     event_type="test.event",
#     payload={"message": "Hello, world!"},
#     source="manual_test",
#     bq_client=client
# )
```

## Models & Tools

**Tools:**
- pytest (testing)
- ruff (linting)
- google-cloud-bigquery (BQ client library)
- pytest-cov (coverage reporting)

**Models:**
- Primary: claude-sonnet-4-5 (implementation)
- Fast tasks: claude-haiku (if applicable)

## Repository

**Branch:** `feat/minimal-event-client`

**Merge Strategy:** squash

**Protected Paths:** None (all new files)

## Decision Log

### Why pass bq_client as parameter instead of creating it in emit()?

**Decision:** Require caller to pass `bq_client` as a parameter.

**Rationale:**
- Allows caller to control client lifecycle (connection pooling, credentials)
- Makes testing easier (can mock the client)
- Avoids hidden global state or singletons
- Caller may have specific client configuration (timeouts, retry logic)

**Trade-off:** Slight inconvenience for callers, but much better for testability and flexibility.

### Why environment variables for table config instead of function parameters?

**Decision:** Use `EVENTS_BQ_DATASET` and `EVENTS_BQ_TABLE` environment variables.

**Rationale:**
- Configuration should be environment-specific, not passed at every call site
- Prevents accidentally writing to wrong table
- Follows 12-factor app principles
- Easy to override in different environments (dev, staging, prod)

**Trade-off:** Requires environment setup, but this is standard practice.

### Why no JSONL backup in v0?

**Decision:** Defer JSONL backup to future work.

**Rationale:**
- Minimizes initial scope and complexity
- BigQuery is the primary WAL; JSONL is secondary backup
- Can be added incrementally without breaking changes
- Focus on getting BQ write working first

**Trade-off:** No backup mechanism initially, but acceptable for v0.

### Why no schema validation in emit()?

**Decision:** No schema validation; callers responsible for correct payload.

**Rationale:**
- Keeps event_client minimal and focused
- Schema validation is a separate concern (auto-gov, future)
- Allows flexibility in early development
- Can add validation later without breaking API (just stricter)

**Trade-off:** Potential for invalid events in BQ, but acceptable trade-off for v0 simplicity.