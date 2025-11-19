# Manual Verification Guide: event_client

This guide provides instructions for manually testing the event_client with a real BigQuery instance.

## Prerequisites

1. Google Cloud project with BigQuery enabled
2. BigQuery dataset and events table created
3. Application default credentials configured

## Create BQ Table

```sql
CREATE TABLE `your-project.your-dataset.events` (
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

## Set Environment Variables

```bash
export EVENTS_BQ_DATASET=your_dataset
export EVENTS_BQ_TABLE=events
```

## Test 1: Basic Event Emission

```python
from google.cloud import bigquery
from lorchestra.stack_clients.event_client import emit

# Create BQ client
client = bigquery.Client()

# Emit a test event
emit(
    event_type="test.event",
    payload={"message": "Hello, world!", "count": 1},
    source="manual_test",
    bq_client=client
)

print("✓ Event emitted successfully")
```

## Test 2: Event with All Fields

```python
from google.cloud import bigquery
from lorchestra.stack_clients.event_client import emit

client = bigquery.Client()

emit(
    event_type="email.received",
    payload={
        "subject": "Test Email",
        "from": "test@example.com",
        "to": "recipient@example.com",
        "body": "This is a test message"
    },
    source="ingester/gmail/test-account",
    bq_client=client,
    schema_ref="email.v1",
    correlation_id="test-corr-123",
    subject_id="test-subject-456"
)

print("✓ Full event emitted successfully")
```

## Test 3: Query Events in BigQuery

```sql
-- View recent events
SELECT
    event_id,
    event_type,
    source,
    created_at,
    payload
FROM `your-project.your-dataset.events`
ORDER BY created_at DESC
LIMIT 10;
```

## Test 4: Verify Envelope Shape

```python
from lorchestra.stack_clients.event_client import build_envelope

envelope = build_envelope(
    event_type="test.event",
    payload={"test": "data"},
    source="test_source"
)

# Verify all required fields
required_fields = [
    'event_id', 'event_type', 'source', 'schema_ref',
    'created_at', 'correlation_id', 'subject_id', 'payload'
]

for field in required_fields:
    assert field in envelope, f"Missing field: {field}"

print("✓ Envelope shape verified")
print(f"  event_id: {envelope['event_id']}")
print(f"  event_type: {envelope['event_type']}")
print(f"  created_at: {envelope['created_at']}")
```

## Test 5: Error Handling

### Missing Environment Variables

```python
import os
from google.cloud import bigquery
from lorchestra.stack_clients.event_client import emit

# Remove env vars
os.environ.pop('EVENTS_BQ_DATASET', None)

client = bigquery.Client()

try:
    emit(
        event_type="test",
        payload={},
        source="test",
        bq_client=client
    )
    print("✗ Should have raised RuntimeError")
except RuntimeError as e:
    print(f"✓ Correctly raised error: {e}")
```

### Invalid Event Type

```python
from lorchestra.stack_clients.event_client import build_envelope

try:
    envelope = build_envelope(
        event_type="",
        payload={},
        source="test"
    )
    print("✗ Should have raised ValueError")
except ValueError as e:
    print(f"✓ Correctly raised error: {e}")
```

## Expected Results

After running all tests:

1. ✅ Events appear in BigQuery table
2. ✅ Envelope fields are correctly populated
3. ✅ Timestamps are in ISO 8601 format
4. ✅ Payload is stored as JSON (queryable with JSON functions)
5. ✅ Error handling works as expected

## Verification Checklist

- [ ] BQ table created with correct schema
- [ ] Environment variables set
- [ ] Basic event emission works
- [ ] Full event with all fields works
- [ ] Events are queryable in BQ console
- [ ] Envelope shape is correct
- [ ] Error handling works (missing env vars, invalid fields)
- [ ] Payload is JSON type (not STRING)
- [ ] created_at is STRING type with ISO 8601 format

## Next Steps

Once manual verification is complete:

1. Document any issues found
2. Test with real ingester jobs (when available)
3. Monitor BQ costs and partition performance
4. Consider adding JSONL backup (future work)
