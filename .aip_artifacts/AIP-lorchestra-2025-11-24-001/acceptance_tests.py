#!/usr/bin/env python
"""
Acceptance tests for refactored event client.

These tests verify the system works as expected from a user perspective.
"""

import os
from google.cloud import bigquery
from datetime import datetime, timedelta
import json

def test_1_job_lifecycle_tracking():
    """Test that job lifecycle events are tracked correctly."""
    print("\n" + "="*80)
    print("TEST 1: Job Lifecycle Tracking")
    print("="*80)

    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    # Get most recent job run
    query = f"""
        SELECT
            correlation_id,
            MIN(created_at) as job_start,
            MAX(created_at) as job_end,
            TIMESTAMP_DIFF(MAX(created_at), MIN(created_at), SECOND) as duration_seconds,
            ARRAY_AGG(event_type ORDER BY created_at) as event_sequence
        FROM `{dataset}.event_log`
        WHERE object_type = 'job_run'
          AND correlation_id LIKE 'gmail_ingest_acct1-%'
        GROUP BY correlation_id
        ORDER BY job_start DESC
        LIMIT 1
    """

    result = list(client.query(query).result())
    if not result:
        print("❌ No job runs found")
        return False

    run = result[0]
    print(f"✓ Found job run: {run.correlation_id}")
    print(f"  Started: {run.job_start}")
    print(f"  Ended: {run.job_end}")
    print(f"  Duration: {run.duration_seconds}s")
    print(f"  Event sequence: {run.event_sequence}")

    expected_sequence = ['job.started', 'job.completed']
    if run.event_sequence == expected_sequence:
        print(f"✓ Event sequence is correct")
        return True
    else:
        print(f"❌ Expected {expected_sequence}, got {run.event_sequence}")
        return False


def test_2_telemetry_payload_structure():
    """Test that telemetry payloads have the right structure."""
    print("\n" + "="*80)
    print("TEST 2: Telemetry Payload Structure")
    print("="*80)

    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    # Check job.started payload
    query = f"""
        SELECT payload
        FROM `{dataset}.event_log`
        WHERE event_type = 'job.started'
          AND correlation_id LIKE 'gmail_ingest_acct1-%'
        ORDER BY created_at DESC
        LIMIT 1
    """

    result = list(client.query(query).result())
    if not result:
        print("❌ No job.started events found")
        return False

    # BigQuery returns JSON fields as dicts, not strings
    payload = result[0].payload if isinstance(result[0].payload, dict) else json.loads(result[0].payload)
    print(f"✓ job.started payload: {payload}")

    required_fields = ['job_name', 'package_name', 'parameters']
    missing = [f for f in required_fields if f not in payload]

    if missing:
        print(f"❌ Missing fields in job.started payload: {missing}")
        return False

    print(f"✓ All required fields present in job.started payload")

    # Check job.completed payload
    query = f"""
        SELECT payload
        FROM `{dataset}.event_log`
        WHERE event_type = 'job.completed'
          AND correlation_id LIKE 'gmail_ingest_acct1-%'
        ORDER BY created_at DESC
        LIMIT 1
    """

    result = list(client.query(query).result())
    payload = result[0].payload if isinstance(result[0].payload, dict) else json.loads(result[0].payload)
    print(f"✓ job.completed payload: {payload}")

    if 'duration_seconds' not in payload:
        print(f"❌ Missing duration_seconds in job.completed payload")
        return False

    print(f"✓ job.completed has duration_seconds: {payload['duration_seconds']}s")

    # Check ingestion.completed payload
    query = f"""
        SELECT payload
        FROM `{dataset}.event_log`
        WHERE event_type = 'ingestion.completed'
        ORDER BY created_at DESC
        LIMIT 1
    """

    result = list(client.query(query).result())
    payload = result[0].payload if isinstance(result[0].payload, dict) else json.loads(result[0].payload)
    print(f"✓ ingestion.completed payload: {payload}")

    required_fields = ['records_extracted', 'duration_seconds']
    missing = [f for f in required_fields if f not in payload]

    if missing:
        print(f"❌ Missing fields in ingestion.completed payload: {missing}")
        return False

    print(f"✓ All required fields present in ingestion.completed payload")
    return True


def test_3_no_per_object_events():
    """Test that we're NOT creating per-object events anymore."""
    print("\n" + "="*80)
    print("TEST 3: No Per-Object Events (Critical)")
    print("="*80)

    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    # Get most recent job run
    query = f"""
        SELECT correlation_id
        FROM `{dataset}.event_log`
        WHERE correlation_id LIKE 'gmail_ingest_acct1-%'
        ORDER BY created_at DESC
        LIMIT 1
    """

    result = list(client.query(query).result())
    run_id = result[0].correlation_id

    # Check for email.received events in this run
    query = f"""
        SELECT COUNT(*) as count
        FROM `{dataset}.event_log`
        WHERE correlation_id = '{run_id}'
          AND event_type = 'email.received'
    """

    result = list(client.query(query).result())
    count = result[0].count

    print(f"✓ Checking run: {run_id}")
    print(f"  Per-object events (email.received): {count}")

    if count == 0:
        print(f"✓ SUCCESS: No per-object events found!")
        print(f"  This means we moved from 'N events' to '1 event' pattern")
        return True
    else:
        print(f"❌ FAIL: Found {count} per-object events (should be 0)")
        return False


def test_4_batch_upsert_verification():
    """Test that objects are being batch upserted correctly."""
    print("\n" + "="*80)
    print("TEST 4: Batch Upsert Verification")
    print("="*80)

    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    # Get most recent ingestion event
    query = f"""
        SELECT
            source_system,
            payload
        FROM `{dataset}.event_log`
        WHERE event_type = 'ingestion.completed'
        ORDER BY created_at DESC
        LIMIT 1
    """

    result = list(client.query(query).result())
    if not result:
        print("❌ No ingestion.completed events found")
        return False

    source_system = result[0].source_system
    payload = result[0].payload if isinstance(result[0].payload, dict) else json.loads(result[0].payload)
    records_extracted = payload.get('records_extracted', 0)

    print(f"✓ Latest ingestion: {source_system}")
    print(f"  Records extracted: {records_extracted}")

    # Check raw_objects has corresponding data
    query = f"""
        SELECT COUNT(*) as count
        FROM `{dataset}.raw_objects`
        WHERE source_system = '{source_system}'
          AND object_type = 'email'
    """

    result = list(client.query(query).result())
    objects_stored = result[0].count

    print(f"  Objects in raw_objects: {objects_stored}")

    if objects_stored >= records_extracted:
        print(f"✓ Objects stored >= records extracted (accounts for previous runs)")
        return True
    else:
        print(f"❌ Objects stored < records extracted (something went wrong)")
        return False


def test_5_idem_key_nullability():
    """Test that idem_key is nullable for telemetry events."""
    print("\n" + "="*80)
    print("TEST 5: idem_key Nullability for Telemetry")
    print("="*80)

    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    # Check that telemetry events have NULL idem_key
    query = f"""
        SELECT
            event_type,
            idem_key
        FROM `{dataset}.event_log`
        WHERE event_type IN ('job.started', 'job.completed', 'ingestion.completed')
        ORDER BY created_at DESC
        LIMIT 5
    """

    result = list(client.query(query).result())

    print(f"✓ Checking recent telemetry events:")
    all_null = True
    for row in result:
        is_null = row.idem_key is None
        status = "✓" if is_null else "❌"
        print(f"  {status} {row.event_type}: idem_key={row.idem_key}")
        if not is_null:
            all_null = False

    if all_null:
        print(f"✓ All telemetry events have NULL idem_key (correct)")
        return True
    else:
        print(f"❌ Some telemetry events have non-NULL idem_key")
        return False


def test_6_correlation_id_consistency():
    """Test that correlation_id is used consistently within a job run."""
    print("\n" + "="*80)
    print("TEST 6: Correlation ID Consistency")
    print("="*80)

    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    # Get most recent job run
    query = f"""
        SELECT correlation_id
        FROM `{dataset}.event_log`
        WHERE correlation_id LIKE 'gmail_ingest_acct1-%'
        ORDER BY created_at DESC
        LIMIT 1
    """

    result = list(client.query(query).result())
    run_id = result[0].correlation_id

    # Get all events for this correlation_id
    query = f"""
        SELECT
            event_type,
            source_system,
            object_type
        FROM `{dataset}.event_log`
        WHERE correlation_id = '{run_id}'
        ORDER BY created_at
    """

    result = list(client.query(query).result())

    print(f"✓ Events for correlation_id: {run_id}")
    for row in result:
        print(f"  - {row.event_type} (source: {row.source_system}, object_type: {row.object_type})")

    # All job events should have same correlation_id
    job_events = [r for r in result if r.object_type == 'job_run']

    if len(job_events) >= 2:
        print(f"✓ Found {len(job_events)} job events with same correlation_id")
        return True
    else:
        print(f"❌ Expected at least 2 job events with same correlation_id")
        return False


def test_7_event_timestamps():
    """Test that event timestamps are logical (job.started < job.completed)."""
    print("\n" + "="*80)
    print("TEST 7: Event Timestamp Ordering")
    print("="*80)

    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    # Get most recent job run
    query = f"""
        SELECT correlation_id
        FROM `{dataset}.event_log`
        WHERE correlation_id LIKE 'gmail_ingest_acct1-%'
          AND event_type = 'job.completed'
        ORDER BY created_at DESC
        LIMIT 1
    """

    result = list(client.query(query).result())
    if not result:
        print("❌ No completed job runs found")
        return False

    run_id = result[0].correlation_id

    # Get all events for this specific run
    query = f"""
        SELECT
            event_type,
            created_at
        FROM `{dataset}.event_log`
        WHERE correlation_id = '{run_id}'
        ORDER BY created_at
    """

    result = list(client.query(query).result())

    print(f"✓ Event timeline for {run_id}:")
    for row in result:
        print(f"  {row.created_at}: {row.event_type}")

    # Verify ordering
    if len(result) >= 2:
        if result[0].event_type == 'job.started' and result[-1].event_type == 'job.completed':
            duration = (result[-1].created_at - result[0].created_at).total_seconds()
            print(f"✓ Timestamps are ordered correctly (duration: {duration}s)")
            return True
        else:
            print(f"❌ Event ordering is incorrect")
            print(f"   Expected: job.started first, job.completed last")
            print(f"   Got: {result[0].event_type} first, {result[-1].event_type} last")
            return False
    else:
        print(f"❌ Not enough events to verify ordering")
        return False


def run_acceptance_tests():
    """Run all acceptance tests."""
    print("\n" + "█"*80)
    print("ACCEPTANCE TESTING")
    print("█"*80)

    tests = [
        test_1_job_lifecycle_tracking,
        test_2_telemetry_payload_structure,
        test_3_no_per_object_events,
        test_4_batch_upsert_verification,
        test_5_idem_key_nullability,
        test_6_correlation_id_consistency,
        test_7_event_timestamps,
    ]

    results = []
    for test_func in tests:
        try:
            passed = test_func()
            results.append((test_func.__name__, passed))
        except Exception as e:
            print(f"\n❌ Test failed with exception: {e}")
            results.append((test_func.__name__, False))

    # Summary
    print("\n" + "█"*80)
    print("ACCEPTANCE TEST SUMMARY")
    print("█"*80)

    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)

    for test_name, passed in results:
        status = "✓ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")

    print(f"\n{passed_count}/{total_count} tests passed")

    if passed_count == total_count:
        print("\n🎉 ALL ACCEPTANCE TESTS PASSED!")
        print("\nThe refactored event client is ready for production!")
        return True
    else:
        print(f"\n⚠️  {total_count - passed_count} test(s) failed")
        print("\nPlease review the failures above.")
        return False


if __name__ == "__main__":
    success = run_acceptance_tests()
    exit(0 if success else 1)
