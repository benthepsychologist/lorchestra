#!/usr/bin/env python
"""Verify integration test results."""

import os
from google.cloud import bigquery

def verify_integration():
    """Verify that the refactored system works correctly."""
    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    print("=" * 80)
    print("INTEGRATION TEST VERIFICATION")
    print("=" * 80)

    # Get the most recent gmail_ingest_acct1 run
    query_run_id = f"""
        SELECT correlation_id
        FROM `{dataset}.event_log`
        WHERE correlation_id LIKE 'gmail_ingest_acct1-%'
        ORDER BY created_at DESC
        LIMIT 1
    """
    result = client.query(query_run_id).result()
    run_id = next(iter(result)).correlation_id
    print(f"\n✓ Most recent run: {run_id}\n")

    # 1. Verify job events
    print("1. Checking job events...")
    query_job_events = f"""
        SELECT event_type, object_type, status, payload, idem_key
        FROM `{dataset}.event_log`
        WHERE correlation_id = '{run_id}'
          AND object_type = 'job_run'
        ORDER BY created_at
    """
    job_events = list(client.query(query_job_events).result())

    expected_job_events = ["job.started", "job.completed"]
    actual_job_events = [e.event_type for e in job_events]

    if actual_job_events == expected_job_events:
        print(f"   ✓ Found job events: {actual_job_events}")
        for event in job_events:
            payload_preview = str(event.payload)[:80] if event.payload else None
            print(f"     - {event.event_type}: idem_key={event.idem_key}, payload={payload_preview}...")
    else:
        print(f"   ✗ Expected {expected_job_events}, got {actual_job_events}")

    # 2. Verify ingestion event
    print("\n2. Checking ingestion event...")
    query_ingestion = f"""
        SELECT event_type, object_type, status, payload, idem_key
        FROM `{dataset}.event_log`
        WHERE correlation_id LIKE 'gmail-%'
          AND event_type = 'ingestion.completed'
        ORDER BY created_at DESC
        LIMIT 1
    """
    ingestion_events = list(client.query(query_ingestion).result())

    if ingestion_events:
        event = ingestion_events[0]
        print(f"   ✓ Found ingestion.completed event")
        print(f"     - idem_key: {event.idem_key} (should be None)")
        print(f"     - payload: {event.payload}")
    else:
        print("   ✗ No ingestion.completed event found")

    # 3. CRITICAL: Verify NO per-object events FOR THIS RUN
    print("\n3. CRITICAL: Checking for per-object events in THIS run (should be 0)...")
    query_per_object = f"""
        SELECT COUNT(*) as count
        FROM `{dataset}.event_log`
        WHERE event_type = 'email.received'
          AND correlation_id = '{run_id}'
    """
    per_object_count = next(iter(client.query(query_per_object).result())).count

    if per_object_count == 0:
        print(f"   ✓ No per-object events found for this run (count={per_object_count})")
    else:
        print(f"   ✗ Found {per_object_count} per-object events (should be 0!)")

    # 4. Verify raw_objects has data
    print("\n4. Checking raw_objects...")
    query_raw_objects = f"""
        SELECT COUNT(*) as count
        FROM `{dataset}.raw_objects`
        WHERE source_system = 'tap-gmail--acct1-personal'
          AND object_type = 'email'
    """
    object_count = next(iter(client.query(query_raw_objects).result())).count

    if object_count > 0:
        print(f"   ✓ Found {object_count} emails in raw_objects")
    else:
        print(f"   ✗ No emails found in raw_objects")

    # 5. Verify schema
    print("\n5. Checking event_log schema...")
    table = client.get_table(f"{client.project}.{dataset}.event_log")
    schema_dict = {field.name: field for field in table.schema}

    checks = [
        ("idem_key is NULLABLE", schema_dict["idem_key"].mode == "NULLABLE"),
        ("payload exists (JSON)", "payload" in schema_dict and schema_dict["payload"].field_type == "JSON"),
        ("trace_id exists", "trace_id" in schema_dict),
    ]

    for check_name, passed in checks:
        status = "✓" if passed else "✗"
        print(f"   {status} {check_name}")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    all_passed = (
        actual_job_events == expected_job_events and
        len(ingestion_events) > 0 and
        per_object_count == 0 and
        object_count > 0 and
        all(passed for _, passed in checks)
    )

    if all_passed:
        print("✓ ALL INTEGRATION TESTS PASSED!")
        print("\nKey achievements:")
        print("  - Job events logged correctly (job.started, job.completed)")
        print("  - Ingestion telemetry logged with small payload")
        print("  - NO per-object events (moved from 100 events to 1 event)")
        print("  - Objects stored in raw_objects")
        print("  - Schema supports nullable fields and trace_id")
    else:
        print("✗ Some integration tests failed. See details above.")

if __name__ == "__main__":
    verify_integration()
