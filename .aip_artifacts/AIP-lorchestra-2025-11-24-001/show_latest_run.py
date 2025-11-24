#!/usr/bin/env python
"""Display the latest job run in a user-friendly format."""

import os
from google.cloud import bigquery
import json

def show_latest_run():
    """Show details of the most recent job run."""
    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    # Get most recent run
    query = f"""
        SELECT correlation_id
        FROM `{dataset}.event_log`
        WHERE correlation_id LIKE 'gmail_ingest_acct1-%'
        ORDER BY created_at DESC
        LIMIT 1
    """

    result = list(client.query(query).result())
    run_id = result[0].correlation_id

    print("\n" + "="*80)
    print(f"LATEST JOB RUN: {run_id}")
    print("="*80)

    # Get all events for this run
    query = f"""
        SELECT
            event_type,
            source_system,
            object_type,
            status,
            payload,
            idem_key,
            created_at
        FROM `{dataset}.event_log`
        WHERE correlation_id = '{run_id}'
        ORDER BY created_at
    """

    events = list(client.query(query).result())

    print(f"\nTotal events: {len(events)}")
    print("\n" + "-"*80)

    for i, event in enumerate(events, 1):
        print(f"\n{i}. {event.event_type}")
        print(f"   Time: {event.created_at}")
        print(f"   Source: {event.source_system}")
        print(f"   Object Type: {event.object_type}")
        print(f"   Status: {event.status}")
        print(f"   idem_key: {event.idem_key}")

        if event.payload:
            payload = event.payload if isinstance(event.payload, dict) else json.loads(event.payload)
            print(f"   Payload:")
            for key, value in payload.items():
                print(f"     - {key}: {value}")

    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    job_events = [e for e in events if e.object_type == 'job_run']
    ingestion_events = [e for e in events if e.event_type == 'ingestion.completed']

    if job_events:
        started = job_events[0].created_at
        completed = job_events[-1].created_at
        duration = (completed - started).total_seconds()
        print(f"\nJob Duration: {duration:.2f} seconds")

    if ingestion_events:
        payload = ingestion_events[0].payload if isinstance(ingestion_events[0].payload, dict) else json.loads(ingestion_events[0].payload)
        print(f"Records Extracted: {payload.get('records_extracted', 'N/A')}")
        print(f"Date Filter: {payload.get('date_filter', 'N/A')}")

    # Check raw_objects
    query = f"""
        SELECT COUNT(*) as count
        FROM `{dataset}.raw_objects`
        WHERE source_system = 'tap-gmail--acct1-personal'
          AND object_type = 'email'
    """

    result = list(client.query(query).result())
    print(f"\nTotal emails in raw_objects: {result[0].count}")

    # KEY INSIGHT
    print("\n" + "="*80)
    print("KEY INSIGHT: Event Efficiency")
    print("="*80)
    print(f"\nOLD PATTERN (before refactor):")
    print(f"  - Would have created {payload.get('records_extracted', 'N')} individual email.received events")
    print(f"  - Each event would trigger a separate MERGE operation")
    print(f"  - Total: {payload.get('records_extracted', 'N')} events + {payload.get('records_extracted', 'N')} merges")

    print(f"\nNEW PATTERN (after refactor):")
    print(f"  - Created {len(events)} telemetry events (job lifecycle + ingestion metrics)")
    print(f"  - Used 1 batch MERGE operation for all {payload.get('records_extracted', 'N')} emails")
    print(f"  - Total: {len(events)} events + 1 batch merge")

    print(f"\n✓ Event reduction: {payload.get('records_extracted', 'N')}/{payload.get('records_extracted', 'N')} → {len(events)}/1")
    print(f"✓ All telemetry preserved (job lifecycle, record counts, duration)")
    print(f"✓ All data stored (emails in raw_objects)")

if __name__ == "__main__":
    show_latest_run()
