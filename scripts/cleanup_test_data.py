#!/usr/bin/env python
"""
Clean up test data from BigQuery tables.

This script removes:
1. Old per-object events (email.received) from before the refactor
2. Test runs and failed job attempts
3. Optionally: ALL data for a fresh start

Be careful - this deletes data!
"""

import os
from google.cloud import bigquery
from datetime import datetime, timezone

def show_current_state():
    """Show what's currently in the tables."""
    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    print("\n" + "="*80)
    print("CURRENT STATE")
    print("="*80)

    # Event log summary
    query = f"""
        SELECT
            event_type,
            COUNT(*) as count
        FROM `{dataset}.event_log`
        GROUP BY event_type
        ORDER BY count DESC
    """

    print("\nEvent Log:")
    for row in client.query(query).result():
        print(f"  {row.event_type}: {row.count} events")

    # Total event count
    query = f"SELECT COUNT(*) as count FROM `{dataset}.event_log`"
    total_events = next(iter(client.query(query).result())).count
    print(f"\nTotal events: {total_events}")

    # Raw objects summary
    query = f"""
        SELECT
            source_system,
            object_type,
            COUNT(*) as count
        FROM `{dataset}.raw_objects`
        GROUP BY source_system, object_type
        ORDER BY count DESC
    """

    print("\nRaw Objects:")
    for row in client.query(query).result():
        print(f"  {row.source_system}/{row.object_type}: {row.count} objects")

    # Total object count
    query = f"SELECT COUNT(*) as count FROM `{dataset}.raw_objects`"
    total_objects = next(iter(client.query(query).result())).count
    print(f"\nTotal objects: {total_objects}")


def cleanup_old_per_object_events():
    """Remove old email.received events from before refactor."""
    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    print("\n" + "="*80)
    print("CLEANUP: Old Per-Object Events")
    print("="*80)

    # Count old events
    query = f"""
        SELECT COUNT(*) as count
        FROM `{dataset}.event_log`
        WHERE event_type = 'email.received'
    """
    count = next(iter(client.query(query).result())).count

    if count == 0:
        print("✓ No old per-object events found")
        return

    print(f"\nFound {count} old email.received events")
    response = input(f"Delete these {count} events? (yes/no): ")

    if response.lower() == 'yes':
        query = f"""
            DELETE FROM `{dataset}.event_log`
            WHERE event_type = 'email.received'
        """
        client.query(query).result()
        print(f"✓ Deleted {count} old email.received events")
    else:
        print("Skipped")


def cleanup_failed_jobs():
    """Remove failed job attempts."""
    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    print("\n" + "="*80)
    print("CLEANUP: Failed Job Attempts")
    print("="*80)

    # Count failed job events
    query = f"""
        SELECT COUNT(*) as count
        FROM `{dataset}.event_log`
        WHERE event_type IN ('job.failed', 'ingestion.failed')
    """
    count = next(iter(client.query(query).result())).count

    if count == 0:
        print("✓ No failed job events found")
        return

    print(f"\nFound {count} failed job/ingestion events")

    # Show them
    query = f"""
        SELECT event_type, correlation_id, created_at, error_message
        FROM `{dataset}.event_log`
        WHERE event_type IN ('job.failed', 'ingestion.failed')
        ORDER BY created_at DESC
        LIMIT 10
    """

    print("\nMost recent failures:")
    for row in client.query(query).result():
        print(f"  {row.created_at}: {row.event_type} - {row.error_message[:60]}")

    response = input(f"\nDelete these {count} failed events? (yes/no): ")

    if response.lower() == 'yes':
        query = f"""
            DELETE FROM `{dataset}.event_log`
            WHERE event_type IN ('job.failed', 'ingestion.failed')
        """
        client.query(query).result()
        print(f"✓ Deleted {count} failed job events")
    else:
        print("Skipped")


def cleanup_all_test_data():
    """Nuclear option: delete ALL data from both tables."""
    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    print("\n" + "="*80)
    print("CLEANUP: ALL DATA (Nuclear Option)")
    print("="*80)

    # Count everything
    query = f"SELECT COUNT(*) as count FROM `{dataset}.event_log`"
    event_count = next(iter(client.query(query).result())).count

    query = f"SELECT COUNT(*) as count FROM `{dataset}.raw_objects`"
    object_count = next(iter(client.query(query).result())).count

    print(f"\n⚠️  WARNING: This will delete:")
    print(f"  - {event_count} events from event_log")
    print(f"  - {object_count} objects from raw_objects")
    print(f"\n⚠️  This action CANNOT be undone!")

    response = input(f"\nAre you SURE you want to delete ALL data? Type 'DELETE ALL' to confirm: ")

    if response == 'DELETE ALL':
        print("\nDeleting all data...")

        # Delete event_log
        query = f"DELETE FROM `{dataset}.event_log` WHERE TRUE"
        client.query(query).result()
        print(f"✓ Deleted all events from event_log")

        # Delete raw_objects
        query = f"DELETE FROM `{dataset}.raw_objects` WHERE TRUE"
        client.query(query).result()
        print(f"✓ Deleted all objects from raw_objects")

        print("\n✓ All test data deleted - fresh start!")
    else:
        print("Cancelled - no data deleted")


def main():
    """Interactive cleanup menu."""
    print("\n" + "█"*80)
    print("BIGQUERY TEST DATA CLEANUP")
    print("█"*80)

    show_current_state()

    print("\n" + "="*80)
    print("CLEANUP OPTIONS")
    print("="*80)
    print("\n1. Remove old per-object events (email.received)")
    print("2. Remove failed job attempts")
    print("3. NUCLEAR: Delete ALL data (fresh start)")
    print("4. Exit (no changes)")

    choice = input("\nSelect option (1-4): ")

    if choice == '1':
        cleanup_old_per_object_events()
    elif choice == '2':
        cleanup_failed_jobs()
    elif choice == '3':
        cleanup_all_test_data()
    elif choice == '4':
        print("Exiting - no changes made")
    else:
        print("Invalid choice")
        return

    # Show final state
    print("\n" + "="*80)
    print("FINAL STATE")
    print("="*80)
    show_current_state()


if __name__ == "__main__":
    main()
