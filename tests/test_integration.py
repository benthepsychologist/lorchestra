"""Integration tests for end-to-end ingestion pipeline."""

import os
import json
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery


def test_end_to_end_event_emission():
    """
    Test the full three-layer architecture:
    1. Create test JSONL data
    2. Emit events using event_client
    3. Verify data in both event_log and raw_objects
    4. Test idempotency
    """
    # Setup
    client = bigquery.Client()

    # Import event_client
    from lorchestra.stack_clients.event_client import emit

    # Test data
    test_payload = {
        "id": "test-msg-integration-001",
        "subject": "Integration Test Email",
        "from": "test@example.com",
        "timestamp": datetime.now().isoformat()
    }

    source_system = "tap-test-integration"
    object_type = "email"
    event_type = "test.email.received"
    run_id = f"test-run-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    print("\n" + "="*80)
    print("INTEGRATION TEST: End-to-End Event Emission")
    print("="*80)

    # Step 1: Emit first event
    print("\n[1] Emitting first event...")
    emit(
        event_type=event_type,
        payload=test_payload,
        source=source_system,
        object_type=object_type,
        bq_client=client,
        correlation_id=run_id
    )
    print("✓ First event emitted")

    # Step 2: Query event_log
    print("\n[2] Querying event_log...")
    event_log_query = f"""
        SELECT COUNT(*) as count
        FROM `events_dev.event_log`
        WHERE correlation_id = '{run_id}'
    """
    result = list(client.query(event_log_query).result())
    event_count = result[0].count
    print(f"✓ Found {event_count} events in event_log")
    assert event_count == 1, f"Expected 1 event, found {event_count}"

    # Step 3: Query raw_objects
    print("\n[3] Querying raw_objects...")
    raw_objects_query = f"""
        SELECT COUNT(*) as count
        FROM `events_dev.raw_objects`
        WHERE source_system = '{source_system}'
        AND object_type = '{object_type}'
    """
    result = list(client.query(raw_objects_query).result())
    object_count = result[0].count
    print(f"✓ Found {object_count} objects in raw_objects")
    assert object_count == 1, f"Expected 1 object, found {object_count}"

    # Step 4: Test idempotency (re-emit same event)
    print("\n[4] Testing idempotency (re-emitting same event)...")
    emit(
        event_type=event_type,
        payload=test_payload,
        source=source_system,
        object_type=object_type,
        bq_client=client,
        correlation_id=run_id
    )
    print("✓ Second event emitted")

    # Step 5: Verify event_log has 2 events (audit trail)
    print("\n[5] Verifying event_log has 2 events...")
    result = list(client.query(event_log_query).result())
    event_count = result[0].count
    print(f"✓ Found {event_count} events in event_log")
    assert event_count == 2, f"Expected 2 events, found {event_count}"

    # Step 6: Verify raw_objects still has 1 object (deduplicated)
    print("\n[6] Verifying raw_objects still has 1 object (deduped)...")
    result = list(client.query(raw_objects_query).result())
    object_count = result[0].count
    print(f"✓ Found {object_count} objects in raw_objects")
    assert object_count == 1, f"Expected 1 object (deduped), found {object_count}"

    # Step 7: Verify payload in raw_objects
    print("\n[7] Verifying payload stored correctly...")
    payload_query = f"""
        SELECT payload, first_seen, last_seen
        FROM `events_dev.raw_objects`
        WHERE source_system = '{source_system}'
        AND object_type = '{object_type}'
        LIMIT 1
    """
    result = list(client.query(payload_query).result())
    row = result[0]
    stored_payload = json.loads(row.payload) if isinstance(row.payload, str) else row.payload
    print(f"✓ Payload retrieved: {stored_payload.get('subject')}")
    assert stored_payload['id'] == test_payload['id']
    assert stored_payload['subject'] == test_payload['subject']

    # Note: Skipping cleanup - BigQuery streaming buffer doesn't support DELETE
    # Test data will remain but is clearly marked with source_system='tap-test-integration'
    print("\n[8] Test data will remain (BigQuery streaming buffer doesn't support DELETE)")

    print("\n" + "="*80)
    print("✓ ALL INTEGRATION TESTS PASSED")
    print("="*80 + "\n")

    return True


if __name__ == "__main__":
    # Load environment
    from dotenv import load_dotenv
    load_dotenv()

    # Verify environment
    required_vars = ['GOOGLE_APPLICATION_CREDENTIALS', 'EVENTS_BQ_DATASET', 'GCP_PROJECT']
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        print(f"❌ Missing environment variables: {missing}")
        print("Run: source .env")
        exit(1)

    # Run test
    try:
        test_end_to_end_event_emission()
    except Exception as e:
        print(f"\n❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
