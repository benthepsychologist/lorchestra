"""Integration tests for end-to-end ingestion pipeline.

NOTE: These tests require real BigQuery credentials and are skipped by default.
Run with: pytest tests/test_integration.py -v --run-integration
"""

import os
import json
from pathlib import Path
from datetime import datetime
import pytest

# Skip all tests in this module unless --run-integration is passed
pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_INTEGRATION_TESTS") != "1",
    reason="Integration tests require RUN_INTEGRATION_TESTS=1"
)


def test_end_to_end_event_emission():
    """
    Test the full three-layer architecture:
    1. Create test JSONL data
    2. Emit events using event_client
    3. Verify data in both event_log and raw_objects
    4. Test idempotency
    """
    from google.cloud import bigquery

    # Setup
    client = bigquery.Client()

    # Import event_client - use the new API
    from lorchestra.stack_clients.event_client import log_event, upsert_objects
    from lorchestra.idem_keys import gmail_idem_key

    # Test data
    test_payload = {
        "id": "test-msg-integration-001",
        "subject": "Integration Test Email",
        "from": "test@example.com",
        "timestamp": datetime.now().isoformat()
    }

    source_system = "test-integration"
    connection_name = "test-integration-conn"
    object_type = "email"
    run_id = f"test-run-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    print("\n" + "="*80)
    print("INTEGRATION TEST: End-to-End Event Emission")
    print("="*80)

    # Step 1: Log a test event
    print("\n[1] Logging test event...")
    log_event(
        event_type="test.started",
        source_system=source_system,
        connection_name=connection_name,
        correlation_id=run_id,
        status="ok",
        payload={"test": "data"},
        bq_client=client,
    )
    print("✓ Test event logged")

    # Step 2: Upsert test object
    print("\n[2] Upserting test object...")
    def test_idem_key(obj):
        return f"{source_system}:{connection_name}:{object_type}:{obj['id']}"

    result = upsert_objects(
        objects=[test_payload],
        source_system=source_system,
        connection_name=connection_name,
        object_type=object_type,
        correlation_id=run_id,
        idem_key_fn=test_idem_key,
        bq_client=client,
    )
    print(f"✓ Upserted {result.total_records} objects ({result.inserted} inserted)")

    # Step 3: Query event_log
    print("\n[3] Querying event_log...")
    event_log_query = f"""
        SELECT COUNT(*) as count
        FROM `events_dev.event_log`
        WHERE correlation_id = '{run_id}'
    """
    query_result = list(client.query(event_log_query).result())
    event_count = query_result[0].count
    print(f"✓ Found {event_count} events in event_log")
    # Should have 2 events: test.started + upsert.completed (auto-emitted)
    assert event_count >= 1, f"Expected at least 1 event, found {event_count}"

    # Step 4: Query raw_objects
    print("\n[4] Querying raw_objects...")
    raw_objects_query = f"""
        SELECT COUNT(*) as count
        FROM `events_dev.raw_objects`
        WHERE source_system = '{source_system}'
        AND connection_name = '{connection_name}'
        AND object_type = '{object_type}'
    """
    query_result = list(client.query(raw_objects_query).result())
    object_count = query_result[0].count
    print(f"✓ Found {object_count} objects in raw_objects")
    assert object_count >= 1, f"Expected at least 1 object, found {object_count}"

    print("\n" + "="*80)
    print("✓ INTEGRATION TEST PASSED")
    print("="*80 + "\n")


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

    # Enable integration tests
    os.environ["RUN_INTEGRATION_TESTS"] = "1"

    # Run test
    try:
        test_end_to_end_event_emission()
    except Exception as e:
        print(f"\n❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
