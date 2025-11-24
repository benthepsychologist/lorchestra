"""Tests for event_client module (refactored API)."""
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime
import uuid


@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client."""
    client = MagicMock()
    client.insert_rows_json.return_value = []  # No errors

    # Mock load_table_from_json for upsert_objects
    mock_load_job = MagicMock()
    mock_load_job.result.return_value = None
    client.load_table_from_json.return_value = mock_load_job

    # Mock query for MERGE operations
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = None
    client.query.return_value = mock_query_job

    # Mock delete_table for cleanup
    client.delete_table.return_value = None

    return client


@pytest.fixture
def env_vars(monkeypatch):
    """Set required environment variables."""
    monkeypatch.setenv("EVENTS_BQ_DATASET", "test_dataset")
    monkeypatch.setenv("EVENT_LOG_TABLE", "event_log")
    monkeypatch.setenv("RAW_OBJECTS_TABLE", "raw_objects")


# ============================================================================
# log_event() tests
# ============================================================================


def test_log_event_basic(mock_bq_client, env_vars):
    """Test basic log_event call."""
    from lorchestra.stack_clients.event_client import log_event

    log_event(
        event_type="job.started",
        source_system="lorchestra",
        correlation_id="test-run-123",
        object_type="job_run",
        status="ok",
        bq_client=mock_bq_client,
    )

    # Should call insert_rows_json for event_log
    mock_bq_client.insert_rows_json.assert_called_once()
    args = mock_bq_client.insert_rows_json.call_args
    assert args[0][0] == "test_dataset.event_log"
    assert len(args[0][1]) == 1  # One row

    envelope = args[0][1][0]
    assert envelope["event_type"] == "job.started"
    assert envelope["source_system"] == "lorchestra"
    assert envelope["correlation_id"] == "test-run-123"
    assert envelope["object_type"] == "job_run"
    assert envelope["status"] == "ok"
    assert envelope["idem_key"] is None  # Telemetry event has no idem_key
    assert envelope["payload"] is None


def test_log_event_with_payload(mock_bq_client, env_vars):
    """Test log_event with small telemetry payload."""
    from lorchestra.stack_clients.event_client import log_event

    log_event(
        event_type="ingestion.completed",
        source_system="tap-gmail--acct1",
        correlation_id="gmail-run-456",
        status="ok",
        payload={"records_extracted": 100, "duration_seconds": 12.5},
        bq_client=mock_bq_client,
    )

    envelope = mock_bq_client.insert_rows_json.call_args[0][1][0]
    assert envelope["payload"] == {"records_extracted": 100, "duration_seconds": 12.5}


def test_log_event_with_error(mock_bq_client, env_vars):
    """Test log_event with error status."""
    from lorchestra.stack_clients.event_client import log_event

    log_event(
        event_type="job.failed",
        source_system="lorchestra",
        correlation_id="test-run-789",
        object_type="job_run",
        status="failed",
        error_message="Connection timeout",
        payload={"error_type": "TimeoutError", "retryable": True},
        bq_client=mock_bq_client,
    )

    envelope = mock_bq_client.insert_rows_json.call_args[0][1][0]
    assert envelope["status"] == "failed"
    assert envelope["error_message"] == "Connection timeout"
    assert envelope["payload"]["error_type"] == "TimeoutError"


def test_log_event_with_trace_id(mock_bq_client, env_vars):
    """Test log_event with trace_id."""
    from lorchestra.stack_clients.event_client import log_event

    log_event(
        event_type="job.started",
        source_system="lorchestra",
        correlation_id="test-run-123",
        trace_id="trace-abc-xyz",
        bq_client=mock_bq_client,
    )

    envelope = mock_bq_client.insert_rows_json.call_args[0][1][0]
    assert envelope["trace_id"] == "trace-abc-xyz"


def test_log_event_missing_event_type(mock_bq_client, env_vars):
    """Test that missing event_type raises ValueError."""
    from lorchestra.stack_clients.event_client import log_event

    with pytest.raises(ValueError, match="event_type"):
        log_event(
            event_type="",
            source_system="test",
            correlation_id="test-123",
            bq_client=mock_bq_client,
        )


def test_log_event_missing_source_system(mock_bq_client, env_vars):
    """Test that missing source_system raises ValueError."""
    from lorchestra.stack_clients.event_client import log_event

    with pytest.raises(ValueError, match="source_system"):
        log_event(
            event_type="test.event",
            source_system="",
            correlation_id="test-123",
            bq_client=mock_bq_client,
        )


def test_log_event_missing_correlation_id(mock_bq_client, env_vars):
    """Test that missing correlation_id raises ValueError."""
    from lorchestra.stack_clients.event_client import log_event

    with pytest.raises(ValueError, match="correlation_id"):
        log_event(
            event_type="test.event",
            source_system="test",
            correlation_id="",
            bq_client=mock_bq_client,
        )


def test_log_event_insert_failure(mock_bq_client, env_vars):
    """Test that event_log insert errors are raised."""
    from lorchestra.stack_clients.event_client import log_event

    # Simulate BQ insert error
    mock_bq_client.insert_rows_json.return_value = [{"error": "test error"}]

    with pytest.raises(RuntimeError, match="event_log insert failed"):
        log_event(
            event_type="test.event",
            source_system="test",
            correlation_id="test-123",
            bq_client=mock_bq_client,
        )


# ============================================================================
# upsert_objects() tests
# ============================================================================


def test_upsert_objects_basic(mock_bq_client, env_vars):
    """Test basic upsert_objects call."""
    from lorchestra.stack_clients.event_client import upsert_objects

    objects = [
        {"id": "msg1", "subject": "Test 1"},
        {"id": "msg2", "subject": "Test 2"},
    ]

    def idem_key_fn(obj):
        return f"email:test-source:{obj['id']}"

    upsert_objects(
        objects=objects,
        source_system="test-source",
        object_type="email",
        correlation_id="test-run-123",
        idem_key_fn=idem_key_fn,
        bq_client=mock_bq_client,
    )

    # Should call load_table_from_json
    mock_bq_client.load_table_from_json.assert_called_once()

    # Should call query for MERGE
    mock_bq_client.query.assert_called_once()
    merge_query = mock_bq_client.query.call_args[0][0]
    assert "MERGE" in merge_query
    assert "test_dataset.raw_objects" in merge_query


def test_upsert_objects_with_iterator(mock_bq_client, env_vars):
    """Test upsert_objects with iterator input."""
    from lorchestra.stack_clients.event_client import upsert_objects

    def object_generator():
        yield {"id": "msg1", "subject": "Test 1"}
        yield {"id": "msg2", "subject": "Test 2"}

    def idem_key_fn(obj):
        return f"email:test-source:{obj['id']}"

    upsert_objects(
        objects=object_generator(),
        source_system="test-source",
        object_type="email",
        correlation_id="test-run-123",
        idem_key_fn=idem_key_fn,
        bq_client=mock_bq_client,
    )

    # Should process batch
    mock_bq_client.load_table_from_json.assert_called_once()


def test_upsert_objects_batching(mock_bq_client, env_vars):
    """Test upsert_objects processes in batches."""
    from lorchestra.stack_clients.event_client import upsert_objects

    # Create 3 objects with batch_size=2
    objects = [
        {"id": f"msg{i}", "subject": f"Test {i}"}
        for i in range(3)
    ]

    def idem_key_fn(obj):
        return f"email:test-source:{obj['id']}"

    upsert_objects(
        objects=objects,
        source_system="test-source",
        object_type="email",
        correlation_id="test-run-123",
        idem_key_fn=idem_key_fn,
        batch_size=2,  # Should create 2 batches
        bq_client=mock_bq_client,
    )

    # Should call load_table_from_json twice (2 batches)
    assert mock_bq_client.load_table_from_json.call_count == 2

    # Should call query twice (2 MERGEs)
    assert mock_bq_client.query.call_count == 2


def test_upsert_objects_missing_source_system(mock_bq_client, env_vars):
    """Test that missing source_system raises ValueError."""
    from lorchestra.stack_clients.event_client import upsert_objects

    with pytest.raises(ValueError, match="source_system"):
        upsert_objects(
            objects=[],
            source_system="",
            object_type="email",
            correlation_id="test-123",
            idem_key_fn=lambda x: "test",
            bq_client=mock_bq_client,
        )


def test_upsert_objects_missing_idem_key_fn(mock_bq_client, env_vars):
    """Test that missing idem_key_fn raises ValueError."""
    from lorchestra.stack_clients.event_client import upsert_objects

    with pytest.raises(ValueError, match="idem_key_fn"):
        upsert_objects(
            objects=[],
            source_system="test",
            object_type="email",
            correlation_id="test-123",
            idem_key_fn=None,  # Missing!
            bq_client=mock_bq_client,
        )


def test_upsert_objects_load_failure(mock_bq_client, env_vars):
    """Test that load failures are raised."""
    from lorchestra.stack_clients.event_client import upsert_objects

    # Simulate load error
    mock_bq_client.load_table_from_json.side_effect = Exception("Load failed")

    def idem_key_fn(obj):
        return f"email:test-source:{obj['id']}"

    with pytest.raises(RuntimeError, match="Batch upsert failed"):
        upsert_objects(
            objects=[{"id": "msg1"}],
            source_system="test",
            object_type="email",
            correlation_id="test-123",
            idem_key_fn=idem_key_fn,
            bq_client=mock_bq_client,
        )


def test_upsert_objects_cleanup_temp_table(mock_bq_client, env_vars):
    """Test that temp table is cleaned up after MERGE."""
    from lorchestra.stack_clients.event_client import upsert_objects

    objects = [{"id": "msg1", "subject": "Test"}]

    def idem_key_fn(obj):
        return f"email:test-source:{obj['id']}"

    upsert_objects(
        objects=objects,
        source_system="test-source",
        object_type="email",
        correlation_id="test-run-123",
        idem_key_fn=idem_key_fn,
        bq_client=mock_bq_client,
    )

    # Should call delete_table to cleanup
    mock_bq_client.delete_table.assert_called_once()
    temp_table_ref = mock_bq_client.delete_table.call_args[0][0]
    assert temp_table_ref.startswith("test_dataset.temp_objects_")


# ============================================================================
# idem_keys module tests
# ============================================================================


def test_gmail_idem_key():
    """Test gmail_idem_key function."""
    from lorchestra.idem_keys import gmail_idem_key

    fn = gmail_idem_key("tap-gmail--acct1")
    idem_key = fn({"id": "msg123", "subject": "Test"})

    assert idem_key == "email:tap-gmail--acct1:msg123"


def test_gmail_idem_key_missing_id():
    """Test gmail_idem_key raises error when id is missing."""
    from lorchestra.idem_keys import gmail_idem_key

    fn = gmail_idem_key("tap-gmail--acct1")

    with pytest.raises(ValueError, match="missing 'id' field"):
        fn({"subject": "Test"})  # No id field


def test_stripe_charge_idem_key():
    """Test stripe_charge_idem_key function."""
    from lorchestra.idem_keys import stripe_charge_idem_key

    fn = stripe_charge_idem_key("tap-stripe--prod")
    idem_key = fn({"id": "ch_123", "amount": 1000})

    assert idem_key == "charge:tap-stripe--prod:ch_123"


def test_stripe_charge_idem_key_missing_id():
    """Test stripe_charge_idem_key raises error when id is missing."""
    from lorchestra.idem_keys import stripe_charge_idem_key

    fn = stripe_charge_idem_key("tap-stripe--prod")

    with pytest.raises(ValueError, match="missing 'id' field"):
        fn({"amount": 1000})  # No id field


# ============================================================================
# Integration-style tests
# ============================================================================


def test_log_event_and_upsert_objects_together(mock_bq_client, env_vars):
    """Test using both log_event and upsert_objects in same workflow."""
    from lorchestra.stack_clients.event_client import log_event, upsert_objects
    from lorchestra.idem_keys import gmail_idem_key

    correlation_id = "gmail-20251124120000"

    # Log job started
    log_event(
        event_type="job.started",
        source_system="lorchestra",
        correlation_id=correlation_id,
        object_type="job_run",
        status="ok",
        payload={"job_name": "gmail_ingest"},
        bq_client=mock_bq_client,
    )

    # Upsert emails
    emails = [
        {"id": "msg1", "subject": "Test 1"},
        {"id": "msg2", "subject": "Test 2"},
    ]

    upsert_objects(
        objects=emails,
        source_system="tap-gmail--acct1",
        object_type="email",
        correlation_id=correlation_id,
        idem_key_fn=gmail_idem_key("tap-gmail--acct1"),
        bq_client=mock_bq_client,
    )

    # Log ingestion completed
    log_event(
        event_type="ingestion.completed",
        source_system="tap-gmail--acct1",
        correlation_id=correlation_id,
        status="ok",
        payload={"records_extracted": 2, "duration_seconds": 5.2},
        bq_client=mock_bq_client,
    )

    # Log job completed
    log_event(
        event_type="job.completed",
        source_system="lorchestra",
        correlation_id=correlation_id,
        object_type="job_run",
        status="ok",
        payload={"job_name": "gmail_ingest", "duration_seconds": 6.0},
        bq_client=mock_bq_client,
    )

    # Should have 3 event_log inserts
    assert mock_bq_client.insert_rows_json.call_count == 3

    # Should have 1 batch upsert
    assert mock_bq_client.load_table_from_json.call_count == 1
    assert mock_bq_client.query.call_count == 1
