"""Tests for event_client module."""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
import uuid


def test_generate_idem_key_with_natural_id():
    """Test idem_key generation when payload has natural ID."""
    from lorchestra.stack_clients.event_client import generate_idem_key

    payload = {"id": "msg123", "subject": "Test"}
    idem_key = generate_idem_key("tap-gmail", "email", payload)

    assert idem_key == "email:tap-gmail:msg123"


def test_generate_idem_key_with_message_id():
    """Test idem_key generation with message_id field."""
    from lorchestra.stack_clients.event_client import generate_idem_key

    payload = {"message_id": "abc456", "body": "content"}
    idem_key = generate_idem_key("tap-exchange", "email", payload)

    assert idem_key == "email:tap-exchange:abc456"


def test_generate_idem_key_without_natural_id():
    """Test idem_key generation falls back to content hash."""
    from lorchestra.stack_clients.event_client import generate_idem_key

    payload = {"answer": "yes", "timestamp": "2024-01-01"}
    idem_key = generate_idem_key("tap-forms", "response", payload)

    # Should be content hash format
    assert idem_key.startswith("response:tap-forms:")
    assert len(idem_key.split(":")[2]) == 16  # Hash is 16 chars


def test_build_envelope_basic():
    """Test envelope creation with required fields."""
    from lorchestra.stack_clients.event_client import build_envelope

    envelope = build_envelope(
        event_type="test.event",
        source="test_source",
        object_type="test_object",
        idem_key="test:test_source:123"
    )

    assert envelope["event_type"] == "test.event"
    assert envelope["source_system"] == "test_source"
    assert envelope["object_type"] == "test_object"
    assert envelope["idem_key"] == "test:test_source:123"
    assert envelope["status"] == "ok"
    assert uuid.UUID(envelope["event_id"])  # Valid UUID
    assert datetime.fromisoformat(envelope["created_at"])  # Valid ISO 8601
    assert "payload" not in envelope  # Payload goes to raw_objects, not envelope


def test_build_envelope_with_optional_fields():
    """Test envelope with all optional fields populated."""
    from lorchestra.stack_clients.event_client import build_envelope

    envelope = build_envelope(
        event_type="test.event",
        source="test_source",
        object_type="test_object",
        idem_key="test:test_source:123",
        correlation_id="corr-123",
        subject_id="subj-456",
        status="error",
        error_message="Test error"
    )

    assert envelope["correlation_id"] == "corr-123"
    assert envelope["subject_id"] == "subj-456"
    assert envelope["status"] == "error"
    assert envelope["error_message"] == "Test error"


def test_build_envelope_missing_event_type():
    """Test that missing event_type raises ValueError."""
    from lorchestra.stack_clients.event_client import build_envelope

    with pytest.raises(ValueError, match="event_type"):
        build_envelope(
            event_type="",
            source="test",
            object_type="test",
            idem_key="test:test:123"
        )


def test_build_envelope_missing_source():
    """Test that missing source raises ValueError."""
    from lorchestra.stack_clients.event_client import build_envelope

    with pytest.raises(ValueError, match="source"):
        build_envelope(
            event_type="test.event",
            source="",
            object_type="test",
            idem_key="test:test:123"
        )


def test_build_envelope_missing_object_type():
    """Test that missing object_type raises ValueError."""
    from lorchestra.stack_clients.event_client import build_envelope

    with pytest.raises(ValueError, match="object_type"):
        build_envelope(
            event_type="test.event",
            source="test",
            object_type="",
            idem_key="test:test:123"
        )


@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client."""
    client = MagicMock()
    client.insert_rows_json.return_value = []  # No errors

    # Mock query for MERGE operations
    mock_job = MagicMock()
    mock_job.result.return_value = None
    client.query.return_value = mock_job

    return client


@pytest.fixture
def env_vars(monkeypatch):
    """Set required environment variables."""
    monkeypatch.setenv("EVENTS_BQ_DATASET", "test_dataset")
    monkeypatch.setenv("EVENT_LOG_TABLE", "event_log")
    monkeypatch.setenv("RAW_OBJECTS_TABLE", "raw_objects")


def test_get_event_log_table_ref_success(mock_bq_client, env_vars):
    """Test event_log table ref construction from env vars."""
    from lorchestra.stack_clients.event_client import _get_event_log_table_ref

    ref = _get_event_log_table_ref(mock_bq_client)
    assert ref == "test_dataset.event_log"


def test_get_raw_objects_table_ref_success(mock_bq_client, env_vars):
    """Test raw_objects table ref construction from env vars."""
    from lorchestra.stack_clients.event_client import _get_raw_objects_table_ref

    ref = _get_raw_objects_table_ref(mock_bq_client)
    assert ref == "test_dataset.raw_objects"


def test_get_event_log_table_ref_missing_dataset(mock_bq_client, monkeypatch):
    """Test that missing EVENTS_BQ_DATASET raises RuntimeError."""
    from lorchestra.stack_clients.event_client import _get_event_log_table_ref

    monkeypatch.delenv("EVENTS_BQ_DATASET", raising=False)

    with pytest.raises(RuntimeError, match="EVENTS_BQ_DATASET"):
        _get_event_log_table_ref(mock_bq_client)


def test_emit_writes_to_both_tables(mock_bq_client, env_vars):
    """Test emit() writes to both event_log and raw_objects."""
    from lorchestra.stack_clients.event_client import emit

    emit(
        event_type="test.event",
        payload={"id": "123", "data": "value"},
        source="test_source",
        object_type="test_object",
        bq_client=mock_bq_client
    )

    # Should call insert_rows_json for event_log
    mock_bq_client.insert_rows_json.assert_called_once()
    args = mock_bq_client.insert_rows_json.call_args
    assert args[0][0] == "test_dataset.event_log"
    assert len(args[0][1]) == 1  # One row
    assert args[0][1][0]["event_type"] == "test.event"

    # Should call query for raw_objects MERGE
    mock_bq_client.query.assert_called_once()
    query_sql = mock_bq_client.query.call_args[0][0]
    assert "MERGE" in query_sql
    assert "test_dataset.raw_objects" in query_sql


def test_emit_missing_env_dataset(mock_bq_client, monkeypatch):
    """Test that missing EVENTS_BQ_DATASET raises RuntimeError."""
    from lorchestra.stack_clients.event_client import emit

    monkeypatch.setenv("EVENT_LOG_TABLE", "event_log")
    monkeypatch.delenv("EVENTS_BQ_DATASET", raising=False)

    with pytest.raises(RuntimeError, match="EVENTS_BQ_DATASET"):
        emit(
            event_type="test.event",
            payload={},
            source="test",
            object_type="test",
            bq_client=mock_bq_client
        )


def test_emit_event_log_insert_failure(mock_bq_client, env_vars):
    """Test that event_log insert errors are raised."""
    from lorchestra.stack_clients.event_client import emit

    # Simulate BQ insert error
    mock_bq_client.insert_rows_json.return_value = [{"error": "test error"}]

    with pytest.raises(RuntimeError, match="event_log insert failed"):
        emit(
            event_type="test.event",
            payload={"id": "123"},
            source="test",
            object_type="test",
            bq_client=mock_bq_client
        )


def test_emit_raw_objects_merge_failure(mock_bq_client, env_vars):
    """Test that raw_objects MERGE errors are raised."""
    from lorchestra.stack_clients.event_client import emit

    # Simulate MERGE query error
    mock_bq_client.query.side_effect = Exception("MERGE failed")

    with pytest.raises(RuntimeError, match="raw_objects MERGE failed"):
        emit(
            event_type="test.event",
            payload={"id": "123"},
            source="test",
            object_type="test",
            bq_client=mock_bq_client
        )


def test_emit_generates_consistent_idem_key(mock_bq_client, env_vars):
    """Test that same payload generates same idem_key (idempotency)."""
    from lorchestra.stack_clients.event_client import emit, generate_idem_key

    payload = {"id": "msg123", "subject": "Test"}

    # Generate idem_key directly
    expected_idem_key = generate_idem_key("test_source", "email", payload)

    emit(
        event_type="email.received",
        payload=payload,
        source="test_source",
        object_type="email",
        bq_client=mock_bq_client
    )

    # Check event_log envelope has correct idem_key
    envelope = mock_bq_client.insert_rows_json.call_args[0][1][0]
    assert envelope["idem_key"] == expected_idem_key

    # Check raw_objects MERGE query uses same idem_key
    query_config = mock_bq_client.query.call_args[1]["job_config"]
    idem_key_param = next(p for p in query_config.query_parameters if p.name == "idem_key")
    assert idem_key_param.value == expected_idem_key
