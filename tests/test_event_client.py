"""Tests for event_client module."""
import pytest
from unittest.mock import MagicMock
from datetime import datetime
import uuid


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


def test_build_envelope_with_optional_fields():
    """Test envelope with all optional fields populated."""
    from lorchestra.stack_clients.event_client import build_envelope

    envelope = build_envelope(
        event_type="test.event",
        payload={"key": "value"},
        source="test_source",
        schema_ref="test.v1",
        correlation_id="corr-123",
        subject_id="subj-456"
    )

    assert envelope["schema_ref"] == "test.v1"
    assert envelope["correlation_id"] == "corr-123"
    assert envelope["subject_id"] == "subj-456"


def test_build_envelope_missing_event_type():
    """Test that missing event_type raises ValueError."""
    from lorchestra.stack_clients.event_client import build_envelope

    with pytest.raises(ValueError, match="event_type"):
        build_envelope(event_type="", payload={}, source="test")


def test_build_envelope_missing_source():
    """Test that missing source raises ValueError."""
    from lorchestra.stack_clients.event_client import build_envelope

    with pytest.raises(ValueError, match="source"):
        build_envelope(event_type="test", payload={}, source="")


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


def test_emit_missing_env_dataset(mock_bq_client, monkeypatch):
    """Test that missing EVENTS_BQ_DATASET raises RuntimeError."""
    from lorchestra.stack_clients.event_client import emit

    monkeypatch.setenv("EVENTS_BQ_TABLE", "test_table")
    monkeypatch.delenv("EVENTS_BQ_DATASET", raising=False)

    with pytest.raises(RuntimeError, match="EVENTS_BQ_DATASET"):
        emit(
            event_type="test.event",
            payload={},
            source="test",
            bq_client=mock_bq_client
        )


def test_emit_missing_env_table(mock_bq_client, monkeypatch):
    """Test that missing EVENTS_BQ_TABLE raises RuntimeError."""
    from lorchestra.stack_clients.event_client import emit

    monkeypatch.setenv("EVENTS_BQ_DATASET", "test_dataset")
    monkeypatch.delenv("EVENTS_BQ_TABLE", raising=False)

    with pytest.raises(RuntimeError, match="EVENTS_BQ_TABLE"):
        emit(
            event_type="test.event",
            payload={},
            source="test",
            bq_client=mock_bq_client
        )


def test_emit_bq_insert_failure(mock_bq_client, env_vars):
    """Test that BQ insert errors are raised."""
    from lorchestra.stack_clients.event_client import emit

    # Simulate BQ insert error
    mock_bq_client.insert_rows_json.return_value = [{"error": "test error"}]

    with pytest.raises(RuntimeError, match="BigQuery insert failed"):
        emit(
            event_type="test.event",
            payload={},
            source="test",
            bq_client=mock_bq_client
        )


def test_get_bq_table_ref_success(env_vars):
    """Test table ref construction from env vars."""
    from lorchestra.stack_clients.event_client import _get_bq_table_ref

    ref = _get_bq_table_ref(None)
    assert ref == "test_dataset.test_table"
