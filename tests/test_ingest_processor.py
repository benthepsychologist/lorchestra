"""Tests for IngestProcessor."""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call
from typing import Any, Iterator

from lorchestra.processors.base import JobContext, UpsertResult
from lorchestra.processors.ingest import (
    IngestProcessor,
    _parse_date_to_datetime,
    _get_last_sync_timestamp,
    _get_idem_key_fn,
)


class TestParseDateToDatetime:
    """Tests for date parsing helper."""

    def test_parse_relative_days(self):
        """Parse relative day strings like '-7d'."""
        now = datetime.now(timezone.utc)
        result = _parse_date_to_datetime("-7d")

        # Should be approximately 7 days ago (within a few seconds)
        delta = now - result
        # delta.days can be 6 if close to midnight boundary, so check total_seconds
        assert 6 * 86400 < delta.total_seconds() < 8 * 86400

    def test_parse_iso_format(self):
        """Parse ISO format dates."""
        result = _parse_date_to_datetime("2024-01-15T10:30:00Z")
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30

    def test_parse_iso_with_offset(self):
        """Parse ISO format with timezone offset."""
        result = _parse_date_to_datetime("2024-01-15T10:30:00+00:00")
        assert result.year == 2024
        assert result.tzinfo is not None


class TestGetIdemKeyFn:
    """Tests for idem_key function resolution."""

    def test_gmail_stream(self):
        """Gmail streams use gmail_idem_key."""
        fn = _get_idem_key_fn("gmail", "gmail-acct1", "email", "gmail.messages")
        result = fn({"id": "msg123"})
        assert result == "gmail:gmail-acct1:email:msg123"

    def test_exchange_stream(self):
        """Exchange streams use exchange_idem_key."""
        fn = _get_idem_key_fn("exchange", "exchange-ben", "email", "exchange.messages")
        result = fn({"id": "AAMk123"})
        assert result == "exchange:exchange-ben:email:AAMk123"

    def test_stripe_stream(self):
        """Stripe streams use stripe_idem_key with object_type."""
        fn = _get_idem_key_fn("stripe", "stripe-prod", "customer", "stripe.customers")
        result = fn({"id": "cus_123"})
        assert result == "stripe:stripe-prod:customer:cus_123"

    def test_google_forms_stream(self):
        """Google Forms streams use google_forms_idem_key."""
        fn = _get_idem_key_fn("google_forms", "forms-intake", "form_response", "google_forms.responses")
        result = fn({"responseId": "resp123"})
        assert result == "google_forms:forms-intake:form_response:resp123"

    def test_generic_fallback(self):
        """Unknown streams use generic fallback with 'id' field."""
        fn = _get_idem_key_fn("custom", "custom-conn", "widget", "custom.widgets")
        result = fn({"id": "widget123"})
        assert result == "custom:custom-conn:widget:widget123"

    def test_missing_id_raises(self):
        """Missing ID field raises ValueError."""
        fn = _get_idem_key_fn("custom", "custom-conn", "widget", "custom.widgets")
        with pytest.raises(ValueError, match="missing 'id' field"):
            fn({"name": "no-id"})


class MockStorageClient:
    """Mock storage client for testing."""

    def __init__(self, upsert_result: UpsertResult | None = None):
        self.upsert_calls = []
        self.upsert_result = upsert_result or UpsertResult(inserted=5, updated=2)

    def upsert_objects(
        self,
        objects: Iterator[dict[str, Any]],
        source_system: str,
        connection_name: str,
        object_type: str,
        idem_key_fn,
        correlation_id: str,
    ) -> UpsertResult:
        # Consume iterator to count objects
        obj_list = list(objects)
        self.upsert_calls.append({
            "objects": obj_list,
            "source_system": source_system,
            "connection_name": connection_name,
            "object_type": object_type,
            "correlation_id": correlation_id,
        })
        return self.upsert_result


class MockEventClient:
    """Mock event client for testing."""

    def __init__(self):
        self.events = []

    def log_event(
        self,
        event_type: str,
        source_system: str,
        correlation_id: str,
        status: str,
        connection_name: str | None = None,
        target_object_type: str | None = None,
        payload: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> None:
        self.events.append({
            "event_type": event_type,
            "source_system": source_system,
            "connection_name": connection_name,
            "target_object_type": target_object_type,
            "correlation_id": correlation_id,
            "status": status,
            "payload": payload,
            "error_message": error_message,
        })


class MockStream:
    """Mock injest stream for testing."""

    def __init__(self, records: list[dict[str, Any]]):
        self.records = records

    def extract(self, since=None, until=None):
        for record in self.records:
            yield record


class TestIngestProcessor:
    """Tests for IngestProcessor."""

    @pytest.fixture
    def processor(self):
        return IngestProcessor()

    @pytest.fixture
    def job_spec(self):
        return {
            "job_id": "gmail_ingest_acct1",
            "job_type": "ingest",
            "source": {
                "stream": "gmail.messages",
                "identity": "gmail:acct1",
            },
            "sink": {
                "source_system": "gmail",
                "connection_name": "gmail-acct1",
                "object_type": "email",
            },
            "options": {},
        }

    @pytest.fixture
    def context(self):
        return JobContext(
            bq_client=MagicMock(),
            run_id="test-run-123",
            dry_run=False,
        )

    @pytest.fixture
    def storage_client(self):
        return MockStorageClient()

    @pytest.fixture
    def event_client(self):
        return MockEventClient()

    def test_successful_ingest(self, processor, job_spec, context, storage_client, event_client):
        """Successful ingest writes objects and emits completion event."""
        mock_stream = MockStream([
            {"id": "msg1", "subject": "Test 1"},
            {"id": "msg2", "subject": "Test 2"},
        ])

        # Mock injest module which is imported inside run()
        mock_injest = MagicMock()
        mock_injest.get_stream.return_value = mock_stream

        mock_injest_config = MagicMock()

        with patch.dict("sys.modules", {
            "injest": mock_injest,
            "lorchestra.injest_config": mock_injest_config,
        }):
            processor.run(job_spec, context, storage_client, event_client)

        # Verify objects were upserted
        assert len(storage_client.upsert_calls) == 1
        call = storage_client.upsert_calls[0]
        assert call["source_system"] == "gmail"
        assert call["connection_name"] == "gmail-acct1"
        assert call["object_type"] == "email"
        assert len(call["objects"]) == 2

        # Verify completion event
        assert len(event_client.events) == 1
        event = event_client.events[0]
        assert event["event_type"] == "ingest.completed"
        assert event["status"] == "ok"
        assert event["payload"]["records_extracted"] == 2
        assert event["payload"]["inserted"] == 5
        assert event["payload"]["updated"] == 2

    def test_dry_run_mode(self, processor, job_spec, storage_client, event_client):
        """Dry run mode extracts but doesn't write."""
        context = JobContext(bq_client=MagicMock(), run_id="test-dry", dry_run=True)

        mock_stream = MockStream([{"id": "msg1"}])
        mock_injest = MagicMock()
        mock_injest.get_stream.return_value = mock_stream

        with patch.dict("sys.modules", {
            "injest": mock_injest,
            "lorchestra.injest_config": MagicMock(),
        }):
            processor.run(job_spec, context, storage_client, event_client)

        # No upsert calls in dry run
        assert len(storage_client.upsert_calls) == 0

        # Event still emitted
        assert len(event_client.events) == 1
        event = event_client.events[0]
        assert event["payload"]["dry_run"] is True
        assert event["payload"]["inserted"] == 0
        assert event["payload"]["updated"] == 0

    def test_custom_event_names(self, processor, job_spec, context, storage_client, event_client):
        """Custom event names from job_spec are used."""
        job_spec["events"] = {
            "on_complete": "gmail.sync.done",
            "on_fail": "gmail.sync.error",
        }

        mock_stream = MockStream([{"id": "msg1"}])
        mock_injest = MagicMock()
        mock_injest.get_stream.return_value = mock_stream

        with patch.dict("sys.modules", {
            "injest": mock_injest,
            "lorchestra.injest_config": MagicMock(),
        }):
            processor.run(job_spec, context, storage_client, event_client)

        assert event_client.events[0]["event_type"] == "gmail.sync.done"

    def test_failure_emits_error_event(self, processor, job_spec, context, storage_client, event_client):
        """Failures emit error event with details."""
        mock_injest = MagicMock()
        mock_injest.get_stream.side_effect = RuntimeError("API connection failed")

        with patch.dict("sys.modules", {
            "injest": mock_injest,
            "lorchestra.injest_config": MagicMock(),
        }):
            with pytest.raises(RuntimeError):
                processor.run(job_spec, context, storage_client, event_client)

        # Error event emitted
        assert len(event_client.events) == 1
        event = event_client.events[0]
        assert event["event_type"] == "ingest.failed"
        assert event["status"] == "failed"
        assert event["error_message"] == "API connection failed"
        assert event["payload"]["error_type"] == "RuntimeError"

    def test_limit_option(self, processor, job_spec, context, storage_client, event_client):
        """Limit option caps number of records processed."""
        job_spec["options"]["limit"] = 2

        mock_stream = MockStream([
            {"id": "msg1"},
            {"id": "msg2"},
            {"id": "msg3"},
            {"id": "msg4"},
        ])
        mock_injest = MagicMock()
        mock_injest.get_stream.return_value = mock_stream

        with patch.dict("sys.modules", {
            "injest": mock_injest,
            "lorchestra.injest_config": MagicMock(),
        }):
            processor.run(job_spec, context, storage_client, event_client)

        # Only 2 records processed due to limit
        call = storage_client.upsert_calls[0]
        assert len(call["objects"]) == 2

    def test_auto_since_queries_bq(self, processor, job_spec, context, storage_client, event_client):
        """auto_since option queries BigQuery for last sync."""
        job_spec["options"]["auto_since"] = True

        mock_stream = MockStream([{"id": "msg1"}])
        mock_injest = MagicMock()
        mock_injest.get_stream.return_value = mock_stream

        with patch.dict("sys.modules", {
            "injest": mock_injest,
            "lorchestra.injest_config": MagicMock(),
        }):
            with patch("lorchestra.processors.ingest._get_last_sync_timestamp", return_value="2024-01-15T10:00:00+00:00") as mock_get_last_sync:
                processor.run(job_spec, context, storage_client, event_client)

                # Verify last sync was queried
                mock_get_last_sync.assert_called_once_with(
                    context.bq_client, "gmail", "gmail-acct1", "email"
                )


class TestIngestProcessorRegistration:
    """Test that IngestProcessor is registered."""

    def test_registered_in_global_registry(self):
        """IngestProcessor is registered for 'ingest' job type."""
        from lorchestra.processors import registry

        # Import to trigger registration
        import lorchestra.processors.ingest  # noqa: F401

        processor = registry.get("ingest")
        assert isinstance(processor, IngestProcessor)
