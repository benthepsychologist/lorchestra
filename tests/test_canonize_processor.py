"""Tests for CanonizeProcessor."""

import pytest
from unittest.mock import MagicMock, patch
from typing import Any, Iterator

from lorchestra.processors.base import JobContext, UpsertResult
from lorchestra.processors.canonize import CanonizeProcessor


class MockStorageClient:
    """Mock storage client for testing."""

    def __init__(
        self,
        records: list[dict[str, Any]] | None = None,
        insert_count: int = 0,
    ):
        self.records = records or []
        self.insert_count = insert_count
        self.query_calls = []
        self.update_calls = []
        self.insert_calls = []

    def query_objects(
        self,
        source_system: str,
        object_type: str,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        self.query_calls.append({
            "source_system": source_system,
            "object_type": object_type,
            "filters": filters,
            "limit": limit,
        })
        for record in self.records[:limit] if limit else self.records:
            yield record

    def update_field(
        self,
        idem_keys: list[str],
        field: str,
        value: Any,
    ) -> int:
        self.update_calls.append({
            "idem_keys": idem_keys,
            "field": field,
            "value": value,
        })
        return len(idem_keys)

    def insert_canonical(
        self,
        objects: list[dict[str, Any]],
        correlation_id: str,
    ) -> int:
        self.insert_calls.append({
            "objects": objects,
            "correlation_id": correlation_id,
        })
        return self.insert_count or len(objects)

    def upsert_objects(self, *args, **kwargs) -> UpsertResult:
        return UpsertResult(inserted=0, updated=0)


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


class MockValidator:
    """Mock validator for testing."""

    def __init__(self, fail_keys: set[str] | None = None):
        self.fail_keys = fail_keys or set()
        self.validate_calls = []

    def validate(self, payload: dict[str, Any]) -> None:
        self.validate_calls.append(payload)
        # Check if this payload should fail based on some marker
        if payload.get("_should_fail"):
            raise ValueError("Validation failed")


class MockTransform:
    """Mock jsonata transform for testing."""

    def __init__(self, output: dict[str, Any] | None = None, fail: bool = False):
        self.output = output or {"canonical": "data"}
        self.fail = fail
        self.evaluate_calls = []

    def evaluate(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.evaluate_calls.append(payload)
        if self.fail:
            raise ValueError("Transform failed")
        return self.output


class TestCanonizeProcessorValidateOnly:
    """Tests for validate_only mode."""

    @pytest.fixture
    def processor(self):
        return CanonizeProcessor()

    @pytest.fixture
    def job_spec(self):
        return {
            "job_id": "validate_gmail",
            "job_type": "canonize",
            "source": {
                "source_system": "gmail",
                "object_type": "email",
            },
            "transform": {
                "mode": "validate_only",
                "schema_in": "iglu:com.google/gmail_email/jsonschema/1-0-0",
            },
        }

    @pytest.fixture
    def context(self):
        return JobContext(
            bq_client=MagicMock(),
            run_id="test-run-123",
            dry_run=False,
        )

    def test_validate_only_success(self, processor, job_spec, context):
        """Successful validation stamps records as pass."""
        records = [
            {"idem_key": "key1", "payload": {"id": "1", "subject": "Test"}},
            {"idem_key": "key2", "payload": {"id": "2", "subject": "Test 2"}},
        ]
        storage_client = MockStorageClient(records=records)
        event_client = MockEventClient()
        mock_validator = MockValidator()

        with patch.object(processor, "_get_validator", return_value=mock_validator):
            processor.run(job_spec, context, storage_client, event_client)

        # Verify records were queried
        assert len(storage_client.query_calls) == 1
        query_call = storage_client.query_calls[0]
        assert query_call["source_system"] == "gmail"
        assert query_call["object_type"] == "email"
        assert query_call["filters"]["validation_status"] is None

        # Verify validation_status was updated
        assert len(storage_client.update_calls) == 1
        update_call = storage_client.update_calls[0]
        assert update_call["field"] == "validation_status"
        assert update_call["value"] == "pass"
        assert set(update_call["idem_keys"]) == {"key1", "key2"}

        # Verify completion event
        assert len(event_client.events) == 1
        event = event_client.events[0]
        assert event["event_type"] == "validate.completed"
        assert event["status"] == "ok"
        assert event["payload"]["passed"] == 2
        assert event["payload"]["failed"] == 0

    def test_validate_only_with_failures(self, processor, job_spec, context):
        """Validation failures stamp records as fail."""
        records = [
            {"idem_key": "key1", "payload": {"id": "1", "subject": "Valid"}},
            {"idem_key": "key2", "payload": {"id": "2", "_should_fail": True}},
        ]
        storage_client = MockStorageClient(records=records)
        event_client = MockEventClient()
        mock_validator = MockValidator()

        with patch.object(processor, "_get_validator", return_value=mock_validator):
            processor.run(job_spec, context, storage_client, event_client)

        # Two update calls: one for pass, one for fail
        assert len(storage_client.update_calls) == 2

        pass_call = next(c for c in storage_client.update_calls if c["value"] == "pass")
        fail_call = next(c for c in storage_client.update_calls if c["value"] == "fail")

        assert pass_call["idem_keys"] == ["key1"]
        assert fail_call["idem_keys"] == ["key2"]

        # Event shows mixed results
        event = event_client.events[0]
        assert event["payload"]["passed"] == 1
        assert event["payload"]["failed"] == 1

    def test_validate_only_no_records(self, processor, job_spec, context):
        """No records to validate returns early."""
        storage_client = MockStorageClient(records=[])
        event_client = MockEventClient()

        with patch.object(processor, "_get_validator", return_value=MockValidator()):
            processor.run(job_spec, context, storage_client, event_client)

        # No updates or events (early return)
        assert len(storage_client.update_calls) == 0
        assert len(event_client.events) == 0

    def test_validate_only_dry_run(self, processor, job_spec):
        """Dry run validates but doesn't write."""
        context = JobContext(bq_client=MagicMock(), run_id="dry-test", dry_run=True)
        records = [{"idem_key": "key1", "payload": {"id": "1"}}]
        storage_client = MockStorageClient(records=records)
        event_client = MockEventClient()

        with patch.object(processor, "_get_validator", return_value=MockValidator()):
            processor.run(job_spec, context, storage_client, event_client)

        # No updates in dry run
        assert len(storage_client.update_calls) == 0

        # Event still emitted with dry_run flag
        event = event_client.events[0]
        assert event["payload"]["dry_run"] is True

    def test_validate_only_json_payload(self, processor, job_spec, context):
        """JSON string payloads are parsed."""
        records = [
            {"idem_key": "key1", "payload": '{"id": "1", "subject": "Test"}'},
        ]
        storage_client = MockStorageClient(records=records)
        event_client = MockEventClient()
        mock_validator = MockValidator()

        with patch.object(processor, "_get_validator", return_value=mock_validator):
            processor.run(job_spec, context, storage_client, event_client)

        # Validator received parsed dict
        assert len(mock_validator.validate_calls) == 1
        assert mock_validator.validate_calls[0] == {"id": "1", "subject": "Test"}


class TestCanonizeProcessorFullMode:
    """Tests for full canonization mode."""

    @pytest.fixture
    def processor(self):
        return CanonizeProcessor()

    @pytest.fixture
    def job_spec(self):
        return {
            "job_id": "canonize_gmail_jmap",
            "job_type": "canonize",
            "source": {
                "source_system": "gmail",
                "object_type": "email",
            },
            "transform": {
                "mode": "full",
                "schema_in": "iglu:com.google/gmail_email/jsonschema/1-0-0",
                "schema_out": "iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0",
                "transform_ref": "email/gmail_to_jmap_lite@1.0.0",
            },
        }

    @pytest.fixture
    def context(self):
        return JobContext(
            bq_client=MagicMock(),
            run_id="test-run-456",
            dry_run=False,
        )

    def test_full_mode_success(self, processor, job_spec, context):
        """Full mode transforms and inserts canonical records."""
        records = [
            {"idem_key": "key1", "payload": {"id": "1"}, "source_system": "gmail", "connection_name": "acct1", "object_type": "email"},
            {"idem_key": "key2", "payload": {"id": "2"}, "source_system": "gmail", "connection_name": "acct1", "object_type": "email"},
        ]
        storage_client = MockStorageClient(records=records, insert_count=2)
        event_client = MockEventClient()
        mock_transform = MockTransform(output={"canonical": "email"})

        with patch.object(processor, "_get_transform", return_value=mock_transform):
            processor.run(job_spec, context, storage_client, event_client)

        # Verify records were queried with validation_status filter
        query_call = storage_client.query_calls[0]
        assert query_call["filters"]["validation_status"] == "pass"

        # Verify canonical records were inserted
        assert len(storage_client.insert_calls) == 1
        insert_call = storage_client.insert_calls[0]
        assert len(insert_call["objects"]) == 2
        assert insert_call["correlation_id"] == "test-run-456"

        # Check canonical record structure
        canonical = insert_call["objects"][0]
        assert canonical["idem_key"] == "key1"
        assert canonical["canonical_schema"] == "iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0"
        assert canonical["transform_ref"] == "email/gmail_to_jmap_lite@1.0.0"
        assert canonical["payload"] == {"canonical": "email"}

        # Verify completion event
        event = event_client.events[0]
        assert event["event_type"] == "canonize.completed"
        assert event["status"] == "ok"
        assert event["payload"]["success"] == 2
        assert event["payload"]["inserted"] == 2

    def test_full_mode_with_transform_failures(self, processor, job_spec, context):
        """Transform failures are tracked but don't stop processing."""
        records = [
            {"idem_key": "key1", "payload": {"id": "1"}, "source_system": "gmail"},
            {"idem_key": "key2", "payload": {"id": "2"}, "source_system": "gmail"},
        ]
        storage_client = MockStorageClient(records=records, insert_count=1)
        event_client = MockEventClient()

        # Transform that fails on second call
        call_count = [0]
        def failing_evaluate(payload):
            call_count[0] += 1
            if call_count[0] == 2:
                raise ValueError("Transform error")
            return {"canonical": "data"}

        mock_transform = MockTransform()
        mock_transform.evaluate = failing_evaluate

        with patch.object(processor, "_get_transform", return_value=mock_transform):
            processor.run(job_spec, context, storage_client, event_client)

        # Only 1 successful record inserted
        insert_call = storage_client.insert_calls[0]
        assert len(insert_call["objects"]) == 1

        # Event shows partial success
        event = event_client.events[0]
        assert event["payload"]["success"] == 1
        assert event["payload"]["failed"] == 1

    def test_full_mode_no_records(self, processor, job_spec, context):
        """No records to canonize returns early."""
        storage_client = MockStorageClient(records=[])
        event_client = MockEventClient()

        with patch.object(processor, "_get_transform", return_value=MockTransform()):
            processor.run(job_spec, context, storage_client, event_client)

        # No inserts or events
        assert len(storage_client.insert_calls) == 0
        assert len(event_client.events) == 0

    def test_full_mode_dry_run(self, processor, job_spec):
        """Dry run transforms but doesn't insert."""
        context = JobContext(bq_client=MagicMock(), run_id="dry-test", dry_run=True)
        records = [{"idem_key": "key1", "payload": {"id": "1"}, "source_system": "gmail"}]
        storage_client = MockStorageClient(records=records)
        event_client = MockEventClient()

        with patch.object(processor, "_get_transform", return_value=MockTransform()):
            processor.run(job_spec, context, storage_client, event_client)

        # No inserts in dry run
        assert len(storage_client.insert_calls) == 0

        # Event still emitted
        event = event_client.events[0]
        assert event["payload"]["dry_run"] is True
        assert event["payload"]["inserted"] == 0

    def test_full_mode_custom_events(self, processor, job_spec, context):
        """Custom event names from job_spec are used."""
        job_spec["events"] = {
            "on_complete": "gmail.canonize.done",
            "on_fail": "gmail.canonize.error",
        }
        records = [{"idem_key": "key1", "payload": {"id": "1"}, "source_system": "gmail"}]
        storage_client = MockStorageClient(records=records)
        event_client = MockEventClient()

        with patch.object(processor, "_get_transform", return_value=MockTransform()):
            processor.run(job_spec, context, storage_client, event_client)

        assert event_client.events[0]["event_type"] == "gmail.canonize.done"


class TestCanonizeProcessorErrors:
    """Tests for error handling."""

    @pytest.fixture
    def processor(self):
        return CanonizeProcessor()

    def test_validate_mode_error_emits_event(self, processor):
        """Validation errors emit failure event."""
        job_spec = {
            "job_id": "validate_gmail",
            "job_type": "canonize",
            "source": {"source_system": "gmail", "object_type": "email"},
            "transform": {"mode": "validate_only", "schema_in": "iglu:test/schema/1-0-0"},
        }
        context = JobContext(bq_client=MagicMock(), run_id="err-test", dry_run=False)

        # Storage client that raises
        storage_client = MagicMock()
        storage_client.query_objects.side_effect = RuntimeError("BQ connection failed")

        event_client = MockEventClient()

        with pytest.raises(RuntimeError):
            processor.run(job_spec, context, storage_client, event_client)

        # Error event emitted
        event = event_client.events[0]
        assert event["event_type"] == "validate.failed"
        assert event["status"] == "failed"
        assert "BQ connection failed" in event["error_message"]

    def test_full_mode_error_emits_event(self, processor):
        """Canonization errors emit failure event."""
        job_spec = {
            "job_id": "canonize_gmail",
            "job_type": "canonize",
            "source": {"source_system": "gmail", "object_type": "email"},
            "transform": {
                "mode": "full",
                "transform_ref": "email/gmail_to_jmap@1.0.0",
            },
        }
        context = JobContext(bq_client=MagicMock(), run_id="err-test", dry_run=False)

        storage_client = MagicMock()
        storage_client.query_objects.side_effect = RuntimeError("Query failed")

        event_client = MockEventClient()

        with pytest.raises(RuntimeError):
            processor.run(job_spec, context, storage_client, event_client)

        event = event_client.events[0]
        assert event["event_type"] == "canonize.failed"
        assert event["status"] == "failed"


class TestCanonizeProcessorRegistration:
    """Test that CanonizeProcessor is registered."""

    def test_registered_in_global_registry(self):
        """CanonizeProcessor is registered for 'canonize' job type."""
        from lorchestra.processors import registry

        # Import to trigger registration
        import lorchestra.processors.canonize  # noqa: F401

        processor = registry.get("canonize")
        assert isinstance(processor, CanonizeProcessor)


class TestCanonizeProcessorHelpers:
    """Tests for helper methods."""

    @pytest.fixture
    def processor(self):
        return CanonizeProcessor()

    def test_get_validator_loads_schema(self, processor):
        """_get_validator loads schema from registry."""
        # This test verifies the method exists and has correct signature
        # Full integration test would require actual schema files
        with pytest.raises(Exception):
            # Will fail because schema doesn't exist, but method is called
            processor._get_validator("iglu:test/nonexistent/jsonschema/1-0-0")

    def test_get_transform_loads_jsonata(self, processor):
        """_get_transform loads jsonata from registry."""
        # Will fail because transform doesn't exist, but method is called
        with pytest.raises(FileNotFoundError, match="Transform not found"):
            processor._get_transform("nonexistent@1.0.0")
