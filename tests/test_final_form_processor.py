"""Tests for FinalFormProcessor."""

import pytest
from unittest.mock import MagicMock, patch
from typing import Any, Iterator

from lorchestra.processors.base import JobContext, UpsertResult
from lorchestra.processors.final_form import FinalFormProcessor


class MockStorageClient:
    """Mock storage client for testing."""

    def __init__(
        self,
        canonical_records: list[dict[str, Any]] | None = None,
        measurements_inserted: int = 0,
        observations_inserted: int = 0,
    ):
        self.canonical_records = canonical_records or []
        self.measurements_inserted = measurements_inserted
        self.observations_inserted = observations_inserted
        self.query_calls = []
        self.measurement_calls = []
        self.observation_calls = []

    def query_canonical(
        self,
        canonical_schema: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        self.query_calls.append({
            "canonical_schema": canonical_schema,
            "filters": filters,
            "limit": limit,
        })
        for record in self.canonical_records[:limit] if limit else self.canonical_records:
            yield record

    def insert_measurements(
        self,
        measurements: list[dict[str, Any]],
        table: str,
        correlation_id: str,
    ) -> int:
        self.measurement_calls.append({
            "measurements": measurements,
            "table": table,
            "correlation_id": correlation_id,
        })
        return self.measurements_inserted or len(measurements)

    def insert_observations(
        self,
        observations: list[dict[str, Any]],
        table: str,
        correlation_id: str,
    ) -> int:
        self.observation_calls.append({
            "observations": observations,
            "table": table,
            "correlation_id": correlation_id,
        })
        return self.observations_inserted or len(observations)

    # Required by protocol but not used in final_form
    def query_objects(self, *args, **kwargs):
        pass

    def upsert_objects(self, *args, **kwargs):
        return UpsertResult(inserted=0, updated=0)

    def update_field(self, *args, **kwargs):
        return 0

    def insert_canonical(self, *args, **kwargs):
        return 0


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


class MockFinalizer:
    """Mock finalizer for testing."""

    def __init__(
        self,
        score: float = 10.0,
        interpretation: str = "mild",
        observations: list[dict[str, Any]] | None = None,
        fail: bool = False,
    ):
        self.score = score
        self.interpretation = interpretation
        self.observations = observations or [
            {"item_id": "q1", "response_value": 1, "score_value": 1},
            {"item_id": "q2", "response_value": 2, "score_value": 2},
        ]
        self.fail = fail
        self.process_calls = []

    def process(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.process_calls.append(payload)
        if self.fail:
            raise ValueError("Finalization failed")
        return {
            "score": self.score,
            "interpretation": self.interpretation,
            "observations": self.observations,
        }


class TestFinalFormProcessor:
    """Tests for FinalFormProcessor."""

    @pytest.fixture
    def processor(self):
        return FinalFormProcessor()

    @pytest.fixture
    def job_spec(self):
        return {
            "job_id": "final_form_phq9",
            "job_type": "final_form",
            "source": {
                "table": "canonical_objects",
                "filter": {
                    "canonical_schema": "iglu:canonical/questionnaire_response/jsonschema/1-0-0",
                    "instrument_id": "phq-9",
                },
            },
            "transform": {
                "instrument_id": "phq-9",
                "instrument_version": "1.0.0",
                "binding_id": "intake_v1",
                "finalform_spec": "phq9_finalform@1.0.0",
            },
            "sink": {
                "measurement_table": "measurement_events",
                "observation_table": "observations",
            },
        }

    @pytest.fixture
    def context(self):
        return JobContext(
            bq_client=MagicMock(),
            run_id="test-run-789",
            dry_run=False,
        )

    def test_successful_finalization(self, processor, job_spec, context):
        """Successful finalization creates measurements and observations."""
        records = [
            {"idem_key": "key1", "payload": {"response": [1, 2, 3]}, "correlation_id": "corr1"},
            {"idem_key": "key2", "payload": {"response": [2, 3, 4]}, "correlation_id": "corr2"},
        ]
        storage_client = MockStorageClient(canonical_records=records)
        event_client = MockEventClient()
        mock_finalizer = MockFinalizer()

        with patch.object(processor, "_get_finalizer", return_value=mock_finalizer):
            processor.run(job_spec, context, storage_client, event_client)

        # Verify canonical records were queried
        assert len(storage_client.query_calls) == 1
        query_call = storage_client.query_calls[0]
        assert query_call["canonical_schema"] == "iglu:canonical/questionnaire_response/jsonschema/1-0-0"
        assert query_call["filters"]["instrument_id"] == "phq-9"

        # Verify measurements inserted
        assert len(storage_client.measurement_calls) == 1
        m_call = storage_client.measurement_calls[0]
        assert m_call["table"] == "measurement_events"
        assert len(m_call["measurements"]) == 2

        # Check measurement structure
        m1 = m_call["measurements"][0]
        assert m1["idem_key"] == "key1:measurement"
        assert m1["canonical_idem_key"] == "key1"
        assert m1["instrument_id"] == "phq-9"
        assert m1["score"] == 10.0
        assert m1["score_interpretation"] == "mild"

        # Verify observations inserted
        assert len(storage_client.observation_calls) == 1
        o_call = storage_client.observation_calls[0]
        assert o_call["table"] == "observations"
        # 2 records x 2 observations each = 4 observations
        assert len(o_call["observations"]) == 4

        # Verify completion event
        assert len(event_client.events) == 1
        event = event_client.events[0]
        assert event["event_type"] == "finalization.completed"
        assert event["status"] == "ok"
        assert event["source_system"] == "phq-9"
        assert event["payload"]["measurements_created"] == 2
        assert event["payload"]["observations_created"] == 4

    def test_no_records_returns_early(self, processor, job_spec, context):
        """No canonical records returns early without event."""
        storage_client = MockStorageClient(canonical_records=[])
        event_client = MockEventClient()

        with patch.object(processor, "_get_finalizer", return_value=MockFinalizer()):
            processor.run(job_spec, context, storage_client, event_client)

        # No inserts or events
        assert len(storage_client.measurement_calls) == 0
        assert len(storage_client.observation_calls) == 0
        assert len(event_client.events) == 0

    def test_dry_run_mode(self, processor, job_spec):
        """Dry run processes but doesn't insert."""
        context = JobContext(bq_client=MagicMock(), run_id="dry-test", dry_run=True)
        records = [{"idem_key": "key1", "payload": {"response": [1]}}]
        storage_client = MockStorageClient(canonical_records=records)
        event_client = MockEventClient()

        with patch.object(processor, "_get_finalizer", return_value=MockFinalizer()):
            processor.run(job_spec, context, storage_client, event_client)

        # No inserts in dry run
        assert len(storage_client.measurement_calls) == 0
        assert len(storage_client.observation_calls) == 0

        # Event still emitted with dry_run flag
        event = event_client.events[0]
        assert event["payload"]["dry_run"] is True
        assert event["payload"]["measurements_inserted"] == 0
        assert event["payload"]["observations_inserted"] == 0

    def test_with_processing_failures(self, processor, job_spec, context):
        """Processing failures are tracked but don't stop other records."""
        records = [
            {"idem_key": "key1", "payload": {"response": [1]}},
            {"idem_key": "key2", "payload": {"response": [2]}},
        ]
        storage_client = MockStorageClient(canonical_records=records)
        event_client = MockEventClient()

        # Finalizer that fails on second call
        call_count = [0]
        def failing_process(payload):
            call_count[0] += 1
            if call_count[0] == 2:
                raise ValueError("Processing error")
            return {"score": 5.0, "interpretation": "minimal", "observations": []}

        mock_finalizer = MockFinalizer()
        mock_finalizer.process = failing_process

        with patch.object(processor, "_get_finalizer", return_value=mock_finalizer):
            processor.run(job_spec, context, storage_client, event_client)

        # Only 1 measurement inserted
        m_call = storage_client.measurement_calls[0]
        assert len(m_call["measurements"]) == 1

        # Event shows partial success
        event = event_client.events[0]
        assert event["payload"]["measurements_created"] == 1
        assert event["payload"]["failed"] == 1

    def test_custom_event_names(self, processor, job_spec, context):
        """Custom event names from job_spec are used."""
        job_spec["events"] = {
            "on_complete": "phq9.scored",
            "on_fail": "phq9.error",
        }
        records = [{"idem_key": "key1", "payload": {}}]
        storage_client = MockStorageClient(canonical_records=records)
        event_client = MockEventClient()

        with patch.object(processor, "_get_finalizer", return_value=MockFinalizer()):
            processor.run(job_spec, context, storage_client, event_client)

        assert event_client.events[0]["event_type"] == "phq9.scored"

    def test_limit_option(self, processor, job_spec, context):
        """Limit option caps records processed."""
        job_spec["options"] = {"limit": 1}
        records = [
            {"idem_key": "key1", "payload": {}},
            {"idem_key": "key2", "payload": {}},
            {"idem_key": "key3", "payload": {}},
        ]
        storage_client = MockStorageClient(canonical_records=records)
        event_client = MockEventClient()

        with patch.object(processor, "_get_finalizer", return_value=MockFinalizer()):
            processor.run(job_spec, context, storage_client, event_client)

        # Query passed limit
        assert storage_client.query_calls[0]["limit"] == 1

        # Only 1 measurement (storage_client respects limit)
        m_call = storage_client.measurement_calls[0]
        assert len(m_call["measurements"]) == 1


class TestFinalFormProcessorErrors:
    """Tests for error handling."""

    @pytest.fixture
    def processor(self):
        return FinalFormProcessor()

    def test_error_emits_failure_event(self, processor):
        """Errors emit failure event with details."""
        job_spec = {
            "job_id": "final_form_phq9",
            "job_type": "final_form",
            "source": {"filter": {"canonical_schema": "test"}},
            "transform": {"instrument_id": "phq-9", "finalform_spec": "test@1.0.0"},
        }
        context = JobContext(bq_client=MagicMock(), run_id="err-test", dry_run=False)

        storage_client = MagicMock()
        storage_client.query_canonical.side_effect = RuntimeError("BQ connection failed")

        event_client = MockEventClient()

        with pytest.raises(RuntimeError):
            processor.run(job_spec, context, storage_client, event_client)

        # Error event emitted
        event = event_client.events[0]
        assert event["event_type"] == "finalization.failed"
        assert event["status"] == "failed"
        assert "BQ connection failed" in event["error_message"]
        assert event["payload"]["error_type"] == "RuntimeError"


class TestFinalFormProcessorRegistration:
    """Test that FinalFormProcessor is registered."""

    def test_registered_in_global_registry(self):
        """FinalFormProcessor is registered for 'final_form' job type."""
        from lorchestra.processors import registry

        # Import to trigger registration
        import lorchestra.processors.final_form  # noqa: F401

        processor = registry.get("final_form")
        assert isinstance(processor, FinalFormProcessor)


class TestFinalFormProcessorHelpers:
    """Tests for helper methods."""

    @pytest.fixture
    def processor(self):
        return FinalFormProcessor()

    def test_get_finalizer_raises_file_not_found(self, processor):
        """_get_finalizer raises FileNotFoundError when spec doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Finalform spec not found"):
            processor._get_finalizer("phq9_finalform@1.0.0")

    def test_get_finalizer_parses_spec_reference(self, processor):
        """_get_finalizer parses name@version format and shows correct path."""
        with pytest.raises(FileNotFoundError) as exc_info:
            processor._get_finalizer("phq9_finalform@1.0.0")

        assert "phq9_finalform" in str(exc_info.value)
        assert "1.0.0" in str(exc_info.value)

    def test_get_finalizer_default_version(self, processor):
        """_get_finalizer uses default version if not specified."""
        with pytest.raises(FileNotFoundError) as exc_info:
            processor._get_finalizer("phq9_finalform")

        # Should use 1.0.0 as default
        assert "1.0.0" in str(exc_info.value)
