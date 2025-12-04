"""Tests for FinalFormProcessor."""

import pytest
from unittest.mock import MagicMock, patch
from typing import Any, Iterator

from lorchestra.processors.base import JobContext, UpsertResult
from lorchestra.processors.final_form import FinalFormProcessor


class MockSource:
    """Mock Source for MeasurementEvent."""

    def __init__(
        self,
        form_id: str = "googleforms::test-form",
        form_submission_id: str = "sub-001",
        binding_id: str = "intake_01",
        binding_version: str = "1.0.0",
    ):
        self.form_id = form_id
        self.form_submission_id = form_submission_id
        self.form_correlation_id = None
        self.binding_id = binding_id
        self.binding_version = binding_version


class MockTelemetry:
    """Mock Telemetry for MeasurementEvent."""

    def __init__(self):
        self.processed_at = "2025-12-03T00:00:00+00:00"
        self.final_form_version = "1.0.0"
        self.measure_spec = "phq9@1.0.0"
        self.form_binding_spec = "intake_01@1.0.0"
        self.warnings = []


class MockObservation:
    """Mock Observation for testing."""

    def __init__(
        self,
        observation_id: str = "obs-001",
        measure_id: str = "phq9",
        code: str = "q1",
        kind: str = "item",
        value: int | float | None = 1,
        value_type: str = "integer",
        label: str | None = None,
        raw_answer: str | None = "A little bit",
        position: int | None = 1,
        missing: bool = False,
    ):
        self.observation_id = observation_id
        self.measure_id = measure_id
        self.code = code
        self.kind = kind
        self.value = value
        self.value_type = value_type
        self.label = label
        self.raw_answer = raw_answer
        self.position = position
        self.missing = missing


class MockMeasurementEvent:
    """Mock MeasurementEvent for testing."""

    def __init__(
        self,
        measurement_event_id: str = "me-001",
        measure_id: str = "phq9",
        measure_version: str = "1.0.0",
        subject_id: str = "user-123",
        timestamp: str = "2025-12-03T12:00:00Z",
        observations: list[MockObservation] | None = None,
        source: MockSource | None = None,
        telemetry: MockTelemetry | None = None,
    ):
        self.measurement_event_id = measurement_event_id
        self.measure_id = measure_id
        self.measure_version = measure_version
        self.subject_id = subject_id
        self.timestamp = timestamp
        self.observations = observations or [
            MockObservation(code="q1", value=1, position=1),
            MockObservation(code="q2", value=2, position=2),
            MockObservation(code="total", kind="scale", value=3, label="minimal", position=None),
        ]
        self.source = source or MockSource()
        self.telemetry = telemetry or MockTelemetry()


class MockDiagnostics:
    """Mock diagnostics for ProcessingResult."""

    def __init__(self, errors: list | None = None, warnings: list | None = None):
        self.errors = errors or []
        self.warnings = warnings or []


class MockProcessingResult:
    """Mock ProcessingResult for testing."""

    def __init__(
        self,
        form_submission_id: str = "sub-001",
        events: list[MockMeasurementEvent] | None = None,
        diagnostics: MockDiagnostics | None = None,
        success: bool = True,
    ):
        self.form_submission_id = form_submission_id
        self.events = events if events is not None else [MockMeasurementEvent()]
        self.diagnostics = diagnostics or MockDiagnostics()
        self.success = success


class MockBindingSpec:
    """Mock binding spec from Pipeline."""

    def __init__(self, binding_id: str = "intake_01", version: str = "1.0.0"):
        self.binding_id = binding_id
        self.version = version


class MockPipeline:
    """Mock Pipeline for testing."""

    def __init__(
        self,
        binding_id: str = "intake_01",
        binding_version: str = "1.0.0",
        process_result: MockProcessingResult | None = None,
        fail: bool = False,
        fail_message: str = "Processing failed",
    ):
        self.binding_spec = MockBindingSpec(binding_id, binding_version)
        self.process_result = process_result or MockProcessingResult()
        self.fail = fail
        self.fail_message = fail_message
        self.process_calls = []

    def process(self, form_response: dict[str, Any]) -> MockProcessingResult:
        self.process_calls.append(form_response)
        if self.fail:
            raise ValueError(self.fail_message)
        return self.process_result


class MockStorageClient:
    """Mock storage client for testing."""

    def __init__(
        self,
        canonical_records: list[dict[str, Any]] | None = None,
        measurements_upserted: int = 0,
        observations_upserted: int = 0,
    ):
        self.canonical_records = canonical_records or []
        self.measurements_upserted = measurements_upserted
        self.observations_upserted = observations_upserted
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

    def upsert_measurements(
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
        return self.measurements_upserted or len(measurements)

    def upsert_observations(
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
        return self.observations_upserted or len(observations)

    # Required by protocol but not used in final_form
    def query_objects(self, *args, **kwargs):
        pass

    def upsert_objects(self, *args, **kwargs):
        return UpsertResult(inserted=0, updated=0)

    def update_field(self, *args, **kwargs):
        return 0

    def insert_canonical(self, *args, **kwargs):
        return 0

    def insert_measurements(self, *args, **kwargs):
        return 0

    def insert_observations(self, *args, **kwargs):
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


class TestFinalFormProcessor:
    """Tests for FinalFormProcessor."""

    @pytest.fixture
    def processor(self):
        return FinalFormProcessor()

    @pytest.fixture
    def job_spec(self):
        return {
            "job_id": "form_intake_01",
            "job_type": "final_form",
            "source": {
                "filter": {
                    "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
                    "form_id": "googleforms::test-form",
                },
            },
            "transform": {
                "binding_id": "intake_01",
                "binding_version": "1.0.0",
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

    def test_successful_formation(self, processor, job_spec, context):
        """Successful formation creates measurements and observations."""
        records = [
            {
                "idem_key": "key1",
                "payload": {
                    "form_id": "googleforms::test-form",
                    "submission_id": "sub-001",
                    "submitted_at": "2025-12-03T12:00:00Z",
                    "respondent": {"id": "user-123"},
                    "items": [
                        {"field_key": "q1", "answer": "A little bit"},
                        {"field_key": "q2", "answer": "Often"},
                    ],
                },
                "correlation_id": "corr1",
            },
            {
                "idem_key": "key2",
                "payload": {
                    "form_id": "googleforms::test-form",
                    "submission_id": "sub-002",
                    "submitted_at": "2025-12-03T12:01:00Z",
                    "respondent": {"id": "user-456"},
                    "items": [
                        {"field_key": "q1", "answer": "Not at all"},
                    ],
                },
                "correlation_id": "corr2",
            },
        ]
        storage_client = MockStorageClient(canonical_records=records)
        event_client = MockEventClient()
        mock_pipeline = MockPipeline()

        with patch.object(processor, "_create_pipeline", return_value=mock_pipeline):
            processor.run(job_spec, context, storage_client, event_client)

        # Verify canonical records were queried
        assert len(storage_client.query_calls) == 1
        query_call = storage_client.query_calls[0]
        assert query_call["canonical_schema"] == "iglu:org.canonical/form_response/jsonschema/1-0-0"
        assert query_call["filters"]["form_id"] == "googleforms::test-form"

        # Verify measurements upserted
        assert len(storage_client.measurement_calls) == 1
        m_call = storage_client.measurement_calls[0]
        assert m_call["table"] == "measurement_events"
        # 2 records x 1 measurement event each = 2 measurements
        assert len(m_call["measurements"]) == 2

        # Check measurement structure
        m1 = m_call["measurements"][0]
        assert m1["idem_key"] == "key1:phq9:measurement"
        assert m1["canonical_idem_key"] == "key1"
        assert m1["measure_id"] == "phq9"
        assert m1["measure_version"] == "1.0.0"
        assert m1["subject_id"] == "user-123"

        # Verify observations upserted
        assert len(storage_client.observation_calls) == 1
        o_call = storage_client.observation_calls[0]
        assert o_call["table"] == "observations"
        # 2 records x 3 observations each (2 items + 1 scale) = 6 observations
        assert len(o_call["observations"]) == 6

        # Verify events (started + completed)
        assert len(event_client.events) == 2
        started_event = event_client.events[0]
        assert started_event["event_type"] == "formation.started"
        assert started_event["source_system"] == "final_form"

        completed_event = event_client.events[1]
        assert completed_event["event_type"] == "formation.completed"
        assert completed_event["status"] == "success"
        assert completed_event["source_system"] == "final_form"
        assert completed_event["payload"]["measurements_created"] == 2
        assert completed_event["payload"]["observations_created"] == 6
        assert completed_event["payload"]["binding_id"] == "intake_01"
        assert completed_event["payload"]["binding_version"] == "1.0.0"
        assert "diagnostics" in completed_event["payload"]

    def test_no_records_returns_early(self, processor, job_spec, context):
        """No canonical records returns early after started event."""
        storage_client = MockStorageClient(canonical_records=[])
        event_client = MockEventClient()

        with patch.object(processor, "_create_pipeline", return_value=MockPipeline()):
            processor.run(job_spec, context, storage_client, event_client)

        # No upserts
        assert len(storage_client.measurement_calls) == 0
        assert len(storage_client.observation_calls) == 0

        # Only started event emitted (no completed since return early)
        assert len(event_client.events) == 1
        assert event_client.events[0]["event_type"] == "formation.started"

    def test_dry_run_mode(self, processor, job_spec):
        """Dry run processes but doesn't upsert."""
        context = JobContext(bq_client=MagicMock(), run_id="dry-test", dry_run=True)
        records = [{
            "idem_key": "key1",
            "payload": {
                "form_id": "test",
                "submission_id": "sub-001",
                "items": [],
            },
        }]
        storage_client = MockStorageClient(canonical_records=records)
        event_client = MockEventClient()

        with patch.object(processor, "_create_pipeline", return_value=MockPipeline()):
            processor.run(job_spec, context, storage_client, event_client)

        # No upserts in dry run
        assert len(storage_client.measurement_calls) == 0
        assert len(storage_client.observation_calls) == 0

        # Completed event still emitted with dry_run flag
        completed_event = event_client.events[-1]
        assert completed_event["event_type"] == "formation.completed"
        assert completed_event["payload"]["dry_run"] is True
        assert completed_event["payload"]["measurements_upserted"] == 0
        assert completed_event["payload"]["observations_upserted"] == 0
        # But measurements_created should still be counted
        assert completed_event["payload"]["measurements_created"] == 1

    def test_with_processing_failures(self, processor, job_spec, context):
        """Processing failures are tracked but don't stop other records."""
        records = [
            {"idem_key": "key1", "payload": {"items": []}},
            {"idem_key": "key2", "payload": {"items": []}},
        ]
        storage_client = MockStorageClient(canonical_records=records)
        event_client = MockEventClient()

        # Pipeline that fails on second call
        call_count = [0]

        def failing_process(form_response):
            call_count[0] += 1
            if call_count[0] == 2:
                raise ValueError("Processing error")
            return MockProcessingResult()

        mock_pipeline = MockPipeline()
        mock_pipeline.process = failing_process

        with patch.object(processor, "_create_pipeline", return_value=mock_pipeline):
            processor.run(job_spec, context, storage_client, event_client)

        # Only 1 measurement upserted (first record succeeded)
        m_call = storage_client.measurement_calls[0]
        assert len(m_call["measurements"]) == 1

        # Event shows partial success
        completed_event = event_client.events[-1]
        assert completed_event["payload"]["measurements_created"] == 1
        assert completed_event["payload"]["records_failed"] == 1

    def test_custom_event_names(self, processor, job_spec, context):
        """Custom event names from job_spec are used."""
        job_spec["events"] = {
            "on_started": "intake.starting",
            "on_complete": "intake.scored",
            "on_fail": "intake.error",
        }
        records = [{"idem_key": "key1", "payload": {"items": []}}]
        storage_client = MockStorageClient(canonical_records=records)
        event_client = MockEventClient()

        with patch.object(processor, "_create_pipeline", return_value=MockPipeline()):
            processor.run(job_spec, context, storage_client, event_client)

        assert event_client.events[0]["event_type"] == "intake.starting"
        assert event_client.events[1]["event_type"] == "intake.scored"

    def test_limit_option(self, processor, job_spec, context):
        """Limit option caps records processed."""
        job_spec["options"] = {"limit": 1}
        records = [
            {"idem_key": "key1", "payload": {"items": []}},
            {"idem_key": "key2", "payload": {"items": []}},
            {"idem_key": "key3", "payload": {"items": []}},
        ]
        storage_client = MockStorageClient(canonical_records=records)
        event_client = MockEventClient()

        with patch.object(processor, "_create_pipeline", return_value=MockPipeline()):
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
            "job_id": "form_intake_01",
            "job_type": "final_form",
            "source": {"filter": {"canonical_schema": "test"}},
            "transform": {"binding_id": "intake_01"},
        }
        context = JobContext(bq_client=MagicMock(), run_id="err-test", dry_run=False)

        storage_client = MagicMock()
        storage_client.query_canonical.side_effect = RuntimeError("BQ connection failed")

        event_client = MockEventClient()

        with pytest.raises(RuntimeError):
            with patch.object(processor, "_create_pipeline", return_value=MockPipeline()):
                processor.run(job_spec, context, storage_client, event_client)

        # Started event emitted, then error event
        assert len(event_client.events) == 2
        assert event_client.events[0]["event_type"] == "formation.started"

        error_event = event_client.events[1]
        assert error_event["event_type"] == "formation.failed"
        assert error_event["status"] == "failed"
        assert "BQ connection failed" in error_event["error_message"]
        assert error_event["payload"]["error_type"] == "RuntimeError"

    def test_pipeline_creation_failure(self, processor):
        """Pipeline creation failure emits failure event."""
        job_spec = {
            "job_id": "form_intake_01",
            "job_type": "final_form",
            "source": {"filter": {}},
            "transform": {"binding_id": "nonexistent"},
        }
        context = JobContext(bq_client=MagicMock(), run_id="err-test", dry_run=False)
        storage_client = MockStorageClient()
        event_client = MockEventClient()

        with pytest.raises(FileNotFoundError):
            with patch.object(
                processor,
                "_create_pipeline",
                side_effect=FileNotFoundError("Binding not found: nonexistent"),
            ):
                processor.run(job_spec, context, storage_client, event_client)

        error_event = event_client.events[-1]
        assert error_event["event_type"] == "formation.failed"
        assert "Binding not found" in error_event["error_message"]


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

    def test_build_form_response(self, processor):
        """_build_form_response correctly maps canonical payload."""
        record = {"idem_key": "key1"}
        payload = {
            "form_id": "googleforms::abc123",
            "submission_id": "sub-001",
            "submitted_at": "2025-12-03T12:00:00Z",
            "respondent": {"id": "user-123"},
            "items": [
                {"field_key": "q1", "answer": "A little bit"},
                {"field_key": "q2", "answer": "Often"},
            ],
        }

        result = processor._build_form_response(record, payload)

        assert result["form_id"] == "googleforms::abc123"
        assert result["form_submission_id"] == "sub-001"
        assert result["subject_id"] == "user-123"
        assert result["timestamp"] == "2025-12-03T12:00:00Z"
        assert len(result["items"]) == 2
        assert result["items"][0] == {"field_key": "q1", "answer": "A little bit"}
        assert result["items"][1] == {"field_key": "q2", "answer": "Often"}

    def test_build_form_response_missing_respondent(self, processor):
        """_build_form_response handles missing respondent."""
        record = {"idem_key": "key1"}
        payload = {"form_id": "test", "items": []}

        result = processor._build_form_response(record, payload)

        assert result["subject_id"] is None

    def test_measurement_event_to_storage(self, processor):
        """_measurement_event_to_storage correctly transforms MeasurementEvent."""
        event = MockMeasurementEvent(
            measurement_event_id="me-001",
            measure_id="phq9",
            measure_version="1.0.0",
            subject_id="user-123",
            timestamp="2025-12-03T12:00:00Z",
        )

        result = processor._measurement_event_to_storage(
            event=event,
            canonical_idem_key="canon-key-1",
            correlation_id="corr-001",
        )

        assert result["idem_key"] == "canon-key-1:phq9:measurement"
        assert result["canonical_idem_key"] == "canon-key-1"
        assert result["measurement_event_id"] == "me-001"
        assert result["measure_id"] == "phq9"
        assert result["measure_version"] == "1.0.0"
        assert result["subject_id"] == "user-123"
        assert result["timestamp"] == "2025-12-03T12:00:00Z"
        assert result["binding_id"] == "intake_01"
        assert result["binding_version"] == "1.0.0"
        assert result["correlation_id"] == "corr-001"
        assert "processed_at" in result

    def test_observation_to_storage_item(self, processor):
        """_observation_to_storage correctly transforms item Observation."""
        obs = MockObservation(
            observation_id="obs-001",
            measure_id="phq9",
            code="q1",
            kind="item",
            value=2,
            value_type="integer",
            raw_answer="A little bit",
            position=1,
            missing=False,
        )

        result = processor._observation_to_storage(
            obs=obs,
            measurement_idem_key="me-key:phq9:measurement",
            correlation_id="corr-001",
        )

        assert result["idem_key"] == "me-key:phq9:measurement:obs:q1"
        assert result["measurement_idem_key"] == "me-key:phq9:measurement"
        assert result["observation_id"] == "obs-001"
        assert result["measure_id"] == "phq9"
        assert result["code"] == "q1"
        assert result["kind"] == "item"
        assert result["value"] == 2
        assert result["value_type"] == "integer"
        assert result["raw_answer"] == "A little bit"
        assert result["position"] == 1
        assert result["missing"] is False
        assert result["correlation_id"] == "corr-001"

    def test_observation_to_storage_scale(self, processor):
        """_observation_to_storage correctly transforms scale Observation."""
        obs = MockObservation(
            observation_id="obs-scale",
            measure_id="phq9",
            code="total",
            kind="scale",
            value=15.0,
            value_type="float",
            label="moderate",
            raw_answer=None,
            position=None,
            missing=False,
        )

        result = processor._observation_to_storage(
            obs=obs,
            measurement_idem_key="me-key:phq9:measurement",
            correlation_id="corr-001",
        )

        assert result["idem_key"] == "me-key:phq9:measurement:obs:total"
        assert result["code"] == "total"
        assert result["kind"] == "scale"
        assert result["value"] == 15.0
        assert result["label"] == "moderate"
        assert result["raw_answer"] is None
        assert result["position"] is None

    def test_create_pipeline_called_with_config(self, processor):
        """_create_pipeline creates Pipeline with correct config."""
        from pathlib import Path

        with patch("lorchestra.processors.final_form.Pipeline") as MockPipelineCls:
            with patch("lorchestra.processors.final_form.PipelineConfig") as MockConfigCls:
                MockConfigCls.return_value = MagicMock()
                MockPipelineCls.return_value = MockPipeline()

                processor._create_pipeline(
                    binding_id="intake_01",
                    binding_version="1.0.0",
                    measure_registry_path=Path("/test/measures"),
                    binding_registry_path=Path("/test/bindings"),
                )

                MockConfigCls.assert_called_once_with(
                    measure_registry_path=Path("/test/measures"),
                    binding_registry_path=Path("/test/bindings"),
                    binding_id="intake_01",
                    binding_version="1.0.0",
                )
                MockPipelineCls.assert_called_once()
