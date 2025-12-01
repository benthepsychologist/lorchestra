"""Tests for JobRunner."""

import json
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from lorchestra.job_runner import (
    BigQueryStorageClient,
    BigQueryEventClient,
    load_job_spec,
    run_job,
)
from lorchestra.processors.base import JobContext


class TestLoadJobSpec:
    """Tests for load_job_spec function."""

    def test_load_valid_spec(self):
        """Load a valid job spec from file."""
        with TemporaryDirectory() as tmpdir:
            specs_dir = Path(tmpdir)
            spec_file = specs_dir / "test_job.json"
            spec_file.write_text(json.dumps({
                "job_id": "test_job",
                "job_type": "ingest",
                "source": {"stream": "test.stream"},
            }))

            spec = load_job_spec("test_job", specs_dir=specs_dir)

            assert spec["job_id"] == "test_job"
            assert spec["job_type"] == "ingest"
            assert spec["source"]["stream"] == "test.stream"

    def test_load_spec_adds_job_id_if_missing(self):
        """Job ID is added from filename if not in spec."""
        with TemporaryDirectory() as tmpdir:
            specs_dir = Path(tmpdir)
            spec_file = specs_dir / "my_job.json"
            spec_file.write_text(json.dumps({
                "job_type": "ingest",
            }))

            spec = load_job_spec("my_job", specs_dir=specs_dir)

            assert spec["job_id"] == "my_job"

    def test_load_nonexistent_spec_raises(self):
        """Loading nonexistent spec raises FileNotFoundError."""
        with TemporaryDirectory() as tmpdir:
            specs_dir = Path(tmpdir)

            with pytest.raises(FileNotFoundError, match="Job spec not found"):
                load_job_spec("nonexistent", specs_dir=specs_dir)

    def test_load_invalid_json_raises(self):
        """Loading invalid JSON raises JSONDecodeError."""
        with TemporaryDirectory() as tmpdir:
            specs_dir = Path(tmpdir)
            spec_file = specs_dir / "bad_spec.json"
            spec_file.write_text("{ invalid json }")

            with pytest.raises(json.JSONDecodeError):
                load_job_spec("bad_spec", specs_dir=specs_dir)


class TestBigQueryStorageClient:
    """Tests for BigQueryStorageClient."""

    @pytest.fixture
    def mock_bq_client(self):
        return MagicMock()

    @pytest.fixture
    def storage_client(self, mock_bq_client):
        return BigQueryStorageClient(mock_bq_client, "test_dataset")

    def test_table_name_production(self, storage_client):
        """Production mode uses original table names."""
        assert storage_client._table_name("raw_objects") == "raw_objects"
        assert storage_client._table_name("canonical_objects") == "canonical_objects"

    def test_table_name_test_mode(self, mock_bq_client):
        """Test mode prefixes table names."""
        client = BigQueryStorageClient(mock_bq_client, "test_dataset", test_table=True)
        assert client._table_name("raw_objects") == "test_raw_objects"
        assert client._table_name("canonical_objects") == "test_canonical_objects"

    def test_query_objects_builds_correct_query(self, storage_client, mock_bq_client):
        """query_objects builds correct BigQuery query."""
        # Setup mock to return empty result
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_bq_client.query.return_value.result.return_value = mock_result

        # Execute
        list(storage_client.query_objects(
            source_system="gmail",
            object_type="email",
            filters={"validation_status": "pass"},
            limit=100,
        ))

        # Verify query was called
        assert mock_bq_client.query.called
        query = mock_bq_client.query.call_args[0][0]

        assert "source_system = @source_system" in query
        assert "object_type = @object_type" in query
        assert "validation_status = @filter_0" in query
        assert "LIMIT 100" in query

    def test_query_objects_handles_null_filter(self, storage_client, mock_bq_client):
        """query_objects handles NULL filter values."""
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_bq_client.query.return_value.result.return_value = mock_result

        list(storage_client.query_objects(
            source_system="gmail",
            object_type="email",
            filters={"validation_status": None},
        ))

        query = mock_bq_client.query.call_args[0][0]
        assert "validation_status IS NULL" in query

    def test_update_field_builds_correct_query(self, storage_client, mock_bq_client):
        """update_field builds correct UPDATE query."""
        mock_result = MagicMock()
        mock_result.num_dml_affected_rows = 5
        mock_bq_client.query.return_value.result.return_value = mock_result

        result = storage_client.update_field(
            idem_keys=["key1", "key2"],
            field="validation_status",
            value="pass",
        )

        assert result == 5
        query = mock_bq_client.query.call_args[0][0]
        assert "UPDATE" in query
        assert "SET validation_status = @value" in query
        assert "WHERE idem_key IN UNNEST(@idem_keys)" in query

    def test_update_field_empty_keys_returns_zero(self, storage_client, mock_bq_client):
        """update_field with empty keys returns 0 without query."""
        result = storage_client.update_field([], "field", "value")
        assert result == 0
        assert not mock_bq_client.query.called

    def test_insert_canonical_builds_correct_rows(self, storage_client, mock_bq_client):
        """insert_canonical builds correct row structure."""
        mock_bq_client.insert_rows_json.return_value = []  # No errors

        objects = [
            {
                "idem_key": "key1",
                "source_system": "gmail",
                "payload": {"canonical": "data"},
                "canonical_schema": "iglu:test/schema/1-0-0",
                "transform_ref": "test_transform@1.0.0",
            }
        ]

        result = storage_client.insert_canonical(objects, "corr-123")

        assert result == 1
        assert mock_bq_client.insert_rows_json.called

        # Check row structure
        rows = mock_bq_client.insert_rows_json.call_args[0][1]
        assert len(rows) == 1
        assert rows[0]["idem_key"] == "key1"
        assert rows[0]["canonical_schema"] == "iglu:test/schema/1-0-0"
        assert rows[0]["correlation_id"] == "corr-123"

    def test_insert_canonical_empty_returns_zero(self, storage_client, mock_bq_client):
        """insert_canonical with empty list returns 0."""
        result = storage_client.insert_canonical([], "corr-123")
        assert result == 0
        assert not mock_bq_client.insert_rows_json.called


class TestBigQueryEventClient:
    """Tests for BigQueryEventClient."""

    def test_log_event_delegates_to_event_client(self):
        """log_event delegates to event_client module."""
        mock_bq_client = MagicMock()
        client = BigQueryEventClient(mock_bq_client)

        with patch("lorchestra.job_runner.ec.log_event") as mock_log_event:
            client.log_event(
                event_type="test.event",
                source_system="test",
                correlation_id="corr-123",
                status="ok",
                payload={"key": "value"},
            )

            mock_log_event.assert_called_once_with(
                event_type="test.event",
                source_system="test",
                correlation_id="corr-123",
                status="ok",
                connection_name=None,
                target_object_type=None,
                payload={"key": "value"},
                error_message=None,
                bq_client=mock_bq_client,
            )


class TestRunJob:
    """Tests for run_job function."""

    @pytest.fixture
    def mock_bq_client(self):
        return MagicMock()

    @pytest.fixture
    def specs_dir(self):
        """Create temporary specs directory with test job."""
        with TemporaryDirectory() as tmpdir:
            specs_dir = Path(tmpdir)
            spec_file = specs_dir / "test_ingest.json"
            spec_file.write_text(json.dumps({
                "job_id": "test_ingest",
                "job_type": "ingest",
                "source": {
                    "stream": "test.stream",
                    "identity": "test:identity",
                },
                "sink": {
                    "source_system": "test",
                    "connection_name": "test-conn",
                    "object_type": "item",
                },
            }))
            yield specs_dir

    def test_run_job_loads_spec_and_dispatches(self, specs_dir, mock_bq_client):
        """run_job loads spec and dispatches to processor."""
        mock_processor = MagicMock()

        with patch("lorchestra.job_runner.registry") as mock_registry:
            with patch("lorchestra.job_runner.ec"):
                mock_registry.get.return_value = mock_processor

                run_job(
                    "test_ingest",
                    specs_dir=specs_dir,
                    bq_client=mock_bq_client,
                    dry_run=True,
                )

                # Verify processor was called
                mock_processor.run.assert_called_once()

                # Verify job_spec was passed
                call_args = mock_processor.run.call_args
                job_spec = call_args[0][0]
                assert job_spec["job_id"] == "test_ingest"
                assert job_spec["job_type"] == "ingest"

                # Verify context was passed
                context = call_args[0][1]
                assert isinstance(context, JobContext)
                assert context.dry_run is True

    def test_run_job_emits_lifecycle_events(self, specs_dir, mock_bq_client):
        """run_job emits job.started and job.completed events."""
        mock_processor = MagicMock()

        with patch("lorchestra.job_runner.registry") as mock_registry:
            with patch("lorchestra.job_runner.ec") as mock_ec:
                mock_registry.get.return_value = mock_processor

                run_job(
                    "test_ingest",
                    specs_dir=specs_dir,
                    bq_client=mock_bq_client,
                )

                # Check log_event was called for started and completed
                log_event_calls = [c for c in mock_ec.log_event.call_args_list]
                event_types = [c.kwargs.get("event_type") for c in log_event_calls]

                assert "job.started" in event_types
                assert "job.completed" in event_types

    def test_run_job_emits_failed_event_on_error(self, specs_dir, mock_bq_client):
        """run_job emits job.failed event on processor error."""
        mock_processor = MagicMock()
        mock_processor.run.side_effect = RuntimeError("Processor error")

        with patch("lorchestra.job_runner.registry") as mock_registry:
            with patch("lorchestra.job_runner.ec") as mock_ec:
                mock_registry.get.return_value = mock_processor

                with pytest.raises(RuntimeError, match="Processor error"):
                    run_job(
                        "test_ingest",
                        specs_dir=specs_dir,
                        bq_client=mock_bq_client,
                    )

                # Check job.failed event was emitted
                log_event_calls = [c for c in mock_ec.log_event.call_args_list]
                event_types = [c.kwargs.get("event_type") for c in log_event_calls]

                assert "job.started" in event_types
                assert "job.failed" in event_types
                assert "job.completed" not in event_types

    def test_run_job_sets_run_mode(self, specs_dir, mock_bq_client):
        """run_job sets and resets run mode."""
        mock_processor = MagicMock()

        with patch("lorchestra.job_runner.registry") as mock_registry:
            with patch("lorchestra.job_runner.ec") as mock_ec:
                mock_registry.get.return_value = mock_processor

                run_job(
                    "test_ingest",
                    specs_dir=specs_dir,
                    bq_client=mock_bq_client,
                    dry_run=True,
                    test_table=True,
                )

                # Verify set_run_mode was called
                mock_ec.set_run_mode.assert_called_once_with(dry_run=True, test_table=True)

                # Verify reset_run_mode was called
                mock_ec.reset_run_mode.assert_called_once()

    def test_run_job_missing_job_type_raises(self, mock_bq_client):
        """run_job raises ValueError if job_type is missing."""
        with TemporaryDirectory() as tmpdir:
            specs_dir = Path(tmpdir)
            spec_file = specs_dir / "bad_job.json"
            spec_file.write_text(json.dumps({
                "job_id": "bad_job",
                # Missing job_type
            }))

            with patch("lorchestra.job_runner.ec"):
                with pytest.raises(ValueError, match="missing job_type"):
                    run_job("bad_job", specs_dir=specs_dir, bq_client=mock_bq_client)

    def test_run_job_unknown_job_type_raises(self, mock_bq_client):
        """run_job raises KeyError for unknown job_type."""
        with TemporaryDirectory() as tmpdir:
            specs_dir = Path(tmpdir)
            spec_file = specs_dir / "unknown_type.json"
            spec_file.write_text(json.dumps({
                "job_id": "unknown_type",
                "job_type": "nonexistent_type",
            }))

            with patch("lorchestra.job_runner.ec"):
                with pytest.raises(KeyError, match="Unknown job_type"):
                    run_job("unknown_type", specs_dir=specs_dir, bq_client=mock_bq_client)
