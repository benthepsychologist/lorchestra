"""Tests for JobRunner."""

import json
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from lorchestra.job_runner import (
    ALLOWED_FACTORY_MODULES,
    BigQueryStorageClient,
    BigQueryEventClient,
    _is_allowed_module,
    load_job_definition,
    load_service_factory,
    run_job,
)
from lorchestra.processors.base import JobContext


# =============================================================================
# FACTORY LOADER TESTS
# =============================================================================


class TestIsAllowedModule:
    """Tests for _is_allowed_module helper."""

    def test_exact_match_allowed(self):
        """Exact match of allowed module returns True."""
        assert _is_allowed_module("gorch.sheets.factories") is True

    def test_submodule_allowed(self):
        """Submodule of allowed module returns True."""
        assert _is_allowed_module("gorch.sheets.factories.submodule") is True

    def test_unrelated_module_rejected(self):
        """Unrelated module returns False."""
        assert _is_allowed_module("os") is False
        assert _is_allowed_module("subprocess") is False
        assert _is_allowed_module("evil.module") is False

    def test_partial_prefix_rejected(self):
        """Partial prefix that doesn't match full module name rejected."""
        # "gorch.sheets.factorie" is NOT a prefix match for "gorch.sheets.factories"
        assert _is_allowed_module("gorch.sheets.factorie") is False
        # "gorch.sheets" is NOT in the allowlist
        assert _is_allowed_module("gorch.sheets") is False

    def test_parent_module_rejected(self):
        """Parent module of allowed is rejected (can't import gorch directly)."""
        assert _is_allowed_module("gorch") is False


class TestLoadServiceFactory:
    """Tests for load_service_factory function."""

    def test_valid_factory_path_loads_function(self):
        """Valid factory path from allowlist loads and returns callable."""
        # Use mock to avoid actual gorch import
        with patch("importlib.import_module") as mock_import:
            mock_module = MagicMock()
            mock_factory = MagicMock()
            mock_module.build_sheets_write_service = mock_factory
            mock_import.return_value = mock_module

            factory = load_service_factory("gorch.sheets.factories:build_sheets_write_service")

            mock_import.assert_called_once_with("gorch.sheets.factories")
            assert factory is mock_factory

    def test_missing_colon_raises_value_error(self):
        """Factory path without colon raises ValueError."""
        with pytest.raises(ValueError, match="must be 'module:function'"):
            load_service_factory("gorch.sheets.factories.build_sheets_write_service")

    def test_non_allowed_module_raises_value_error(self):
        """Non-allowlisted module raises ValueError."""
        with pytest.raises(ValueError, match="not in allowlist"):
            load_service_factory("os:system")

        with pytest.raises(ValueError, match="not in allowlist"):
            load_service_factory("subprocess:run")

        with pytest.raises(ValueError, match="not in allowlist"):
            load_service_factory("evil.module:bad_function")

    def test_import_error_propagates(self):
        """ImportError from missing module propagates with context."""
        with patch("importlib.import_module") as mock_import:
            mock_import.side_effect = ImportError("No module named 'gorch'")

            with pytest.raises(ImportError, match="Cannot import factory module"):
                load_service_factory("gorch.sheets.factories:build_sheets_write_service")

    def test_missing_function_raises_attribute_error(self):
        """Missing function in module raises AttributeError."""
        with patch("importlib.import_module") as mock_import:
            mock_module = MagicMock(spec=[])  # Empty spec, no attributes
            mock_import.return_value = mock_module

            with pytest.raises(AttributeError, match="not found in"):
                load_service_factory("gorch.sheets.factories:nonexistent_function")

    def test_non_callable_raises_type_error(self):
        """Non-callable attribute raises TypeError."""
        with patch("importlib.import_module") as mock_import:
            mock_module = MagicMock()
            mock_module.not_a_function = "just a string"
            mock_import.return_value = mock_module

            with pytest.raises(TypeError, match="is not callable"):
                load_service_factory("gorch.sheets.factories:not_a_function")

    def test_submodule_allowed(self):
        """Submodule of allowed module is allowed."""
        with patch("importlib.import_module") as mock_import:
            mock_module = MagicMock()
            mock_factory = MagicMock()
            mock_module.custom_factory = mock_factory
            mock_import.return_value = mock_module

            factory = load_service_factory("gorch.sheets.factories.custom:custom_factory")

            mock_import.assert_called_once_with("gorch.sheets.factories.custom")
            assert factory is mock_factory


class TestAllowlistSecurity:
    """Security-focused tests for factory loader allowlist."""

    def test_allowlist_is_minimal(self):
        """Allowlist only contains expected modules."""
        assert ALLOWED_FACTORY_MODULES == ["gorch.sheets.factories"]

    def test_cannot_load_arbitrary_code(self):
        """Cannot use factory loader to execute arbitrary code."""
        dangerous_paths = [
            "os:system",
            "subprocess:run",
            "builtins:eval",
            "builtins:exec",
            "__builtins__:eval",
            "importlib:import_module",
            "pickle:loads",
        ]
        for path in dangerous_paths:
            with pytest.raises(ValueError, match="not in allowlist"):
                load_service_factory(path)


class TestLoadJobDefinition:
    """Tests for load_job_definition function."""

    def test_load_valid_definition(self):
        """Load a valid job definition from file."""
        with TemporaryDirectory() as tmpdir:
            defs_dir = Path(tmpdir)
            def_file = defs_dir / "test_job.json"
            def_file.write_text(json.dumps({
                "job_id": "test_job",
                "job_type": "ingest",
                "source": {"stream": "test.stream"},
            }))

            job_def = load_job_definition("test_job", definitions_dir=defs_dir)

            assert job_def["job_id"] == "test_job"
            assert job_def["job_type"] == "ingest"
            assert job_def["source"]["stream"] == "test.stream"

    def test_load_definition_adds_job_id_if_missing(self):
        """Job ID is added from filename if not in definition."""
        with TemporaryDirectory() as tmpdir:
            defs_dir = Path(tmpdir)
            def_file = defs_dir / "my_job.json"
            def_file.write_text(json.dumps({
                "job_type": "ingest",
            }))

            job_def = load_job_definition("my_job", definitions_dir=defs_dir)

            assert job_def["job_id"] == "my_job"

    def test_load_nonexistent_definition_raises(self):
        """Loading nonexistent definition raises FileNotFoundError."""
        with TemporaryDirectory() as tmpdir:
            defs_dir = Path(tmpdir)

            with pytest.raises(FileNotFoundError, match="Job definition not found"):
                load_job_definition("nonexistent", definitions_dir=defs_dir)

    def test_load_invalid_json_raises(self):
        """Loading invalid JSON raises JSONDecodeError."""
        with TemporaryDirectory() as tmpdir:
            defs_dir = Path(tmpdir)
            def_file = defs_dir / "bad_def.json"
            def_file.write_text("{ invalid json }")

            with pytest.raises(json.JSONDecodeError):
                load_job_definition("bad_def", definitions_dir=defs_dir)


class TestBigQueryStorageClient:
    """Tests for BigQueryStorageClient."""

    @pytest.fixture
    def mock_bq_client(self):
        return MagicMock()

    @pytest.fixture
    def storage_client(self, mock_bq_client, test_config):
        return BigQueryStorageClient(mock_bq_client, test_config)

    def test_table_name_production(self, storage_client):
        """Production mode uses original table names."""
        assert storage_client._table_name("raw_objects") == "raw_objects"
        assert storage_client._table_name("canonical_objects") == "canonical_objects"

    def test_table_name_test_mode(self, mock_bq_client, test_config):
        """Test mode prefixes table names."""
        client = BigQueryStorageClient(mock_bq_client, test_config, test_table=True)
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

    def test_upsert_canonical_builds_merge_query(self, storage_client, mock_bq_client):
        """upsert_canonical builds MERGE query with correct structure."""
        # Mock load_table_from_json (temp table loading)
        mock_load_job = MagicMock()
        mock_load_job.result.return_value = None
        mock_bq_client.load_table_from_json.return_value = mock_load_job

        # Mock query result (MERGE query)
        mock_result = MagicMock()
        mock_result.num_dml_affected_rows = 1
        mock_bq_client.query.return_value.result.return_value = mock_result

        # Mock delete_table (cleanup)
        mock_bq_client.delete_table.return_value = None

        objects = [
            {
                "idem_key": "key1",
                "source_system": "gmail",
                "payload": {"canonical": "data"},
                "canonical_schema": "iglu:test/schema/1-0-0",
                "transform_ref": "test_transform@1.0.0",
            }
        ]

        result = storage_client.upsert_canonical(objects, "corr-123")

        assert result["inserted"] == 1

        # Verify temp table was loaded with data
        assert mock_bq_client.load_table_from_json.called
        load_call = mock_bq_client.load_table_from_json.call_args
        loaded_rows = load_call[0][0]  # First positional arg is the rows
        assert len(loaded_rows) == 1
        assert loaded_rows[0]["idem_key"] == "key1"
        assert loaded_rows[0]["canonical_schema"] == "iglu:test/schema/1-0-0"

        # Verify MERGE query was executed
        assert mock_bq_client.query.called
        query = mock_bq_client.query.call_args[0][0]
        assert "MERGE" in query
        # MERGE from temp table, not inline values
        assert "temp_canonical_" in query
        assert "ON T.idem_key = S.idem_key" in query

    def test_upsert_canonical_empty_returns_zero(self, storage_client, mock_bq_client):
        """upsert_canonical with empty list returns zeros."""
        result = storage_client.upsert_canonical([], "corr-123")
        assert result == {"inserted": 0, "updated": 0}
        assert not mock_bq_client.query.called


class TestBigQueryEventClient:
    """Tests for BigQueryEventClient."""

    def test_log_event_delegates_to_event_client(self, test_config):
        """log_event delegates to event_client module."""
        mock_bq_client = MagicMock()
        client = BigQueryEventClient(mock_bq_client, test_config)

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
                dataset="test_events",
            )


class TestRunJob:
    """Tests for run_job function."""

    @pytest.fixture
    def mock_bq_client(self):
        return MagicMock()

    @pytest.fixture
    def defs_dir(self):
        """Create temporary definitions directory with test job."""
        with TemporaryDirectory() as tmpdir:
            defs_dir = Path(tmpdir)
            def_file = defs_dir / "test_ingest.json"
            def_file.write_text(json.dumps({
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
            yield defs_dir

    def test_run_job_loads_definition_and_dispatches(self, defs_dir, mock_bq_client, test_config):
        """run_job loads definition and dispatches to processor."""
        mock_processor = MagicMock()

        with patch("lorchestra.job_runner.registry") as mock_registry:
            with patch("lorchestra.job_runner.ec"):
                mock_registry.get.return_value = mock_processor

                run_job(
                    "test_ingest",
                    definitions_dir=defs_dir,
                    bq_client=mock_bq_client,
                    config=test_config,
                    dry_run=True,
                )

                # Verify processor was called
                mock_processor.run.assert_called_once()

                # Verify job_def was passed
                call_args = mock_processor.run.call_args
                job_def = call_args[0][0]
                assert job_def["job_id"] == "test_ingest"
                assert job_def["job_type"] == "ingest"

                # Verify context was passed
                context = call_args[0][1]
                assert isinstance(context, JobContext)
                assert context.dry_run is True

    def test_run_job_emits_lifecycle_events(self, defs_dir, mock_bq_client, test_config):
        """run_job emits job.started and job.completed events."""
        mock_processor = MagicMock()

        with patch("lorchestra.job_runner.registry") as mock_registry:
            with patch("lorchestra.job_runner.ec") as mock_ec:
                mock_registry.get.return_value = mock_processor

                run_job(
                    "test_ingest",
                    definitions_dir=defs_dir,
                    bq_client=mock_bq_client,
                    config=test_config,
                )

                # Check log_event was called for started and completed
                log_event_calls = [c for c in mock_ec.log_event.call_args_list]
                event_types = [c.kwargs.get("event_type") for c in log_event_calls]

                assert "job.started" in event_types
                assert "job.completed" in event_types

    def test_run_job_emits_failed_event_on_error(self, defs_dir, mock_bq_client, test_config):
        """run_job emits job.failed event on processor error."""
        mock_processor = MagicMock()
        mock_processor.run.side_effect = RuntimeError("Processor error")

        with patch("lorchestra.job_runner.registry") as mock_registry:
            with patch("lorchestra.job_runner.ec") as mock_ec:
                mock_registry.get.return_value = mock_processor

                with pytest.raises(RuntimeError, match="Processor error"):
                    run_job(
                        "test_ingest",
                        definitions_dir=defs_dir,
                        bq_client=mock_bq_client,
                        config=test_config,
                    )

                # Check job.failed event was emitted
                log_event_calls = [c for c in mock_ec.log_event.call_args_list]
                event_types = [c.kwargs.get("event_type") for c in log_event_calls]

                assert "job.started" in event_types
                assert "job.failed" in event_types
                assert "job.completed" not in event_types

    def test_run_job_sets_run_mode(self, defs_dir, mock_bq_client, test_config):
        """run_job sets and resets run mode."""
        mock_processor = MagicMock()

        with patch("lorchestra.job_runner.registry") as mock_registry:
            with patch("lorchestra.job_runner.ec") as mock_ec:
                mock_registry.get.return_value = mock_processor

                run_job(
                    "test_ingest",
                    definitions_dir=defs_dir,
                    bq_client=mock_bq_client,
                    config=test_config,
                    dry_run=True,
                    test_table=True,
                )

                # Verify set_run_mode was called with expected args
                mock_ec.set_run_mode.assert_called_once_with(
                    dry_run=True, test_table=True, smoke_namespace=None
                )

                # Verify reset_run_mode was called
                mock_ec.reset_run_mode.assert_called_once()

    def test_run_job_missing_job_type_raises(self, mock_bq_client, test_config):
        """run_job raises ValueError if job_type is missing."""
        with TemporaryDirectory() as tmpdir:
            defs_dir = Path(tmpdir)
            def_file = defs_dir / "bad_job.json"
            def_file.write_text(json.dumps({
                "job_id": "bad_job",
                # Missing job_type
            }))

            with patch("lorchestra.job_runner.ec"):
                with pytest.raises(ValueError, match="missing job_type"):
                    run_job("bad_job", definitions_dir=defs_dir, bq_client=mock_bq_client, config=test_config)

    def test_run_job_unknown_job_type_raises(self, mock_bq_client, test_config):
        """run_job raises KeyError for unknown job_type."""
        with TemporaryDirectory() as tmpdir:
            defs_dir = Path(tmpdir)
            def_file = defs_dir / "unknown_type.json"
            def_file.write_text(json.dumps({
                "job_id": "unknown_type",
                "job_type": "nonexistent_type",
            }))

            with patch("lorchestra.job_runner.ec"):
                with pytest.raises(KeyError, match="Unknown job_type"):
                    run_job("unknown_type", definitions_dir=defs_dir, bq_client=mock_bq_client, config=test_config)
