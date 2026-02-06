"""Gate 05c: Validate projection sync jobs - both legacy and new architecture.

Legacy path (preserved for backwards compatibility):
- bq_reader.execute() reads BQ and returns items for sqlite.sync

New architecture (e005b-09b):
- storacle.query → canonizer(projection/*) → plan.build → storacle.submit
- Canonizer aggregation mode for projection/* transforms
- Projection transforms package rows for sqlite.sync and sheets.write_table
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lorchestra.callable.result import CallableResult

DEFINITIONS_DIR = Path(__file__).resolve().parent.parent.parent / "lorchestra" / "jobs" / "definitions"


# ============================================================================
# Mock BQ client for bq_reader tests
# ============================================================================


def _make_mock_query_result(rows):
    """Build a mock QueryResult matching storacle.clients.bigquery.QueryResult."""
    mock_result = MagicMock()
    mock_result.rows = rows
    mock_result.rows_returned = len(rows)
    mock_result.job_id = "test-job-123"
    mock_result.elapsed_ms = 42
    return mock_result


@pytest.fixture
def mock_bq_client():
    """Mock storacle BigQueryClient for bq_reader tests."""
    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_cls.return_value = mock_instance
    return mock_cls, mock_instance


# ============================================================================
# bq_reader callable
# ============================================================================


class TestBqReader:
    """Test bq_reader.execute() with mocked BQ client."""

    def test_returns_callable_result(self, mock_bq_client):
        mock_cls, mock_instance = mock_bq_client
        mock_instance.query.return_value = _make_mock_query_result([
            {"client_id": "c1", "name": "Alice"},
            {"client_id": "c2", "name": "Bob"},
        ])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_cls):
            from lorchestra.callable.bq_reader import execute
            result = execute({
                "projection": "proj_clients",
                "sqlite_path": "/tmp/test.db",
                "table": "clients",
            })

        assert result["schema_version"] == "1.0"
        assert len(result["items"]) == 1
        item = result["items"][0]
        assert item["sqlite_path"] == "/tmp/test.db"
        assert item["table"] == "clients"
        assert len(item["rows"]) == 2
        assert "projected_at" in item["columns"]

    def test_rows_stringified(self, mock_bq_client):
        mock_cls, mock_instance = mock_bq_client
        mock_instance.query.return_value = _make_mock_query_result([
            {"id": 42, "active": True, "score": None},
        ])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_cls):
            from lorchestra.callable.bq_reader import execute
            result = execute({
                "projection": "proj_clients",
                "sqlite_path": "/tmp/test.db",
                "table": "t",
            })

        row = result["items"][0]["rows"][0]
        assert row["id"] == "42"
        assert row["active"] == "True"
        assert row["score"] is None
        assert row["projected_at"] is not None

    def test_columns_include_projected_at(self, mock_bq_client):
        mock_cls, mock_instance = mock_bq_client
        mock_instance.query.return_value = _make_mock_query_result([
            {"a": "1", "b": "2"},
        ])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_cls):
            from lorchestra.callable.bq_reader import execute
            result = execute({
                "projection": "proj_clients",
                "sqlite_path": "/tmp/test.db",
                "table": "t",
            })

        columns = result["items"][0]["columns"]
        assert columns == ["a", "b", "projected_at"]

    def test_empty_result(self, mock_bq_client):
        mock_cls, mock_instance = mock_bq_client
        mock_instance.query.return_value = _make_mock_query_result([])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_cls):
            from lorchestra.callable.bq_reader import execute
            result = execute({
                "projection": "proj_clients",
                "sqlite_path": "/tmp/test.db",
                "table": "clients",
            })

        assert len(result["items"]) == 1
        item = result["items"][0]
        assert item["rows"] == []
        assert item["columns"] == []
        assert result["stats"]["output"] == 0

    def test_canonical_dataset(self, mock_bq_client):
        """Default dataset is canonical."""
        mock_cls, mock_instance = mock_bq_client
        mock_instance.query.return_value = _make_mock_query_result([{"a": "1"}])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_cls):
            from lorchestra.callable.bq_reader import execute
            execute({
                "projection": "proj_clients",
                "sqlite_path": "/tmp/test.db",
                "table": "t",
            })

        sql_arg = mock_instance.query.call_args[0][0]
        assert "test_canonical" in sql_arg
        assert "test-project" in sql_arg

    def test_derived_dataset(self, mock_bq_client):
        """dataset='derived' uses derived dataset."""
        mock_cls, mock_instance = mock_bq_client
        mock_instance.query.return_value = _make_mock_query_result([{"a": "1"}])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_cls):
            from lorchestra.callable.bq_reader import execute
            execute({
                "projection": "measurement_events",
                "sqlite_path": "/tmp/test.db",
                "table": "measurement_events",
                "dataset": "derived",
            })

        sql_arg = mock_instance.query.call_args[0][0]
        assert "test_derived" in sql_arg

    def test_missing_params_raises(self, mock_bq_client):
        mock_cls, _ = mock_bq_client

        with patch("storacle.clients.bigquery.BigQueryClient", mock_cls):
            from lorchestra.callable.bq_reader import execute
            with pytest.raises(ValueError, match="Missing required params"):
                execute({"projection": "proj_clients"})

    def test_stats(self, mock_bq_client):
        mock_cls, mock_instance = mock_bq_client
        mock_instance.query.return_value = _make_mock_query_result([
            {"a": "1"}, {"a": "2"}, {"a": "3"},
        ])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_cls):
            from lorchestra.callable.bq_reader import execute
            result = execute({
                "projection": "proj_clients",
                "sqlite_path": "/tmp/test.db",
                "table": "t",
            })

        assert result["stats"]["input"] == 3
        assert result["stats"]["output"] == 3


# ============================================================================
# Dispatch integration
# ============================================================================


class TestDispatchIntegration:
    """Verify bq_reader is registered and dispatchable."""

    def test_bq_reader_registered(self):
        from lorchestra.callable.dispatch import get_callables
        callables = get_callables()
        assert "bq_reader" in callables


# ============================================================================
# YAML job definition loading and compilation
# ============================================================================


SYNC_JOB_IDS = [
    "sync_proj_clients",
    "sync_proj_sessions",
    "sync_proj_transcripts",
    "sync_proj_clinical_documents",
    "sync_proj_form_responses",
    "sync_proj_contact_events",
    "sync_measurement_events",
    "sync_observations",
]


class TestYamlJobDefs:
    """Verify YAML job definitions load and compile correctly (new architecture)."""

    @pytest.mark.parametrize("job_id", SYNC_JOB_IDS)
    def test_sync_yaml_loads(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        assert job_def.job_id == job_id
        assert job_def.version == "2.0"
        assert len(job_def.steps) == 4  # read, package, persist, write

    @pytest.mark.parametrize("job_id", SYNC_JOB_IDS)
    def test_sync_yaml_has_correct_steps(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        steps = job_def.steps
        # Step 1: storacle.query (read from BQ)
        assert steps[0].op == "storacle.query"
        assert "table" in steps[0].params
        # Step 2: call canonizer (package)
        assert steps[1].op == "call"
        assert steps[1].params["callable"] == "canonizer"
        assert steps[1].params["config"]["transform_id"] == "projection/bq_rows_to_sqlite_sync@1.0.0"
        # Step 3: plan.build (persist)
        assert steps[2].op == "plan.build"
        assert steps[2].params["method"] == "sqlite.sync"
        # Step 4: storacle.submit (write)
        assert steps[3].op == "storacle.submit"

    @pytest.mark.parametrize("job_id", SYNC_JOB_IDS)
    def test_sync_yaml_compiles(self, job_id):
        from lorchestra.registry import JobRegistry
        from lorchestra.compiler import compile_job
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        instance = compile_job(job_def)
        assert instance.job_id == job_id
        assert len(instance.steps) == 4

    def test_derived_jobs_have_dataset_param(self):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)

        for job_id in ["sync_measurement_events", "sync_observations"]:
            job_def = registry.load(job_id)
            assert job_def.steps[0].params.get("dataset") == "derived"


# ============================================================================
# Pipeline integration: bq_reader items -> plan.build -> sqlite.sync plan
# ============================================================================


class TestPipelineIntegration:
    """Test the bq_reader -> plan.build pipeline (legacy)."""

    def test_bq_reader_to_plan(self, mock_bq_client):
        """bq_reader items feed into plan.build with sqlite.sync method."""
        mock_cls, mock_instance = mock_bq_client
        mock_instance.query.return_value = _make_mock_query_result([
            {"name": "Alice", "age": "30"},
            {"name": "Bob", "age": "25"},
        ])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_cls):
            from lorchestra.callable.bq_reader import execute
            from lorchestra.plan_builder import build_plan_from_items

            result = execute({
                "projection": "proj_clients",
                "sqlite_path": "/tmp/test.db",
                "table": "clients",
            })
            items = result["items"]

            plan = build_plan_from_items(
                items=items,
                correlation_id="test_05c",
                method="sqlite.sync",
            )

        assert plan.to_dict()["plan_version"] == "storacle.plan/1.0.0"
        assert len(plan.ops) == 1
        assert plan.ops[0].method == "sqlite.sync"
        assert plan.ops[0].params["table"] == "clients"
        assert plan.ops[0].params["sqlite_path"] == "/tmp/test.db"
        assert len(plan.ops[0].params["rows"]) == 2
        assert "projected_at" in plan.ops[0].params["columns"]
        assert plan.ops[0].idempotency_key is None


# ============================================================================
# Canonizer Aggregation Mode Tests (e005b-09b)
# ============================================================================


class TestCanonizerAggregationMode:
    """Test canonizer aggregation mode for projection/* transforms."""

    def test_projection_transform_aggregates_items(self):
        """projection/* transforms receive all items as single input."""
        from canonizer import execute

        # Simulate BQ rows from storacle.query
        rows = [
            {"client_id": "c1", "name": "Alice"},
            {"client_id": "c2", "name": "Bob"},
        ]

        result = execute({
            "source_type": "projection",
            "items": rows,
            "config": {
                "transform_id": "projection/bq_rows_to_sqlite_sync@1.0.0",
                "sqlite_path": "/tmp/test.db",
                "table": "clients",
                "auto_timestamp_columns": ["projected_at"],
            },
        })

        # Aggregation: N input rows -> 1 output item
        assert result["schema_version"] == "1.0"
        assert len(result["items"]) == 1
        assert result["stats"]["input"] == 2
        assert result["stats"]["output"] == 1

        # Output is a packaged sqlite.sync payload
        item = result["items"][0]
        assert item["sqlite_path"] == "/tmp/test.db"
        assert item["table"] == "clients"
        assert "projected_at" in item["columns"]
        assert len(item["rows"]) == 2

    def test_projection_transform_empty_returns_no_items(self):
        """Empty rows returns no items (skip the sync)."""
        from canonizer import execute

        result = execute({
            "source_type": "projection",
            "items": [],
            "config": {
                "transform_id": "projection/bq_rows_to_sqlite_sync@1.0.0",
                "sqlite_path": "/tmp/test.db",
                "table": "clients",
            },
        })

        # Empty input -> no items (skip the sync op)
        assert len(result["items"]) == 0
        assert result["stats"]["input"] == 0
        assert result["stats"]["skipped"] == 0

    def test_sheets_transform_builds_2d_array(self):
        """sheets.write_table transform builds 2D values array."""
        from canonizer import execute

        rows = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

        result = execute({
            "source_type": "projection",
            "items": rows,
            "config": {
                "transform_id": "projection/bq_rows_to_sheets_write_table@1.0.0",
                "spreadsheet_id": "test-spreadsheet-id",
                "sheet_name": "clients",
                "strategy": "replace",
                "account": "acct1",
            },
        })

        assert len(result["items"]) == 1
        item = result["items"][0]
        assert item["spreadsheet_id"] == "test-spreadsheet-id"
        assert item["sheet_name"] == "clients"
        assert item["strategy"] == "replace"
        assert item["account"] == "acct1"
        # 2D array: header + data rows
        assert len(item["values"]) == 3  # 1 header + 2 data rows

    def test_sheets_transform_respects_column_order(self):
        """sheets.write_table respects column_order config."""
        from canonizer import execute

        rows = [{"b": "2", "a": "1", "c": "3"}]

        result = execute({
            "source_type": "projection",
            "items": rows,
            "config": {
                "transform_id": "projection/bq_rows_to_sheets_write_table@1.0.0",
                "spreadsheet_id": "test-id",
                "sheet_name": "test",
                "column_order": ["c", "a", "b"],  # explicit ordering
            },
        })

        item = result["items"][0]
        # Header row should be in specified order
        assert item["values"][0] == ["c", "a", "b"]


# ============================================================================
# Sheets Job YAML Tests (e005b-09b)
# ============================================================================


SHEETS_JOB_IDS = [
    "proj_sheets_clients",
    "proj_sheets_sessions",
    "proj_sheets_clinical_documents",
    "proj_sheets_contact_events",
    "proj_sheets_proj_clients",
]


class TestSheetsYamlJobDefs:
    """Verify Sheets YAML job definitions use new architecture."""

    @pytest.mark.parametrize("job_id", SHEETS_JOB_IDS)
    def test_sheets_yaml_loads(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        assert job_def.job_id == job_id
        assert job_def.version == "2.0"
        assert len(job_def.steps) == 4

    @pytest.mark.parametrize("job_id", SHEETS_JOB_IDS)
    def test_sheets_yaml_has_correct_steps(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        steps = job_def.steps
        # Step 1: storacle.query
        assert steps[0].op == "storacle.query"
        # Step 2: call canonizer with sheets transform
        assert steps[1].op == "call"
        assert steps[1].params["callable"] == "canonizer"
        assert steps[1].params["config"]["transform_id"] == "projection/bq_rows_to_sheets_write_table@1.0.0"
        # Step 3: plan.build with sheets.write_table method
        assert steps[2].op == "plan.build"
        assert steps[2].params["method"] == "sheets.write_table"
        # Step 4: storacle.submit
        assert steps[3].op == "storacle.submit"
