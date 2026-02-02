"""Gate 05c: Validate bq_reader callable and sync_sqlite YAML jobs.

Confirms:
- bq_reader.execute() reads BQ and returns items for sqlite.sync
- bq_reader handles empty results
- bq_reader resolves canonical/derived datasets correctly
- bq_reader is registered in dispatch
- All 8 sync YAML job defs load and compile correctly
- Pipeline integration: bq_reader items -> plan.build -> sqlite.sync plan
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
    """Verify YAML job definitions load and compile correctly."""

    @pytest.mark.parametrize("job_id", SYNC_JOB_IDS)
    def test_sync_yaml_loads(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        assert job_def.job_id == job_id
        assert job_def.version == "2.0"
        assert len(job_def.steps) == 3

    @pytest.mark.parametrize("job_id", SYNC_JOB_IDS)
    def test_sync_yaml_has_correct_steps(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        steps = job_def.steps
        assert steps[0].op == "call"
        assert steps[0].params["callable"] == "bq_reader"
        assert steps[1].op == "plan.build"
        assert steps[1].params["method"] == "sqlite.sync"
        assert steps[2].op == "storacle.submit"

    @pytest.mark.parametrize("job_id", SYNC_JOB_IDS)
    def test_sync_yaml_compiles(self, job_id):
        from lorchestra.registry import JobRegistry
        from lorchestra.compiler import compile_job
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        instance = compile_job(job_def)
        assert instance.job_id == job_id
        assert len(instance.steps) == 3

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
    """Test the bq_reader -> plan.build pipeline."""

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
