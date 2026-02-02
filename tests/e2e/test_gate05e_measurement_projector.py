"""Gate 05e: Validate measurement_projector callable and YAML jobs.

Confirms:
- measurement_projector.execute() transforms canonical records to measurement_event rows
- Incremental query logic filters already-processed records
- Empty results handled
- Missing params raise ValueError
- measurement_projector is registered in dispatch
- All 3 measurement YAML job defs load and compile correctly
- Pipeline integration: measurement_projector items -> plan.build -> bq.upsert plan
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lorchestra.callable.result import CallableResult

DEFINITIONS_DIR = Path(__file__).resolve().parent.parent.parent / "lorchestra" / "jobs" / "definitions"


# ============================================================================
# Helpers
# ============================================================================


def _make_canonical_record(
    idem_key: str = "ik-001",
    connection_name: str = "google-forms-intake-01",
    respondent_id: str = "subj-1",
    form_id: str = "form-abc",
    response_id: str = "resp-xyz",
    submitted_at: str = "2025-01-15T12:00:00Z",
    correlation_id: str = "corr-001",
) -> dict:
    """Build a canonical form_response record as returned by BQ."""
    return {
        "idem_key": idem_key,
        "source_system": "google_forms",
        "connection_name": connection_name,
        "object_type": "form_response",
        "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
        "transform_ref": None,
        "payload": {
            "form_id": form_id,
            "response_id": response_id,
            "submitted_at": submitted_at,
            "respondent": {"id": respondent_id, "email": "test@example.com"},
        },
        "correlation_id": correlation_id,
    }


def _mock_bq_query(records):
    """Create a mock BigQueryClient whose query() returns given records."""
    mock_client_cls = MagicMock()
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_result = MagicMock()
    mock_result.rows = records
    mock_client.query.return_value = mock_result
    return mock_client_cls


# ============================================================================
# measurement_projector callable
# ============================================================================


class TestMeasurementProjector:
    """Test measurement_projector.execute() with mocked BQ."""

    def test_returns_callable_result(self):
        from lorchestra.callable.measurement_projector import execute

        records = [_make_canonical_record()]
        mock_bq = _mock_bq_query(records)

        with patch("storacle.clients.bigquery.BigQueryClient", mock_bq):
            result = execute({
                "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
                "connection_name": "google-forms-intake-01",
                "event_type": "form",
                "event_subtype": "intake_01",
            })

        assert result["schema_version"] == "1.0"
        assert len(result["items"]) == 1  # single batch item

    def test_transforms_record_fields(self):
        from lorchestra.callable.measurement_projector import execute

        records = [_make_canonical_record(
            idem_key="ik-test",
            respondent_id="subj-42",
            form_id="form-xyz",
            response_id="resp-999",
            submitted_at="2025-03-01T10:00:00Z",
        )]
        mock_bq = _mock_bq_query(records)

        with patch("storacle.clients.bigquery.BigQueryClient", mock_bq):
            result = execute({
                "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
                "event_type": "form",
                "event_subtype": "intake_01",
            })

        batch = result["items"][0]
        assert batch["dataset"] == "test_derived"
        assert batch["table"] == "measurement_events"
        assert batch["key_columns"] == ["idem_key"]

        row = batch["rows"][0]
        assert row["idem_key"] == "ik-test"
        assert row["measurement_event_id"] == "ik-test"
        assert row["subject_id"] == "subj-42"
        assert row["event_type"] == "form"
        assert row["event_subtype"] == "intake_01"
        assert row["form_id"] == "form-xyz"
        assert row["source_id"] == "resp-999"
        assert row["occurred_at"] == "2025-03-01T10:00:00Z"
        assert row["binding_id"] == "intake_01"
        assert row["source_system"] == "google_forms"
        assert row["source_entity"] == "form_response"
        assert row["canonical_object_id"] == "ik-test"

    def test_multiple_records(self):
        from lorchestra.callable.measurement_projector import execute

        records = [
            _make_canonical_record(idem_key="ik-1", respondent_id="s1"),
            _make_canonical_record(idem_key="ik-2", respondent_id="s2"),
            _make_canonical_record(idem_key="ik-3", respondent_id="s3"),
        ]
        mock_bq = _mock_bq_query(records)

        with patch("storacle.clients.bigquery.BigQueryClient", mock_bq):
            result = execute({
                "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
                "event_type": "form",
                "event_subtype": "followup",
            })

        batch = result["items"][0]
        assert len(batch["rows"]) == 3
        assert result["stats"]["input"] == 3
        assert result["stats"]["output"] == 3

    def test_empty_result(self):
        from lorchestra.callable.measurement_projector import execute

        mock_bq = _mock_bq_query([])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_bq):
            result = execute({
                "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
                "event_type": "form",
                "event_subtype": "intake_01",
            })

        assert result["items"] == []
        assert result["stats"]["input"] == 0
        assert result["stats"]["output"] == 0

    def test_missing_canonical_schema_raises(self):
        from lorchestra.callable.measurement_projector import execute

        with pytest.raises(ValueError, match="canonical_schema"):
            execute({"event_subtype": "intake_01"})

    def test_missing_event_subtype_raises(self):
        from lorchestra.callable.measurement_projector import execute

        with pytest.raises(ValueError, match="event_subtype"):
            execute({
                "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
            })

    def test_respondent_email_fallback(self):
        """If respondent has no id, fall back to email."""
        from lorchestra.callable.measurement_projector import execute

        record = _make_canonical_record()
        record["payload"]["respondent"] = {"email": "user@example.com"}
        mock_bq = _mock_bq_query([record])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_bq):
            result = execute({
                "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
                "event_type": "form",
                "event_subtype": "intake_01",
            })

        row = result["items"][0]["rows"][0]
        assert row["subject_id"] == "user@example.com"

    def test_payload_as_json_string(self):
        """Payload may come as JSON string from BQ; should be parsed."""
        from lorchestra.callable.measurement_projector import execute

        record = _make_canonical_record()
        record["payload"] = json.dumps(record["payload"])
        mock_bq = _mock_bq_query([record])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_bq):
            result = execute({
                "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
                "event_type": "form",
                "event_subtype": "intake_01",
            })

        row = result["items"][0]["rows"][0]
        assert row["subject_id"] == "subj-1"

    def test_incremental_query_sql(self):
        """Verify the BQ query includes incremental LEFT JOIN logic."""
        from lorchestra.callable.measurement_projector import execute

        mock_bq = _mock_bq_query([])

        with patch("storacle.clients.bigquery.BigQueryClient", mock_bq):
            execute({
                "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
                "connection_name": "google-forms-intake-01",
                "event_type": "form",
                "event_subtype": "intake_01",
            })

        mock_client = mock_bq.return_value
        call_args = mock_client.query.call_args
        sql = call_args[0][0]

        # Verify incremental join
        assert "LEFT JOIN" in sql
        assert "canonical_object_id IS NULL OR" in sql
        assert "canonicalized_at > m.processed_at" in sql
        # Verify table refs
        assert "test-project.test_canonical.canonical_objects" in sql
        assert "test-project.test_derived.measurement_events" in sql
        # Verify filters
        params = call_args[1]["params"]
        param_names = [p["name"] for p in params]
        assert "canonical_schema" in param_names
        assert "connection_name" in param_names

    def test_stats(self):
        from lorchestra.callable.measurement_projector import execute

        records = [
            _make_canonical_record(idem_key=f"ik-{i}") for i in range(5)
        ]
        mock_bq = _mock_bq_query(records)

        with patch("storacle.clients.bigquery.BigQueryClient", mock_bq):
            result = execute({
                "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
                "event_type": "form",
                "event_subtype": "intake_02",
            })

        assert result["stats"]["input"] == 5
        assert result["stats"]["output"] == 5
        assert result["stats"]["errors"] == 0

    def test_metadata_is_json_string(self):
        """metadata field should be a JSON string."""
        from lorchestra.callable.measurement_projector import execute

        records = [_make_canonical_record()]
        mock_bq = _mock_bq_query(records)

        with patch("storacle.clients.bigquery.BigQueryClient", mock_bq):
            result = execute({
                "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
                "event_type": "form",
                "event_subtype": "intake_01",
            })

        row = result["items"][0]["rows"][0]
        assert row["metadata"] == "{}"
        # Verify it's valid JSON
        parsed = json.loads(row["metadata"])
        assert parsed == {}


# ============================================================================
# Dispatch integration
# ============================================================================


class TestDispatchIntegration:
    """Verify measurement_projector is registered and dispatchable."""

    def test_measurement_projector_registered(self):
        from lorchestra.callable.dispatch import get_callables
        callables = get_callables()
        assert "measurement_projector" in callables


# ============================================================================
# YAML job definition loading and compilation
# ============================================================================


ME_JOB_IDS = [
    "proj_me_intake_01",
    "proj_me_intake_02",
    "proj_me_followup",
]


class TestYamlJobDefs:
    """Verify YAML job definitions load and compile correctly."""

    @pytest.mark.parametrize("job_id", ME_JOB_IDS)
    def test_me_yaml_loads(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        assert job_def.job_id == job_id
        assert job_def.version == "2.0"
        assert len(job_def.steps) == 3

    @pytest.mark.parametrize("job_id", ME_JOB_IDS)
    def test_me_yaml_has_correct_steps(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        steps = job_def.steps
        assert steps[0].op == "call"
        assert steps[0].params["callable"] == "measurement_projector"
        assert steps[1].op == "plan.build"
        assert steps[1].params["method"] == "bq.upsert"
        assert steps[2].op == "storacle.submit"

    @pytest.mark.parametrize("job_id", ME_JOB_IDS)
    def test_me_yaml_has_correct_callable_params(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        params = job_def.steps[0].params
        assert params["canonical_schema"] == "iglu:org.canonical/form_response/jsonschema/1-0-0"
        assert params["event_type"] == "form"
        assert "connection_name" in params
        assert "event_subtype" in params

    @pytest.mark.parametrize("job_id", ME_JOB_IDS)
    def test_me_yaml_compiles(self, job_id):
        from lorchestra.registry import JobRegistry
        from lorchestra.compiler import compile_job
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        instance = compile_job(job_def)
        assert instance.job_id == job_id
        assert len(instance.steps) == 3


# ============================================================================
# Pipeline integration
# ============================================================================


class TestPipelineIntegration:
    """Test measurement_projector -> plan.build pipeline."""

    def test_measurement_projector_to_plan(self):
        """measurement_projector items feed into plan.build with bq.upsert method."""
        from lorchestra.callable.measurement_projector import execute
        from lorchestra.plan_builder import build_plan_from_items

        records = [
            _make_canonical_record(idem_key="ik-1"),
            _make_canonical_record(idem_key="ik-2"),
        ]
        mock_bq = _mock_bq_query(records)

        with patch("storacle.clients.bigquery.BigQueryClient", mock_bq):
            result = execute({
                "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
                "event_type": "form",
                "event_subtype": "intake_01",
            })

        items = result["items"]

        plan = build_plan_from_items(
            items=items,
            correlation_id="test_05e",
            method="bq.upsert",
        )

        assert plan.kind == "storacle.plan"
        assert len(plan.ops) == 1  # single batch op
        assert plan.ops[0].method == "bq.upsert"
        assert plan.ops[0].params["dataset"] == "test_derived"
        assert plan.ops[0].params["table"] == "measurement_events"
        assert plan.ops[0].params["key_columns"] == ["idem_key"]
        assert len(plan.ops[0].params["rows"]) == 2
        # bq.upsert doesn't use idempotency keys
        assert plan.ops[0].idempotency_key is None
