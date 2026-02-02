"""Gate 05f: Validate observation_projector callable and YAML jobs.

Confirms:
- observation_projector.execute() scores measurement_events into observation rows
- Incremental query logic filters already-scored records
- Canonical lookup and form_response building
- Score extraction, severity mapping
- Empty results handled
- Missing params raise ValueError
- observation_projector is registered in dispatch
- All 3 observation YAML job defs load and compile correctly
- Pipeline integration: observation_projector items -> plan.build -> bq.upsert plan
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from lorchestra.callable.result import CallableResult

DEFINITIONS_DIR = Path(__file__).resolve().parent.parent.parent / "lorchestra" / "jobs" / "definitions"


# ============================================================================
# Helpers / Fixtures
# ============================================================================


def _make_measurement_event(
    idem_key: str = "me-001",
    measurement_event_id: str = "me-001",
    subject_id: str = "subj-1",
    canonical_object_id: str = "ik-001",
    binding_id: str = "intake_01",
    correlation_id: str = "corr-001",
) -> dict:
    """Build a measurement_event record as returned by BQ."""
    return {
        "idem_key": idem_key,
        "measurement_event_id": measurement_event_id,
        "subject_id": subject_id,
        "event_type": "form",
        "event_subtype": binding_id,
        "occurred_at": "2025-01-15T12:00:00Z",
        "source_system": "google_forms",
        "source_entity": "form_response",
        "source_id": "resp-xyz",
        "canonical_object_id": canonical_object_id,
        "form_id": "form-abc",
        "binding_id": binding_id,
        "binding_version": None,
        "correlation_id": correlation_id,
    }


def _make_canonical_record(
    idem_key: str = "ik-001",
    connection_name: str = "google-forms-intake-01",
    answers: list | None = None,
) -> dict:
    """Build a canonical object record."""
    if answers is None:
        answers = [
            {"question_id": "q1", "answer_value": "3"},
            {"question_id": "q2", "answer_value": "2"},
        ]
    return {
        "idem_key": idem_key,
        "source_system": "google_forms",
        "connection_name": connection_name,
        "object_type": "form_response",
        "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
        "transform_ref": None,
        "payload": {
            "form_id": "form-abc",
            "response_id": "resp-xyz",
            "submitted_at": "2025-01-15T12:00:00Z",
            "respondent": {"id": "subj-1", "email": "test@example.com"},
            "answers": answers,
        },
        "correlation_id": "corr-001",
    }


def _make_observation(
    code: str = "phq9_total",
    kind: str = "scale",
    value: int | float | None = 12,
    value_type: str = "integer",
    label: str | None = "Moderate",
    raw_answer: str | None = None,
    position: int | None = None,
    missing: bool = False,
) -> SimpleNamespace:
    """Build a mock Observation."""
    return SimpleNamespace(
        code=code,
        kind=kind,
        value=value,
        value_type=value_type,
        label=label,
        raw_answer=raw_answer,
        position=position,
        missing=missing,
    )


def _make_measurement_event_result(
    measure_id: str = "phq9",
    measure_version: str = "1.0.0",
    measurement_event_id: str = "evt-001",
    subject_id: str = "subj-1",
    observations: list | None = None,
) -> SimpleNamespace:
    """Build a mock MeasurementEvent from final-form."""
    if observations is None:
        observations = [
            _make_observation(code="phq9_total", kind="scale", value=12, label="Moderate"),
            _make_observation(code="phq9_q1", kind="item", value=3, value_type="integer", position=0),
        ]
    return SimpleNamespace(
        measure_id=measure_id,
        measure_version=measure_version,
        measurement_event_id=measurement_event_id,
        subject_id=subject_id,
        observations=observations,
    )


def _make_processing_result(events: list | None = None) -> SimpleNamespace:
    """Build a mock ProcessingResult."""
    if events is None:
        events = [_make_measurement_event_result()]
    return SimpleNamespace(
        form_submission_id="resp-xyz",
        events=events,
        diagnostics=None,
        success=True,
    )


def _setup_bq_mocks(me_records, canonical_records_by_key):
    """Set up BQ mocks for both measurement_events query and canonical lookups.

    Returns a mock BigQueryClient class that handles both types of queries.
    """
    mock_client_cls = MagicMock()
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    call_count = {"n": 0}

    def mock_query(sql, params=None):
        result = MagicMock()
        call_count["n"] += 1
        # First call is the measurement_events query
        if call_count["n"] == 1:
            result.rows = me_records
        else:
            # Subsequent calls are canonical lookups
            idem_key = None
            if params:
                for p in params:
                    if p["name"] == "idem_key":
                        idem_key = p["value"]
            if idem_key and idem_key in canonical_records_by_key:
                result.rows = [canonical_records_by_key[idem_key]]
            else:
                result.rows = []
        return result

    mock_client.query.side_effect = mock_query
    return mock_client_cls


# ============================================================================
# observation_projector callable
# ============================================================================


class TestObservationProjector:
    """Test observation_projector.execute() with mocked BQ and Pipeline."""

    def _execute_with_mocks(self, params, me_records, canonical_map, processing_result=None):
        """Helper to run execute with standard mocks."""
        mock_bq = _setup_bq_mocks(me_records, canonical_map)
        mock_pipeline_cls = MagicMock()
        mock_pipeline = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline
        mock_pipeline.process.return_value = processing_result or _make_processing_result()
        mock_pipeline.binding_spec = SimpleNamespace(binding_id="intake_01", version="1.0.0")

        with patch("storacle.clients.bigquery.BigQueryClient", mock_bq), \
             patch("finalform.pipeline.Pipeline", mock_pipeline_cls), \
             patch("finalform.pipeline.PipelineConfig"):
            from lorchestra.callable.observation_projector import execute
            return execute(params)

    def test_returns_callable_result(self):
        me_records = [_make_measurement_event()]
        canonical_map = {"ik-001": _make_canonical_record()}

        result = self._execute_with_mocks(
            {"binding_id": "intake_01"},
            me_records, canonical_map,
        )

        assert result["schema_version"] == "1.0"
        assert len(result["items"]) == 1

    def test_observation_row_fields(self):
        me_records = [_make_measurement_event(measurement_event_id="me-test")]
        canonical_map = {"ik-001": _make_canonical_record()}

        result = self._execute_with_mocks(
            {"binding_id": "intake_01"},
            me_records, canonical_map,
        )

        batch = result["items"][0]
        assert batch["dataset"] == "test_derived"
        assert batch["table"] == "observations"
        assert batch["key_columns"] == ["idem_key"]

        row = batch["rows"][0]
        assert row["idem_key"] == "me-test:phq9"
        assert row["measurement_event_id"] == "me-test"
        assert row["subject_id"] == "subj-1"
        assert row["measure_code"] == "phq9"
        assert row["measure_version"] == "1.0.0"
        assert row["value_numeric"] == 12.0
        assert row["severity_label"] == "Moderate"
        assert row["severity_code"] == "moderate"
        assert row["unit"] == "score"

    def test_components_json(self):
        me_records = [_make_measurement_event()]
        canonical_map = {"ik-001": _make_canonical_record()}

        result = self._execute_with_mocks(
            {"binding_id": "intake_01"},
            me_records, canonical_map,
        )

        row = result["items"][0]["rows"][0]
        components = json.loads(row["components"])
        assert len(components) == 2
        assert components[0]["code"] == "phq9_total"
        assert components[0]["kind"] == "scale"
        assert components[1]["code"] == "phq9_q1"
        assert components[1]["kind"] == "item"

    def test_multiple_measures_per_event(self):
        """Each measure in the ProcessingResult creates a separate observation row."""
        me_records = [_make_measurement_event()]
        canonical_map = {"ik-001": _make_canonical_record()}
        processing_result = _make_processing_result(events=[
            _make_measurement_event_result(measure_id="phq9"),
            _make_measurement_event_result(measure_id="gad7"),
        ])

        result = self._execute_with_mocks(
            {"binding_id": "intake_01"},
            me_records, canonical_map,
            processing_result=processing_result,
        )

        batch = result["items"][0]
        assert len(batch["rows"]) == 2
        assert batch["rows"][0]["measure_code"] == "phq9"
        assert batch["rows"][1]["measure_code"] == "gad7"

    def test_empty_result(self):
        result = self._execute_with_mocks(
            {"binding_id": "intake_01"},
            [], {},
        )

        assert result["items"] == []
        assert result["stats"]["input"] == 0
        assert result["stats"]["output"] == 0

    def test_missing_binding_id_raises(self):
        from lorchestra.callable.observation_projector import execute

        with pytest.raises(ValueError, match="binding_id"):
            execute({})

    def test_stats(self):
        me_records = [
            _make_measurement_event(idem_key=f"me-{i}", measurement_event_id=f"me-{i}", canonical_object_id=f"ik-{i}")
            for i in range(3)
        ]
        canonical_map = {f"ik-{i}": _make_canonical_record(idem_key=f"ik-{i}") for i in range(3)}

        result = self._execute_with_mocks(
            {"binding_id": "intake_01"},
            me_records, canonical_map,
        )

        assert result["stats"]["input"] == 3
        # Each ME produces 1 event with 1 observation row (from default mock)
        assert result["stats"]["output"] == 3
        assert result["stats"]["errors"] == 0

    def test_custom_observation_table(self):
        me_records = [_make_measurement_event()]
        canonical_map = {"ik-001": _make_canonical_record()}

        result = self._execute_with_mocks(
            {"binding_id": "intake_01", "observation_table": "custom_obs"},
            me_records, canonical_map,
        )

        batch = result["items"][0]
        assert batch["table"] == "custom_obs"


# ============================================================================
# Score extraction helpers
# ============================================================================


class TestScoreExtraction:
    """Test _extract_total_score, _extract_severity, _severity_label_to_code."""

    def test_extract_total_score(self):
        from lorchestra.callable.observation_projector import _extract_total_score

        event = _make_measurement_event_result(observations=[
            _make_observation(code="phq9_q1", kind="item", value=3),
            _make_observation(code="phq9_total", kind="scale", value=15),
        ])
        assert _extract_total_score(event) == 15.0

    def test_extract_total_score_none(self):
        from lorchestra.callable.observation_projector import _extract_total_score

        event = _make_measurement_event_result(observations=[
            _make_observation(code="phq9_q1", kind="item", value=3),
        ])
        assert _extract_total_score(event) is None

    def test_extract_severity(self):
        from lorchestra.callable.observation_projector import _extract_severity

        event = _make_measurement_event_result(observations=[
            _make_observation(code="phq9_total", kind="scale", value=15, label="Moderately Severe"),
        ])
        assert _extract_severity(event) == "Moderately Severe"

    def test_severity_label_to_code(self):
        from lorchestra.callable.observation_projector import _severity_label_to_code

        assert _severity_label_to_code("Minimal") == "none"
        assert _severity_label_to_code("None-Minimal") == "none"
        assert _severity_label_to_code("Mild") == "mild"
        assert _severity_label_to_code("Moderate") == "moderate"
        assert _severity_label_to_code("Moderately Severe") == "moderate_severe"
        assert _severity_label_to_code("Severe") == "severe"
        assert _severity_label_to_code("Unknown Category") is None
        assert _severity_label_to_code("") is None


# ============================================================================
# Form response building
# ============================================================================


class TestBuildFormResponse:
    """Test _build_form_response helper."""

    def test_builds_form_response(self):
        from lorchestra.callable.observation_projector import _build_form_response

        record = _make_canonical_record()
        payload = record["payload"]

        fr = _build_form_response(record, payload)

        assert fr["form_id"] == "form-abc"
        assert fr["form_submission_id"] == "resp-xyz"
        assert fr["subject_id"] == "subj-1"
        assert fr["timestamp"] == "2025-01-15T12:00:00Z"
        assert len(fr["items"]) == 2
        assert fr["items"][0]["field_key"] == "q1"
        assert fr["items"][0]["answer"] == "3"
        assert fr["items"][0]["position"] == 0

    def test_fallback_form_id(self):
        from lorchestra.callable.observation_projector import _build_form_response

        record = _make_canonical_record()
        payload = dict(record["payload"])
        payload["form_id"] = None  # No form_id

        fr = _build_form_response(record, payload)
        assert fr["form_id"] == "googleforms::google-forms-intake-01"


# ============================================================================
# Dispatch integration
# ============================================================================


class TestDispatchIntegration:
    """Verify observation_projector is registered and dispatchable."""

    def test_observation_projector_registered(self):
        from lorchestra.callable.dispatch import get_callables
        callables = get_callables()
        assert "observation_projector" in callables


# ============================================================================
# YAML job definition loading and compilation
# ============================================================================


OBS_JOB_IDS = [
    "proj_obs_intake_01",
    "proj_obs_intake_02",
    "proj_obs_followup",
]


class TestYamlJobDefs:
    """Verify YAML job definitions load and compile correctly."""

    @pytest.mark.parametrize("job_id", OBS_JOB_IDS)
    def test_obs_yaml_loads(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        assert job_def.job_id == job_id
        assert job_def.version == "2.0"
        assert len(job_def.steps) == 3

    @pytest.mark.parametrize("job_id", OBS_JOB_IDS)
    def test_obs_yaml_has_correct_steps(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        steps = job_def.steps
        assert steps[0].op == "call"
        assert steps[0].params["callable"] == "observation_projector"
        assert steps[1].op == "plan.build"
        assert steps[1].params["method"] == "bq.upsert"
        assert steps[2].op == "storacle.submit"

    @pytest.mark.parametrize("job_id", OBS_JOB_IDS)
    def test_obs_yaml_has_correct_callable_params(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        params = job_def.steps[0].params
        assert "binding_id" in params
        assert params["binding_version"] == "1.0.0"
        assert params["measurement_table"] == "measurement_events"
        assert params["observation_table"] == "observations"

    @pytest.mark.parametrize("job_id", OBS_JOB_IDS)
    def test_obs_yaml_compiles(self, job_id):
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
    """Test observation_projector -> plan.build pipeline."""

    def test_observation_projector_to_plan(self):
        """observation_projector items feed into plan.build with bq.upsert method."""
        from lorchestra.plan_builder import build_plan_from_items

        # Simulate what the callable returns
        items = [{
            "dataset": "test_derived",
            "table": "observations",
            "rows": [
                {"idem_key": "me-1:phq9", "measure_code": "phq9", "value_numeric": 12},
                {"idem_key": "me-1:gad7", "measure_code": "gad7", "value_numeric": 8},
            ],
            "key_columns": ["idem_key"],
        }]

        plan = build_plan_from_items(
            items=items,
            correlation_id="test_05f",
            method="bq.upsert",
        )

        assert plan.to_dict()["plan_version"] == "storacle.plan/1.0.0"
        assert len(plan.ops) == 1  # single batch op
        assert plan.ops[0].method == "bq.upsert"
        assert plan.ops[0].params["dataset"] == "test_derived"
        assert plan.ops[0].params["table"] == "observations"
        assert plan.ops[0].params["key_columns"] == ["idem_key"]
        assert len(plan.ops[0].params["rows"]) == 2
        assert plan.ops[0].idempotency_key is None
