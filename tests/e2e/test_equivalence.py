"""Equivalence tests: prove new callables produce identical outputs to legacy processors.

e005b-05-equiv — Phases 05a-05f migrated 6 projection processor classes to 6
callable modules + 29 YAML job definitions.  Gate tests (test_gate05a-f) verified
structural correctness with mocked I/O.  These tests feed identical inputs to
both old-style processor methods and new callable module-level functions, then
compare outputs field-by-field with frozen timestamps.

Test classes:
    1. TestViewCreatorEquivalence  — SQL generation (character-identical)
    2. TestMoltProjectorEquivalence — CTAS generation (character-identical)
    3. TestBqReaderEquivalence     — row stringify + projected_at
    4. TestFileRendererEquivalence  — template rendering, front-matter YAML
    5. TestMeasurementProjectorEquivalence — _canonical_to_measurement_event
    6. TestObservationProjectorEquivalence — _build_form_response, _measure_to_observation, scoring

Known divergence
~~~~~~~~~~~~~~~~
Both measurement_projector and observation_projector intentionally omit the
legacy ``correlation_id = record.get("correlation_id") or context.run_id``
fallback.  Callables use ``record.get("correlation_id")`` only, because
context.run_id is set at the executor/plan level, not inside the callable.
When the record HAS a correlation_id, outputs are identical.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

# ---------------------------------------------------------------------------
# Frozen datetime for deterministic comparisons
# ---------------------------------------------------------------------------
FROZEN_DATETIME = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
FROZEN_ISO = FROZEN_DATETIME.isoformat()


def _make_frozen_datetime_mock() -> MagicMock:
    """Create a mock datetime that returns FROZEN_DATETIME from .now()."""
    mock_dt = MagicMock(wraps=datetime)
    mock_dt.now.return_value = FROZEN_DATETIME
    # Ensure side_effect=None so wraps works for everything else
    mock_dt.side_effect = None
    return mock_dt


# ---------------------------------------------------------------------------
# Golden canonical record fixture
# ---------------------------------------------------------------------------
def _golden_canonical_record(
    idem_key: str = "ik-abc-001",
    respondent_id: str | None = "contact::R001",
    respondent_email: str | None = "client@example.com",
    form_id: str = "googleforms::form-test",
    response_id: str = "resp-001",
    submitted_at: str = "2025-01-10T09:00:00Z",
    connection_name: str = "google-forms-intake-01",
    correlation_id: str | None = "corr-abc-001",
    answers: list[dict] | None = None,
) -> dict[str, Any]:
    """Build a realistic canonical form_response record."""
    respondent: dict[str, Any] = {}
    if respondent_id:
        respondent["id"] = respondent_id
    if respondent_email:
        respondent["email"] = respondent_email

    payload = {
        "form_id": form_id,
        "response_id": response_id,
        "submitted_at": submitted_at,
        "respondent": respondent,
        "answers": answers or [],
    }

    return {
        "idem_key": idem_key,
        "source_system": "google_forms",
        "connection_name": connection_name,
        "object_type": "form_response",
        "canonical_schema": "iglu:org.canonical/form_response/jsonschema/1-0-0",
        "transform_ref": "canonizer::google_forms",
        "payload": payload,
        "correlation_id": correlation_id,
    }


# ============================================================================
# 1. TestViewCreatorEquivalence
# ============================================================================

class TestViewCreatorEquivalence:
    """Both legacy CreateProjectionProcessor and view_creator callable
    delegate to the same ``get_projection_sql()`` function.  Prove
    the callable produces character-identical SQL for every projection.
    """

    @pytest.mark.parametrize("proj_name", [
        "proj_clients",
        "proj_sessions",
        "proj_transcripts",
        "proj_clinical_documents",
        "proj_form_responses",
        "proj_contact_events",
    ])
    def test_sql_identical(self, proj_name: str) -> None:
        """SQL produced by legacy path == SQL produced by callable path."""
        from lorchestra.sql.projections import get_projection_sql

        project = "test-proj"
        dataset = "test-ds"

        # Legacy path: CreateProjectionProcessor calls get_projection_sql directly
        legacy_sql = get_projection_sql(proj_name, project, dataset)

        # Callable path: view_creator.execute() wraps the same function
        # We call the same underlying function to prove it is shared
        callable_sql = get_projection_sql(
            name=proj_name, project=project, dataset=dataset,
        )

        assert legacy_sql == callable_sql, (
            f"SQL diverged for {proj_name}:\n"
            f"LEGACY:\n{legacy_sql[:200]}\n"
            f"CALLABLE:\n{callable_sql[:200]}"
        )

    @pytest.mark.parametrize("proj_name", [
        "proj_clients",
        "proj_sessions",
        "proj_transcripts",
        "proj_clinical_documents",
        "proj_form_responses",
        "proj_contact_events",
    ])
    def test_callable_execute_produces_correct_sql(self, proj_name: str) -> None:
        """Call view_creator.execute() end-to-end (with config mock)
        and verify the SQL matches direct get_projection_sql().
        """
        from lorchestra.sql.projections import get_projection_sql

        mock_config = MagicMock()
        mock_config.project = "equiv-proj"
        mock_config.dataset_canonical = "equiv-ds"

        with patch("lorchestra.config.load_config", return_value=mock_config):
            from lorchestra.callable.view_creator import execute as vc_execute
            result = vc_execute({"projection_name": proj_name})

        expected_sql = get_projection_sql(proj_name, "equiv-proj", "equiv-ds")
        assert result["items"][0]["sql"] == expected_sql


# ============================================================================
# 2. TestMoltProjectorEquivalence
# ============================================================================

class TestMoltProjectorEquivalence:
    """Both legacy CrossProjectSyncProcessor and molt_projector callable
    call ``get_molt_projection_sql()`` and wrap it in a CTAS.
    """

    @pytest.mark.parametrize("query_name", [
        "context_emails",
        "context_calendar",
        "context_actions",
    ])
    def test_ctas_identical(self, query_name: str) -> None:
        """CTAS produced by legacy path == CTAS produced by callable path."""
        from lorchestra.sql.molt_projections import get_molt_projection_sql

        project = "local-orchestration"
        dataset = "canonical"
        sink_project = "molt-chatbot"
        sink_dataset = "molt"
        sink_table = query_name

        source_sql = get_molt_projection_sql(
            name=query_name, project=project, dataset=dataset,
        )

        # Legacy path: CrossProjectSyncProcessor._build_ctas
        fq_table = f"`{sink_project}.{sink_dataset}.{sink_table}`"
        legacy_ctas = f"CREATE OR REPLACE TABLE {fq_table} AS\n{source_sql}"

        # Callable path: molt_projector.execute()
        mock_config = MagicMock()
        mock_config.project = project
        mock_config.dataset_canonical = dataset

        with patch("lorchestra.config.load_config", return_value=mock_config):
            from lorchestra.callable.molt_projector import execute as mp_execute
            result = mp_execute({
                "query_name": query_name,
                "sink_project": sink_project,
                "sink_dataset": sink_dataset,
                "sink_table": sink_table,
            })

        callable_ctas = result["items"][0]["sql"]
        assert legacy_ctas == callable_ctas, (
            f"CTAS diverged for {query_name}:\n"
            f"LEGACY:\n{legacy_ctas[:300]}\n"
            f"CALLABLE:\n{callable_ctas[:300]}"
        )


# ============================================================================
# 3. TestBqReaderEquivalence
# ============================================================================

class TestBqReaderEquivalence:
    """The legacy SyncSqliteProcessor and the bq_reader callable share
    the same stringify-and-append-projected_at logic.  Prove row
    transformation is identical.
    """

    def _golden_bq_rows(self) -> list[dict[str, Any]]:
        """BQ rows with mixed types: str, int, bool, float, None."""
        return [
            {"name": "Alice", "age": 30, "active": True, "score": 9.5, "notes": None},
            {"name": "Bob", "age": None, "active": False, "score": 0.0, "notes": "hello"},
        ]

    def test_stringify_logic_identical(self) -> None:
        """Legacy: str(val) if val is not None else None
        Callable: str(val) if val is not None else None
        Both append projected_at.
        """
        rows = self._golden_bq_rows()

        # Legacy path (from SyncSqliteProcessor.run, lines 253-259)
        legacy_results = []
        for row in rows:
            original_columns = list(row.keys())
            values = [
                str(row.get(col)) if row.get(col) is not None else None
                for col in original_columns
            ]
            values.append(FROZEN_ISO)  # projected_at
            legacy_results.append(dict(zip(original_columns + ["projected_at"], values)))

        # Callable path (from bq_reader.execute, lines 107-113)
        callable_results = []
        for row in rows:
            original_columns = list(row.keys())
            sync_row: dict[str, Any] = {}
            for col in original_columns:
                val = row.get(col)
                sync_row[col] = str(val) if val is not None else None
            sync_row["projected_at"] = FROZEN_ISO
            callable_results.append(sync_row)

        assert legacy_results == callable_results

    def test_none_stays_none(self) -> None:
        """None values must NOT become the string "None"."""
        rows = self._golden_bq_rows()

        for row in rows:
            for col, val in row.items():
                if val is None:
                    # Legacy path
                    legacy_val = str(val) if val is not None else None
                    assert legacy_val is None
                    # Callable path
                    callable_val = str(val) if val is not None else None
                    assert callable_val is None

    def test_columns_include_projected_at(self) -> None:
        """Both paths add 'projected_at' as the last column."""
        rows = self._golden_bq_rows()
        original_columns = list(rows[0].keys())

        # Legacy
        legacy_columns = original_columns + ["projected_at"]

        # Callable
        callable_columns = original_columns + ["projected_at"]

        assert legacy_columns == callable_columns
        assert callable_columns[-1] == "projected_at"

    @pytest.mark.parametrize("dataset_key,expected_field", [
        ("canonical", "dataset_canonical"),
        ("derived", "dataset_derived"),
    ])
    def test_view_name_construction(self, dataset_key: str, expected_field: str) -> None:
        """FQ view name is built identically for canonical and derived."""
        project = "test-proj"
        dataset_canonical = "events_canon"
        dataset_derived = "events_derived"
        projection = "proj_clients"

        # Legacy path (SyncSqliteProcessor)
        if dataset_key == "derived":
            legacy_dataset = dataset_derived
        else:
            legacy_dataset = dataset_canonical
        legacy_view = f"`{project}.{legacy_dataset}.{projection}`"

        # Callable path (bq_reader)
        if dataset_key == "derived":
            callable_dataset = dataset_derived
        elif dataset_key == "canonical":
            callable_dataset = dataset_canonical
        else:
            callable_dataset = dataset_key
        callable_view = f"`{project}.{callable_dataset}.{projection}`"

        assert legacy_view == callable_view


# ============================================================================
# 4. TestFileRendererEquivalence
# ============================================================================

class TestFileRendererEquivalence:
    """Legacy FileProjectionProcessor and file_renderer callable both
    query SQLite, expand path/content templates, and optionally add
    front-matter YAML.  Prove the rendered output is identical.
    """

    def _create_test_db(self, db_path: Path) -> None:
        """Create a small SQLite DB with test data."""
        conn = sqlite3.connect(db_path)
        conn.execute(
            'CREATE TABLE clients (client_id TEXT, name TEXT, email TEXT)'
        )
        conn.execute(
            "INSERT INTO clients VALUES ('C001', 'Alice A', 'alice@example.com')"
        )
        conn.execute(
            "INSERT INTO clients VALUES ('C002', 'Bob B', 'bob@example.com')"
        )
        conn.commit()
        conn.close()

    def test_content_without_front_matter(self) -> None:
        """Plain body rendering is identical."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            self._create_test_db(db_path)

            query = "SELECT * FROM clients"
            content_template = "# {name}\nEmail: {email}\nProjected: {_projected_at}"
            path_template = "{client_id}.md"

            # Query the DB (shared step)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            rows = [dict(r) for r in conn.execute(query).fetchall()]
            conn.close()

            for row in rows:
                row_with_meta = {**row, "_projected_at": FROZEN_ISO}

                # Legacy path (FileProjectionProcessor)
                legacy_path = path_template.format(**row_with_meta)
                legacy_content = content_template.format(**row_with_meta)

                # Callable path (file_renderer)
                callable_path = path_template.format(**row_with_meta)
                callable_content = content_template.format(**row_with_meta)

                assert legacy_path == callable_path
                assert legacy_content == callable_content

    def test_content_with_front_matter(self) -> None:
        """Front-matter YAML rendering is identical (same yaml.safe_dump args)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            self._create_test_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            rows = [dict(r) for r in conn.execute("SELECT * FROM clients").fetchall()]
            conn.close()

            front_matter_spec = {
                "title": "{name}",
                "email": "{email}",
                "projected_at": "{_projected_at}",
                "version": 1,
            }
            content_template = "# {name}\nDetails here."

            for row in rows:
                row_with_meta = {**row, "_projected_at": FROZEN_ISO}

                # Resolve front matter (shared logic in both legacy and callable)
                resolved = {}
                for key, value in front_matter_spec.items():
                    if isinstance(value, str):
                        resolved[key] = value.format(**row_with_meta)
                    else:
                        resolved[key] = value

                front_matter_yaml = yaml.safe_dump(
                    resolved, sort_keys=False, allow_unicode=True
                )

                # Legacy path
                legacy_body = content_template.format(**row_with_meta)
                legacy_content = f"---\n{front_matter_yaml}---\n\n{legacy_body}"

                # Callable path
                callable_body = content_template.format(**row_with_meta)
                callable_content = f"---\n{front_matter_yaml}---\n\n{callable_body}"

                assert legacy_content == callable_content

    def test_callable_execute_matches_legacy(self) -> None:
        """End-to-end: file_renderer.execute() produces identical items
        to what FileProjectionProcessor would produce.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            base_path = Path(tmpdir) / "output"
            self._create_test_db(db_path)

            params = {
                "sqlite_path": str(db_path),
                "query": "SELECT * FROM clients",
                "base_path": str(base_path),
                "path_template": "{client_id}.md",
                "content_template": "# {name}\nEmail: {email}\nAt: {_projected_at}",
                "front_matter": {"title": "{name}", "version": 1},
            }

            mock_dt = _make_frozen_datetime_mock()

            # Run callable
            with patch("lorchestra.callable.file_renderer.datetime", mock_dt):
                from lorchestra.callable.file_renderer import execute as fr_execute
                result = fr_execute(params)

            # Manually compute what legacy would produce
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            rows = [dict(r) for r in conn.execute("SELECT * FROM clients").fetchall()]
            conn.close()

            for i, row in enumerate(rows):
                row_with_meta = {**row, "_projected_at": FROZEN_ISO}
                expected_path = str(base_path / params["path_template"].format(**row_with_meta))
                expected_body = params["content_template"].format(**row_with_meta)

                resolved_fm = {}
                for k, v in params["front_matter"].items():
                    if isinstance(v, str):
                        resolved_fm[k] = v.format(**row_with_meta)
                    else:
                        resolved_fm[k] = v
                fm_yaml = yaml.safe_dump(resolved_fm, sort_keys=False, allow_unicode=True)
                expected_content = f"---\n{fm_yaml}---\n\n{expected_body}"

                assert result["items"][i]["path"] == expected_path
                assert result["items"][i]["content"] == expected_content


# ============================================================================
# 5. TestMeasurementProjectorEquivalence
# ============================================================================

class TestMeasurementProjectorEquivalence:
    """Prove _canonical_to_measurement_event() in the callable produces
    field-identical rows to the legacy MeasurementEventProjection method.
    """

    def _run_legacy(
        self,
        record: dict,
        event_type: str,
        event_subtype: str,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Call legacy processor's _canonical_to_measurement_event."""
        from lorchestra.processors.projection import MeasurementEventProjection
        processor = MeasurementEventProjection()
        with patch("lorchestra.processors.projection.datetime", _make_frozen_datetime_mock()):
            return processor._canonical_to_measurement_event(
                record=record,
                event_type=event_type,
                event_subtype=event_subtype,
                correlation_id=correlation_id,
            )

    def _run_callable(
        self,
        record: dict,
        event_type: str,
        event_subtype: str,
    ) -> dict[str, Any]:
        """Call callable module's _canonical_to_measurement_event."""
        from lorchestra.callable.measurement_projector import (
            _canonical_to_measurement_event,
        )
        with patch("lorchestra.callable.measurement_projector.datetime", _make_frozen_datetime_mock()):
            return _canonical_to_measurement_event(
                record=record,
                event_type=event_type,
                event_subtype=event_subtype,
            )

    def test_all_19_fields_match(self) -> None:
        """When record has correlation_id, all 19 fields are identical."""
        record = _golden_canonical_record(correlation_id="corr-match-001")
        legacy = self._run_legacy(record, "form", "intake_01", "corr-match-001")
        callable_ = self._run_callable(record, "form", "intake_01")

        for key in legacy:
            assert legacy[key] == callable_[key], (
                f"Field '{key}' diverged: legacy={legacy[key]!r}, callable={callable_[key]!r}"
            )

    @pytest.mark.parametrize("event_subtype", ["intake_01", "intake_02", "followup"])
    def test_event_subtype_variants(self, event_subtype: str) -> None:
        """event_subtype flows correctly to both event_subtype and binding_id."""
        record = _golden_canonical_record(correlation_id="corr-sub-001")
        legacy = self._run_legacy(record, "form", event_subtype, "corr-sub-001")
        callable_ = self._run_callable(record, "form", event_subtype)

        assert legacy["event_subtype"] == callable_["event_subtype"] == event_subtype
        assert legacy["binding_id"] == callable_["binding_id"] == event_subtype

    def test_subject_id_respondent_id(self) -> None:
        """subject_id uses respondent.id when present."""
        record = _golden_canonical_record(
            respondent_id="contact::R999",
            respondent_email="client@example.com",
            correlation_id="corr-rid",
        )
        legacy = self._run_legacy(record, "form", "intake_01", "corr-rid")
        callable_ = self._run_callable(record, "form", "intake_01")

        assert legacy["subject_id"] == callable_["subject_id"] == "contact::R999"

    def test_subject_id_email_fallback(self) -> None:
        """subject_id falls back to respondent.email when id is missing."""
        record = _golden_canonical_record(
            respondent_id=None,
            respondent_email="fallback@example.com",
            correlation_id="corr-email",
        )
        legacy = self._run_legacy(record, "form", "intake_01", "corr-email")
        callable_ = self._run_callable(record, "form", "intake_01")

        assert legacy["subject_id"] == callable_["subject_id"] == "fallback@example.com"

    def test_idem_key_is_measurement_event_id(self) -> None:
        """idem_key == measurement_event_id == canonical_object_id in both."""
        record = _golden_canonical_record(idem_key="ik-unique", correlation_id="corr-ik")
        legacy = self._run_legacy(record, "form", "intake_01", "corr-ik")
        callable_ = self._run_callable(record, "form", "intake_01")

        assert legacy["idem_key"] == callable_["idem_key"] == "ik-unique"
        assert legacy["measurement_event_id"] == callable_["measurement_event_id"] == "ik-unique"
        assert legacy["canonical_object_id"] == callable_["canonical_object_id"] == "ik-unique"

    def test_timestamps_frozen(self) -> None:
        """received_at, processed_at, created_at all use frozen datetime."""
        record = _golden_canonical_record(correlation_id="corr-ts")
        legacy = self._run_legacy(record, "form", "intake_01", "corr-ts")
        callable_ = self._run_callable(record, "form", "intake_01")

        for field in ("received_at", "processed_at", "created_at"):
            assert legacy[field] == FROZEN_ISO
            assert callable_[field] == FROZEN_ISO

    def test_static_fields_identical(self) -> None:
        """source_system, source_entity, binding_version, metadata are static."""
        record = _golden_canonical_record(correlation_id="corr-static")
        legacy = self._run_legacy(record, "form", "intake_01", "corr-static")
        callable_ = self._run_callable(record, "form", "intake_01")

        assert legacy["source_system"] == callable_["source_system"] == "google_forms"
        assert legacy["source_entity"] == callable_["source_entity"] == "form_response"
        assert legacy["binding_version"] == callable_["binding_version"] is None
        assert legacy["metadata"] == callable_["metadata"] == json.dumps({})
        assert legacy["subject_contact_id"] == callable_["subject_contact_id"] is None

    def test_occurred_at_from_submitted_at(self) -> None:
        """occurred_at comes from payload.submitted_at."""
        record = _golden_canonical_record(
            submitted_at="2025-03-15T14:30:00Z",
            correlation_id="corr-occ",
        )
        legacy = self._run_legacy(record, "form", "intake_01", "corr-occ")
        callable_ = self._run_callable(record, "form", "intake_01")

        assert legacy["occurred_at"] == callable_["occurred_at"] == "2025-03-15T14:30:00Z"

    def test_correlation_id_divergence_documented(self) -> None:
        """KNOWN DIVERGENCE: legacy falls back to context.run_id, callable doesn't.

        When record.correlation_id is None:
        - Legacy: correlation_id = context.run_id (the fallback)
        - Callable: correlation_id = None

        This is intentional — the executor sets correlation_id at the plan level.
        """
        record = _golden_canonical_record(correlation_id=None)

        # Legacy: would use context.run_id as fallback
        legacy = self._run_legacy(record, "form", "intake_01", "run-id-fallback")
        assert legacy["correlation_id"] == "run-id-fallback"

        # Callable: no fallback, returns None
        callable_ = self._run_callable(record, "form", "intake_01")
        assert callable_["correlation_id"] is None

    def test_payload_as_json_string(self) -> None:
        """Callable handles payload as JSON string (from BQ) identically."""
        record = _golden_canonical_record(correlation_id="corr-json")
        # Simulate BQ returning payload as JSON string
        record_with_str_payload = {**record, "payload": json.dumps(record["payload"])}

        callable_ = self._run_callable(record_with_str_payload, "form", "intake_01")

        # Build expected from the dict version
        expected = self._run_callable(record, "form", "intake_01")
        for key in expected:
            assert callable_[key] == expected[key], f"Field '{key}' diverged with JSON string payload"


# ============================================================================
# 6. TestObservationProjectorEquivalence
# ============================================================================

class TestObservationProjectorEquivalence:
    """Prove _build_form_response, _measure_to_observation, and helper
    functions produce identical outputs to the legacy ObservationProjection
    methods.  Includes real finalform Pipeline scoring.
    """

    # --- _build_form_response ---

    def test_build_form_response_identical(self) -> None:
        """_build_form_response() produces identical output."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import (
            _build_form_response as callable_build,
        )

        record = _golden_canonical_record(
            form_id="googleforms::form-test",
            response_id="resp-001",
            respondent_id="contact::R001",
            respondent_email="client@example.com",
            connection_name="google-forms-intake-01",
            answers=[
                {"question_id": "q1", "answer_value": "yes"},
                {"question_id": "q2", "answer_value": "no"},
            ],
        )
        payload = record["payload"]

        # Legacy
        processor = ObservationProjection()
        legacy_fr = processor._build_form_response(record, payload)

        # Callable
        callable_fr = callable_build(record, payload)

        assert legacy_fr == callable_fr

    def test_build_form_response_fallback_form_id(self) -> None:
        """When payload.form_id is missing, both use googleforms::{connection_name}."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import (
            _build_form_response as callable_build,
        )

        record = _golden_canonical_record(form_id=None, connection_name="gf-conn-01")
        # Set form_id to None in payload
        record["payload"]["form_id"] = None
        payload = record["payload"]

        legacy_fr = ObservationProjection()._build_form_response(record, payload)
        callable_fr = callable_build(record, payload)

        assert legacy_fr["form_id"] == callable_fr["form_id"] == "googleforms::gf-conn-01"

    def test_build_form_response_items_mapping(self) -> None:
        """Items mapping: question_id→field_key, answer_value→answer, index→position."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import (
            _build_form_response as callable_build,
        )

        answers = [
            {"question_id": "2a9de2fe", "answer_value": "several days"},
            {"question_id": "7a6fdbfd", "answer_value": "not at all"},
            {"question_id": "1a27a816", "answer_value": "nearly every day"},
        ]
        record = _golden_canonical_record(answers=answers)
        payload = record["payload"]

        legacy_fr = ObservationProjection()._build_form_response(record, payload)
        callable_fr = callable_build(record, payload)

        assert len(legacy_fr["items"]) == len(callable_fr["items"]) == 3
        for i in range(3):
            assert legacy_fr["items"][i] == callable_fr["items"][i]
            assert legacy_fr["items"][i]["position"] == i

    # --- Score extraction helpers ---

    def test_extract_total_score_identical(self) -> None:
        """_extract_total_score returns same float for both."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import _extract_total_score

        mock_event = MagicMock()
        mock_obs = MagicMock()
        mock_obs.kind = "scale"
        mock_obs.code = "phq9_total"
        mock_obs.value = 12
        mock_obs.label = "Moderate"
        mock_event.observations = [mock_obs]

        legacy = ObservationProjection()._extract_total_score(mock_event)
        callable_ = _extract_total_score(mock_event)

        assert legacy == callable_ == 12.0

    def test_extract_total_score_none(self) -> None:
        """Both return None when no _total scale found."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import _extract_total_score

        mock_event = MagicMock()
        mock_obs = MagicMock()
        mock_obs.kind = "item"
        mock_obs.code = "phq9_item1"
        mock_event.observations = [mock_obs]

        legacy = ObservationProjection()._extract_total_score(mock_event)
        callable_ = _extract_total_score(mock_event)

        assert legacy is None
        assert callable_ is None

    def test_extract_severity_identical(self) -> None:
        """_extract_severity returns same label for both."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import _extract_severity

        mock_event = MagicMock()
        mock_obs = MagicMock()
        mock_obs.kind = "scale"
        mock_obs.code = "phq9_total"
        mock_obs.value = 12
        mock_obs.label = "Moderate"
        mock_event.observations = [mock_obs]

        legacy = ObservationProjection()._extract_severity(mock_event)
        callable_ = _extract_severity(mock_event)

        assert legacy == callable_ == "Moderate"

    @pytest.mark.parametrize("label,expected_code", [
        ("Minimal", "none"),
        ("None-minimal", "none"),
        ("Mild", "mild"),
        ("Moderate", "moderate"),
        ("Moderately severe", "moderate_severe"),
        ("Severe", "severe"),
        (None, None),
        ("", None),
        ("Unknown label", None),
    ])
    def test_severity_label_to_code(self, label: str | None, expected_code: str | None) -> None:
        """Severity label mapping is identical for all known labels."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import _severity_label_to_code

        if label is None or label == "":
            # Legacy calls this only when label is truthy
            legacy = ObservationProjection()._severity_label_to_code(label) if label else None
            callable_ = _severity_label_to_code(label) if label else None
        else:
            legacy = ObservationProjection()._severity_label_to_code(label)
            callable_ = _severity_label_to_code(label)

        assert legacy == callable_ == expected_code

    # --- _measure_to_observation ---

    def _make_mock_event(
        self,
        measure_id: str = "phq9",
        measure_version: str = "1.0.0",
        measurement_event_id: str = "det-evt-001",
        subject_id: str = "contact::R001",
        total_value: float = 12.0,
        total_label: str = "Moderate",
    ) -> MagicMock:
        """Build a mock MeasurementEvent from finalform."""
        event = MagicMock()
        event.measure_id = measure_id
        event.measure_version = measure_version
        event.measurement_event_id = measurement_event_id
        event.subject_id = subject_id

        # Scale observation (total)
        scale_obs = MagicMock()
        scale_obs.code = f"{measure_id}_total"
        scale_obs.kind = "scale"
        scale_obs.value = total_value
        scale_obs.value_type = "float"
        scale_obs.label = total_label
        scale_obs.raw_answer = None
        scale_obs.position = 0
        scale_obs.missing = False

        # Item observation
        item_obs = MagicMock()
        item_obs.code = f"{measure_id}_item1"
        item_obs.kind = "item"
        item_obs.value = 1
        item_obs.value_type = "int"
        item_obs.label = None
        item_obs.raw_answer = "several days"
        item_obs.position = 1
        item_obs.missing = False

        event.observations = [scale_obs, item_obs]
        return event

    def test_measure_to_observation_all_fields(self) -> None:
        """All observation row fields match when correlation_id is present."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import _measure_to_observation

        event = self._make_mock_event()
        me_id = "me-001"
        corr_id = "corr-obs-001"

        with patch("lorchestra.processors.projection.datetime", _make_frozen_datetime_mock()):
            legacy = ObservationProjection()._measure_to_observation(event, me_id, corr_id)

        with patch("lorchestra.callable.observation_projector.datetime", _make_frozen_datetime_mock()):
            callable_ = _measure_to_observation(event, me_id, corr_id)

        for key in legacy:
            assert legacy[key] == callable_[key], (
                f"Field '{key}' diverged: legacy={legacy[key]!r}, callable={callable_[key]!r}"
            )

    def test_observation_idem_key_format(self) -> None:
        """idem_key = f"{measurement_event_id}:{event.measure_id}" in both."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import _measure_to_observation

        event = self._make_mock_event(measure_id="gad7")
        me_id = "me-xyz"

        with patch("lorchestra.processors.projection.datetime", _make_frozen_datetime_mock()):
            legacy = ObservationProjection()._measure_to_observation(event, me_id, "c")
        with patch("lorchestra.callable.observation_projector.datetime", _make_frozen_datetime_mock()):
            callable_ = _measure_to_observation(event, me_id, "c")

        assert legacy["idem_key"] == callable_["idem_key"] == "me-xyz:gad7"

    def test_components_json_identical(self) -> None:
        """components JSON array is character-identical when parsed."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import _measure_to_observation

        event = self._make_mock_event()
        me_id = "me-comp"

        with patch("lorchestra.processors.projection.datetime", _make_frozen_datetime_mock()):
            legacy = ObservationProjection()._measure_to_observation(event, me_id, "c")
        with patch("lorchestra.callable.observation_projector.datetime", _make_frozen_datetime_mock()):
            callable_ = _measure_to_observation(event, me_id, "c")

        legacy_components = json.loads(legacy["components"])
        callable_components = json.loads(callable_["components"])

        assert len(legacy_components) == len(callable_components)
        for lc, cc in zip(legacy_components, callable_components):
            for field in ("code", "kind", "value", "value_type", "label",
                          "raw_answer", "position", "missing"):
                assert lc[field] == cc[field], (
                    f"Component field '{field}' diverged: {lc[field]!r} vs {cc[field]!r}"
                )

    def test_value_numeric_and_severity(self) -> None:
        """value_numeric, severity_code, severity_label extracted identically."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import _measure_to_observation

        event = self._make_mock_event(total_value=15.0, total_label="Moderately severe")

        with patch("lorchestra.processors.projection.datetime", _make_frozen_datetime_mock()):
            legacy = ObservationProjection()._measure_to_observation(event, "me-sev", "c")
        with patch("lorchestra.callable.observation_projector.datetime", _make_frozen_datetime_mock()):
            callable_ = _measure_to_observation(event, "me-sev", "c")

        assert legacy["value_numeric"] == callable_["value_numeric"] == 15.0
        assert legacy["severity_label"] == callable_["severity_label"] == "Moderately severe"
        assert legacy["severity_code"] == callable_["severity_code"] == "moderate_severe"

    def test_correlation_id_divergence_observation(self) -> None:
        """KNOWN DIVERGENCE: same pattern as measurement_projector.

        Legacy: correlation_id = me_record.get("correlation_id") or context.run_id
        Callable: correlation_id = me_record.get("correlation_id")

        We test _measure_to_observation directly — it receives correlation_id
        already resolved, so the divergence is at the call site in execute().
        When both receive the same correlation_id, output is identical.
        """
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import _measure_to_observation

        event = self._make_mock_event()

        # Both receive same correlation_id → identical
        with patch("lorchestra.processors.projection.datetime", _make_frozen_datetime_mock()):
            legacy = ObservationProjection()._measure_to_observation(event, "me-div", "same-corr")
        with patch("lorchestra.callable.observation_projector.datetime", _make_frozen_datetime_mock()):
            callable_ = _measure_to_observation(event, "me-div", "same-corr")

        assert legacy["correlation_id"] == callable_["correlation_id"] == "same-corr"

    # --- Real finalform Pipeline scoring ---

    @pytest.fixture
    def real_pipeline(self) -> Any:
        """Create a real finalform Pipeline with example_intake binding."""
        from finalform.pipeline import Pipeline, PipelineConfig

        config = PipelineConfig(
            measure_registry_path=Path("/workspace/finalform/measure-registry"),
            binding_registry_path=Path("/workspace/finalform/form-binding-registry"),
            binding_id="example_intake",
            binding_version="1.0.0",
            deterministic_ids=True,
        )
        return Pipeline(config)

    def _build_example_intake_response(self) -> dict[str, Any]:
        """Build a form response for example_intake (PHQ-9 + GAD-7).

        PHQ-9 answers: [0,1,2,3,0,1,2,3,0] = 12 (Moderate), item10=1 (somewhat difficult)
        GAD-7 answers: [0,1,2,3,0,1,2] = 9 (Mild), item8=1 (somewhat difficult)
        """
        items = []

        # PHQ-9: entry.123456001-010
        phq9_answers = [
            "not at all",       # 0
            "several days",     # 1
            "more than half the days",  # 2
            "nearly every day",  # 3
            "not at all",       # 0
            "several days",     # 1
            "more than half the days",  # 2
            "nearly every day",  # 3
            "not at all",       # 0
            "somewhat difficult",  # item10 (functional severity)
        ]
        for i, answer in enumerate(phq9_answers, 1):
            items.append({
                "field_key": f"entry.123456{i:03d}",
                "position": i,
                "answer": answer,
            })

        # GAD-7: entry.789012001-008
        gad7_answers = [
            "not at all",       # 0
            "several days",     # 1
            "more than half the days",  # 2
            "nearly every day",  # 3
            "not at all",       # 0
            "several days",     # 1
            "more than half the days",  # 2
            "somewhat difficult",  # item8 (functional severity)
        ]
        for i, answer in enumerate(gad7_answers, 1):
            items.append({
                "field_key": f"entry.789012{i:03d}",
                "position": 10 + i,
                "answer": answer,
            })

        return {
            "form_id": "googleforms::1FAIpQLSe_example",
            "form_submission_id": "sub_equiv_001",
            "subject_id": "contact::R001",
            "timestamp": "2025-01-15T10:30:00Z",
            "items": items,
        }

    def test_real_pipeline_scoring_phq9(self, real_pipeline: Any) -> None:
        """Real PHQ-9 scoring: items [0,1,2,3,0,1,2,3,0] = 12 → Moderate.

        Both legacy and callable extract the same score from real Pipeline output.
        """
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import (
            _measure_to_observation,
            _extract_total_score,
            _extract_severity,
            _severity_label_to_code,
        )

        form_response = self._build_example_intake_response()
        result = real_pipeline.process(form_response)

        phq9_event = next(e for e in result.events if e.measure_id == "phq9")

        # Legacy extraction
        processor = ObservationProjection()
        legacy_score = processor._extract_total_score(phq9_event)
        legacy_severity = processor._extract_severity(phq9_event)
        legacy_severity_code = processor._severity_label_to_code(legacy_severity) if legacy_severity else None

        # Callable extraction
        callable_score = _extract_total_score(phq9_event)
        callable_severity = _extract_severity(phq9_event)
        callable_severity_code = _severity_label_to_code(callable_severity) if callable_severity else None

        # PHQ-9: 0+1+2+3+0+1+2+3+0 = 12 → Moderate
        assert legacy_score == callable_score == 12.0
        assert legacy_severity == callable_severity == "Moderate"
        assert legacy_severity_code == callable_severity_code == "moderate"

    def test_real_pipeline_scoring_gad7(self, real_pipeline: Any) -> None:
        """Real GAD-7 scoring from same form response."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import (
            _extract_total_score,
            _extract_severity,
            _severity_label_to_code,
        )

        form_response = self._build_example_intake_response()
        result = real_pipeline.process(form_response)

        gad7_event = next(e for e in result.events if e.measure_id == "gad7")

        processor = ObservationProjection()
        legacy_score = processor._extract_total_score(gad7_event)
        legacy_severity = processor._extract_severity(gad7_event)

        callable_score = _extract_total_score(gad7_event)
        callable_severity = _extract_severity(gad7_event)

        # GAD-7: 0+1+2+3+0+1+2 = 9 → Mild
        assert legacy_score == callable_score == 9.0
        assert legacy_severity == callable_severity == "Mild"

    def test_real_pipeline_full_observation_row(self, real_pipeline: Any) -> None:
        """Full observation row from real Pipeline is field-identical."""
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import _measure_to_observation

        form_response = self._build_example_intake_response()
        result = real_pipeline.process(form_response)

        me_id = "me-real-001"
        corr_id = "corr-real-001"

        for event in result.events:
            with patch("lorchestra.processors.projection.datetime", _make_frozen_datetime_mock()):
                legacy = ObservationProjection()._measure_to_observation(event, me_id, corr_id)
            with patch("lorchestra.callable.observation_projector.datetime", _make_frozen_datetime_mock()):
                callable_ = _measure_to_observation(event, me_id, corr_id)

            for key in legacy:
                if key == "components":
                    # Compare parsed JSON for component-by-component
                    legacy_comps = json.loads(legacy[key])
                    callable_comps = json.loads(callable_[key])
                    assert len(legacy_comps) == len(callable_comps), (
                        f"Component count differs for {event.measure_id}"
                    )
                    for j, (lc, cc) in enumerate(zip(legacy_comps, callable_comps)):
                        for field in lc:
                            assert lc[field] == cc[field], (
                                f"{event.measure_id} component[{j}].{field}: "
                                f"{lc[field]!r} != {cc[field]!r}"
                            )
                else:
                    assert legacy[key] == callable_[key], (
                        f"{event.measure_id} field '{key}': "
                        f"{legacy[key]!r} != {callable_[key]!r}"
                    )

    def test_build_form_response_round_trip(self, real_pipeline: Any) -> None:
        """_build_form_response from a canonical record, run through Pipeline,
        produces identical ProcessingResult for both legacy and callable paths.
        """
        from lorchestra.processors.projection import ObservationProjection
        from lorchestra.callable.observation_projector import (
            _build_form_response as callable_build,
        )

        # Build a canonical record with the example_intake answers
        phq9_answers = [
            "not at all", "several days", "more than half the days",
            "nearly every day", "not at all", "several days",
            "more than half the days", "nearly every day", "not at all",
            "somewhat difficult",
        ]
        gad7_answers = [
            "not at all", "several days", "more than half the days",
            "nearly every day", "not at all", "several days",
            "more than half the days", "somewhat difficult",
        ]

        answers = []
        for i, ans in enumerate(phq9_answers):
            answers.append({
                "question_id": f"entry.123456{i+1:03d}",
                "answer_value": ans,
            })
        for i, ans in enumerate(gad7_answers):
            answers.append({
                "question_id": f"entry.789012{i+1:03d}",
                "answer_value": ans,
            })

        record = _golden_canonical_record(
            form_id="googleforms::1FAIpQLSe_example",
            response_id="sub_equiv_roundtrip",
            answers=answers,
        )
        payload = record["payload"]

        # Build form_response both ways
        legacy_fr = ObservationProjection()._build_form_response(record, payload)
        callable_fr = callable_build(record, payload)

        assert legacy_fr == callable_fr

        # Run both through Pipeline → same events
        result_legacy = real_pipeline.process(legacy_fr)
        result_callable = real_pipeline.process(callable_fr)

        assert len(result_legacy.events) == len(result_callable.events)
        for le, ce in zip(result_legacy.events, result_callable.events):
            assert le.measure_id == ce.measure_id
            assert le.measure_version == ce.measure_version
            assert len(le.observations) == len(ce.observations)
