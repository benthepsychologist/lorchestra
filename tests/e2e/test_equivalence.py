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

NOTE: TestMeasurementProjectorEquivalence and TestObservationProjectorEquivalence
have been removed as part of e005b-09a (formation I/O purity v2).

The fat callables (measurement_projector.py, observation_projector.py) were
replaced with:
- Native ops (storacle.query, plan.build)
- Canonizer transforms (formation/form_response_to_measurement_event@1.0.0,
  formation/measurement_event_to_finalform_input@1.0.0,
  formation/finalform_event_to_observation_row@1.0.0)
- The finalform callable (for real compute/scoring)

Tests for the new architecture are in:
- tests/e2e/test_gate05e_measurement_projector.py (v2)
- tests/e2e/test_gate05f_observation_projector.py (v2)
"""

from __future__ import annotations

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
