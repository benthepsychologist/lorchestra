"""Gate 05d: Validate file_renderer callable and file_projection YAML jobs.

Confirms:
- file_renderer.execute() reads SQLite, renders templates, returns file.write items
- Front matter is rendered correctly (YAML format)
- Empty results handled
- Missing params raise ValueError
- file_renderer is registered in dispatch
- All 6 file projection YAML job defs load and compile correctly
- Pipeline integration: file_renderer items -> plan.build -> file.write plan
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lorchestra.callable.result import CallableResult

DEFINITIONS_DIR = Path(__file__).resolve().parent.parent.parent / "lorchestra" / "jobs" / "definitions"


# ============================================================================
# file_renderer callable
# ============================================================================


class TestFileRenderer:
    """Test file_renderer.execute() with temp SQLite databases."""

    def _create_test_db(self, tmp_path, table, columns, rows):
        """Create a test SQLite DB and return its path."""
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
        conn.execute(f'CREATE TABLE "{table}" ({col_defs})')
        placeholders = ", ".join("?" * len(columns))
        for row in rows:
            values = [row.get(c) for c in columns]
            conn.execute(f'INSERT INTO "{table}" VALUES ({placeholders})', values)
        conn.commit()
        conn.close()
        return db_path

    def test_returns_callable_result(self, tmp_path):
        from lorchestra.callable.file_renderer import execute

        db_path = self._create_test_db(tmp_path, "clients", ["name", "id"], [
            {"name": "Alice", "id": "c1"},
            {"name": "Bob", "id": "c2"},
        ])

        result = execute({
            "sqlite_path": db_path,
            "query": "SELECT name, id FROM clients ORDER BY name",
            "base_path": str(tmp_path / "output"),
            "path_template": "{name}.md",
            "content_template": "# {name}",
        })

        assert result["schema_version"] == "1.0"
        assert len(result["items"]) == 2

    def test_renders_path_and_content(self, tmp_path):
        from lorchestra.callable.file_renderer import execute

        db_path = self._create_test_db(tmp_path, "t", ["name", "age"], [
            {"name": "Alice", "age": "30"},
        ])

        result = execute({
            "sqlite_path": db_path,
            "query": "SELECT name, age FROM t",
            "base_path": str(tmp_path / "out"),
            "path_template": "{name}/info.md",
            "content_template": "# {name}\n\nAge: {age}",
        })

        item = result["items"][0]
        assert item["path"] == str(tmp_path / "out" / "Alice" / "info.md")
        assert item["content"] == "# Alice\n\nAge: 30"

    def test_renders_front_matter(self, tmp_path):
        from lorchestra.callable.file_renderer import execute

        db_path = self._create_test_db(tmp_path, "t", ["name", "id"], [
            {"name": "Alice", "id": "c1"},
        ])

        result = execute({
            "sqlite_path": db_path,
            "query": "SELECT name, id FROM t",
            "base_path": str(tmp_path / "out"),
            "path_template": "{name}.md",
            "content_template": "# {name}",
            "front_matter": {
                "entity": "contacts",
                "record_id": "{id}",
                "projected_at": "{_projected_at}",
            },
        })

        content = result["items"][0]["content"]
        assert content.startswith("---\n")
        assert "entity: contacts" in content
        assert "record_id: c1" in content
        assert "projected_at:" in content
        assert "---\n\n# Alice" in content

    def test_front_matter_with_nested_dict(self, tmp_path):
        """Non-string values in front_matter are passed through."""
        from lorchestra.callable.file_renderer import execute

        db_path = self._create_test_db(tmp_path, "t", ["name"], [
            {"name": "Alice"},
        ])

        result = execute({
            "sqlite_path": db_path,
            "query": "SELECT name FROM t",
            "base_path": str(tmp_path / "out"),
            "path_template": "{name}.md",
            "content_template": "body",
            "front_matter": {
                "editable_fields": {"field_a": "body"},
            },
        })

        content = result["items"][0]["content"]
        assert "editable_fields:" in content
        assert "field_a: body" in content

    def test_empty_result(self, tmp_path):
        from lorchestra.callable.file_renderer import execute

        db_path = self._create_test_db(tmp_path, "t", ["name"], [])

        result = execute({
            "sqlite_path": db_path,
            "query": "SELECT name FROM t",
            "base_path": str(tmp_path / "out"),
            "path_template": "{name}.md",
            "content_template": "body",
        })

        assert result["items"] == []
        assert result["stats"]["output"] == 0

    def test_missing_params_raises(self):
        from lorchestra.callable.file_renderer import execute

        with pytest.raises(ValueError, match="Missing required params"):
            execute({"sqlite_path": "/tmp/test.db", "query": "SELECT 1"})

    def test_stats(self, tmp_path):
        from lorchestra.callable.file_renderer import execute

        db_path = self._create_test_db(tmp_path, "t", ["name"], [
            {"name": "A"}, {"name": "B"}, {"name": "C"},
        ])

        result = execute({
            "sqlite_path": db_path,
            "query": "SELECT name FROM t",
            "base_path": str(tmp_path / "out"),
            "path_template": "{name}.md",
            "content_template": "body",
        })

        assert result["stats"]["input"] == 3
        assert result["stats"]["output"] == 3


# ============================================================================
# Dispatch integration
# ============================================================================


class TestDispatchIntegration:
    """Verify file_renderer is registered and dispatchable."""

    def test_file_renderer_registered(self):
        from lorchestra.callable.dispatch import get_callables
        callables = get_callables()
        assert "file_renderer" in callables


# ============================================================================
# YAML job definition loading and compilation
# ============================================================================


FILE_JOB_IDS = [
    "file_proj_clients",
    "file_proj_reports",
    "file_proj_session_files",
    "file_proj_session_notes",
    "file_proj_session_summaries",
    "file_proj_transcripts",
]


class TestYamlJobDefs:
    """Verify YAML job definitions load and compile correctly."""

    @pytest.mark.parametrize("job_id", FILE_JOB_IDS)
    def test_file_yaml_loads(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        assert job_def.job_id == job_id
        assert job_def.version == "2.0"
        assert len(job_def.steps) == 3

    @pytest.mark.parametrize("job_id", FILE_JOB_IDS)
    def test_file_yaml_has_correct_steps(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        steps = job_def.steps
        assert steps[0].op == "call"
        assert steps[0].params["callable"] == "file_renderer"
        assert steps[1].op == "plan.build"
        assert steps[1].params["method"] == "file.write"
        assert steps[2].op == "storacle.submit"

    @pytest.mark.parametrize("job_id", FILE_JOB_IDS)
    def test_file_yaml_compiles(self, job_id):
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
    """Test file_renderer -> plan.build pipeline."""

    def test_file_renderer_to_plan(self, tmp_path):
        """file_renderer items feed into plan.build with file.write method."""
        from lorchestra.callable.file_renderer import execute
        from lorchestra.plan_builder import build_plan_from_items

        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.execute('CREATE TABLE t (name TEXT, id TEXT)')
        conn.execute("INSERT INTO t VALUES ('Alice', 'c1')")
        conn.execute("INSERT INTO t VALUES ('Bob', 'c2')")
        conn.commit()
        conn.close()

        result = execute({
            "sqlite_path": db_path,
            "query": "SELECT name, id FROM t ORDER BY name",
            "base_path": str(tmp_path / "out"),
            "path_template": "{name}.md",
            "content_template": "# {name}",
        })
        items = result["items"]

        plan = build_plan_from_items(
            items=items,
            correlation_id="test_05d",
            method="file.write",
        )

        assert plan.kind == "storacle.plan"
        assert len(plan.ops) == 2
        assert plan.ops[0].method == "file.write"
        assert plan.ops[0].params["path"].endswith("Alice.md")
        assert plan.ops[0].params["content"] == "# Alice"
        assert plan.ops[1].params["path"].endswith("Bob.md")
        assert plan.ops[0].idempotency_key is None
