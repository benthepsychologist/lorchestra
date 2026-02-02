"""Gate 05a: Validate new storacle RPC method handlers.

Confirms:
- bq.execute handler accepts SQL and returns rows_affected (dry_run)
- bq.upsert handler accepts rows/key_columns and returns rows_written (dry_run)
- sqlite.sync handler writes rows to SQLite table (full replace)
- file.write handler writes content to filesystem path
- plan_builder creates plans with each new method string
- New methods are classified as write methods in storacle RPC
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lorchestra.plan_builder import build_plan_from_items


# ============================================================================
# plan_builder: verify method passes through for all new methods
# ============================================================================


class TestPlanBuilderMethods:
    """Verify plan_builder creates plans with new method strings."""

    def test_bq_execute_method_in_plan(self):
        """plan_builder creates plan with bq.execute method."""
        items = [{"sql": "CREATE OR REPLACE VIEW proj_clients AS SELECT 1"}]
        plan = build_plan_from_items(items, correlation_id="test_05a", method="bq.execute")

        assert plan.kind == "storacle.plan"
        assert len(plan.ops) == 1
        assert plan.ops[0].method == "bq.execute"
        assert plan.ops[0].params["sql"].startswith("CREATE")
        # Non-wal.append methods should NOT have idempotency key
        assert plan.ops[0].idempotency_key is None

    def test_bq_upsert_method_in_plan(self):
        """plan_builder creates plan with bq.upsert method."""
        items = [{"idem_key": "k1", "value": "v1"}, {"idem_key": "k2", "value": "v2"}]
        plan = build_plan_from_items(items, correlation_id="test_05a", method="bq.upsert")

        assert len(plan.ops) == 2
        assert plan.ops[0].method == "bq.upsert"
        assert plan.ops[1].method == "bq.upsert"
        assert plan.ops[0].idempotency_key is None

    def test_sqlite_sync_method_in_plan(self):
        """plan_builder creates plan with sqlite.sync method."""
        items = [{"sqlite_path": "/tmp/test.db", "table": "clients", "columns": ["id"], "rows": []}]
        plan = build_plan_from_items(items, correlation_id="test_05a", method="sqlite.sync")

        assert len(plan.ops) == 1
        assert plan.ops[0].method == "sqlite.sync"
        assert plan.ops[0].idempotency_key is None

    def test_file_write_method_in_plan(self):
        """plan_builder creates plan with file.write method."""
        items = [{"path": "/tmp/test.md", "content": "# Hello"}]
        plan = build_plan_from_items(items, correlation_id="test_05a", method="file.write")

        assert len(plan.ops) == 1
        assert plan.ops[0].method == "file.write"
        assert plan.ops[0].idempotency_key is None

    def test_wal_append_still_gets_idempotency_key(self):
        """Verify wal.append still computes idempotency keys (regression)."""
        items = [{"idem_key": "k1", "value": "v1"}]
        plan = build_plan_from_items(items, correlation_id="test_05a", method="wal.append")

        assert plan.ops[0].method == "wal.append"
        assert plan.ops[0].idempotency_key is not None
        assert plan.ops[0].idempotency_key.startswith("sha256:")


# ============================================================================
# Handler unit tests (imported from storacle — uses fixture for import)
# ============================================================================


@pytest.fixture
def rpc_handlers():
    """Import storacle RPC handlers.

    Uses a fixture (not module-level import) because pytest's sys.path can
    shadow /workspace/storacle with tests/storacle/ at collection time.
    The conftest autouse fixture fixes sys.path at test-run time.
    """
    from storacle.rpc import (
        _handle_bq_execute,
        _handle_bq_upsert,
        _handle_sqlite_sync,
        _handle_file_write,
        _is_write_method,
    )
    return {
        "bq_execute": _handle_bq_execute,
        "bq_upsert": _handle_bq_upsert,
        "sqlite_sync": _handle_sqlite_sync,
        "file_write": _handle_file_write,
        "is_write": _is_write_method,
    }


class TestBqExecuteHandler:
    """Test _handle_bq_execute handler."""

    def test_dry_run_returns_zero_rows(self, rpc_handlers):
        handler = rpc_handlers["bq_execute"]
        result = handler({"sql": "CREATE VIEW test AS SELECT 1"}, bq_client=None, dry_run=True)
        assert result["dry_run"] is True
        assert result["rows_affected"] == 0

    def test_missing_sql_raises(self, rpc_handlers):
        handler = rpc_handlers["bq_execute"]
        with pytest.raises(ValueError, match="Missing required params"):
            handler({}, bq_client=None, dry_run=True)


class TestBqUpsertHandler:
    """Test _handle_bq_upsert handler."""

    def test_dry_run_returns_row_count(self, rpc_handlers):
        handler = rpc_handlers["bq_upsert"]
        rows = [{"id": "1", "name": "test"}, {"id": "2", "name": "test2"}]
        result = handler(
            {"dataset": "d", "table": "t", "rows": rows, "key_columns": ["id"]},
            bq_client=None,
            dry_run=True,
        )
        assert result["dry_run"] is True
        assert result["rows_written"] == 2

    def test_missing_params_raises(self, rpc_handlers):
        handler = rpc_handlers["bq_upsert"]
        with pytest.raises(ValueError, match="Missing required params"):
            handler({"dataset": "d"}, bq_client=None, dry_run=True)

    def test_invalid_rows_type_raises(self, rpc_handlers):
        handler = rpc_handlers["bq_upsert"]
        with pytest.raises(ValueError, match="rows must be a list"):
            handler(
                {"dataset": "d", "table": "t", "rows": "not a list", "key_columns": ["id"]},
                bq_client=None,
                dry_run=True,
            )


class TestSqliteSyncHandler:
    """Test _handle_sqlite_sync handler."""

    def test_dry_run_returns_row_count(self, rpc_handlers):
        handler = rpc_handlers["sqlite_sync"]
        result = handler(
            {"sqlite_path": "/tmp/test.db", "table": "t", "columns": ["a"], "rows": [{"a": "1"}]},
            dry_run=True,
        )
        assert result["dry_run"] is True
        assert result["rows_written"] == 1

    def test_writes_to_sqlite(self, rpc_handlers, tmp_path):
        """Actually writes rows to a SQLite file."""
        handler = rpc_handlers["sqlite_sync"]
        db_path = str(tmp_path / "test.db")
        rows = [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]
        columns = ["name", "age"]

        result = handler(
            {"sqlite_path": db_path, "table": "people", "columns": columns, "rows": rows},
            dry_run=False,
        )

        assert result["rows_written"] == 2
        assert result["dry_run"] is False

        # Verify data actually written
        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT name, age FROM people ORDER BY name")
        actual = cursor.fetchall()
        conn.close()

        assert actual == [("Alice", "30"), ("Bob", "25")]

    def test_full_replace_drops_old_data(self, rpc_handlers, tmp_path):
        """Second sync replaces all data (not append)."""
        handler = rpc_handlers["sqlite_sync"]
        db_path = str(tmp_path / "test.db")
        columns = ["id", "val"]

        # First write
        handler(
            {"sqlite_path": db_path, "table": "t", "columns": columns, "rows": [{"id": "1", "val": "a"}]},
            dry_run=False,
        )

        # Second write (full replace)
        handler(
            {"sqlite_path": db_path, "table": "t", "columns": columns, "rows": [{"id": "2", "val": "b"}]},
            dry_run=False,
        )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT id, val FROM t")
        actual = cursor.fetchall()
        conn.close()

        # Should only have second batch, not both
        assert actual == [("2", "b")]

    def test_missing_params_raises(self, rpc_handlers):
        handler = rpc_handlers["sqlite_sync"]
        with pytest.raises(ValueError, match="Missing required params"):
            handler({"sqlite_path": "/tmp/test.db"}, dry_run=True)


class TestFileWriteHandler:
    """Test _handle_file_write handler."""

    def test_dry_run_returns_bytes(self, rpc_handlers):
        handler = rpc_handlers["file_write"]
        result = handler({"path": "/tmp/test.md", "content": "# Hello"}, dry_run=True)
        assert result["dry_run"] is True
        assert result["bytes_written"] == 7

    def test_writes_file(self, rpc_handlers, tmp_path):
        """Actually writes content to a file."""
        handler = rpc_handlers["file_write"]
        file_path = str(tmp_path / "subdir" / "test.md")
        content = "# Test\n\nHello world."

        result = handler({"path": file_path, "content": content}, dry_run=False)

        assert result["dry_run"] is False
        assert result["bytes_written"] == len(content)

        # Verify file actually written
        assert Path(file_path).read_text() == content

    def test_missing_params_raises(self, rpc_handlers):
        handler = rpc_handlers["file_write"]
        with pytest.raises(ValueError, match="Missing required params"):
            handler({"path": "/tmp/test.md"}, dry_run=True)


class TestWriteMethodClassification:
    """Verify new methods are classified as write methods."""

    def test_new_methods_are_write_methods(self, rpc_handlers):
        is_write = rpc_handlers["is_write"]
        assert is_write("bq.execute") is True
        assert is_write("bq.upsert") is True
        assert is_write("sqlite.sync") is True
        assert is_write("file.write") is True

    def test_existing_methods_still_classified(self, rpc_handlers):
        is_write = rpc_handlers["is_write"]
        assert is_write("wal.append") is True
        assert is_write("sheets.write_table") is True
        assert is_write("log.append") is True

    def test_non_write_methods(self, rpc_handlers):
        is_write = rpc_handlers["is_write"]
        assert is_write("assert.exists") is False
        assert is_write("unknown.method") is False
