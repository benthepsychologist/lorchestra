"""Tests for lorchestra.sql_runner module."""
import os
from unittest.mock import MagicMock, patch

import click
import pytest


class TestValidateReadonlySql:
    """Tests for validate_readonly_sql function."""

    def test_accepts_simple_select(self):
        """Test that simple SELECT query passes."""
        from lorchestra.sql_runner import validate_readonly_sql

        validate_readonly_sql("SELECT * FROM table")

    def test_accepts_with_cte(self):
        """Test that WITH (CTE) query passes."""
        from lorchestra.sql_runner import validate_readonly_sql

        validate_readonly_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")

    def test_accepts_select_with_trailing_semicolon(self):
        """Test that trailing semicolon is allowed."""
        from lorchestra.sql_runner import validate_readonly_sql

        validate_readonly_sql("SELECT * FROM table;")

    def test_rejects_insert(self):
        """Test that INSERT is rejected."""
        from lorchestra.sql_runner import validate_readonly_sql

        with pytest.raises(click.UsageError) as exc_info:
            validate_readonly_sql("INSERT INTO table VALUES (1)")
        assert "must start with SELECT or WITH" in str(exc_info.value)

    def test_rejects_update(self):
        """Test that UPDATE is rejected."""
        from lorchestra.sql_runner import validate_readonly_sql

        with pytest.raises(click.UsageError) as exc_info:
            validate_readonly_sql("UPDATE table SET col = 1")
        assert "must start with SELECT or WITH" in str(exc_info.value)

    def test_rejects_delete(self):
        """Test that DELETE is rejected."""
        from lorchestra.sql_runner import validate_readonly_sql

        with pytest.raises(click.UsageError) as exc_info:
            validate_readonly_sql("DELETE FROM table")
        assert "must start with SELECT or WITH" in str(exc_info.value)

    def test_rejects_drop(self):
        """Test that DROP is rejected."""
        from lorchestra.sql_runner import validate_readonly_sql

        with pytest.raises(click.UsageError) as exc_info:
            validate_readonly_sql("DROP TABLE table")
        assert "must start with SELECT or WITH" in str(exc_info.value)

    def test_rejects_create(self):
        """Test that CREATE is rejected."""
        from lorchestra.sql_runner import validate_readonly_sql

        with pytest.raises(click.UsageError) as exc_info:
            validate_readonly_sql("CREATE TABLE table (col INT)")
        assert "must start with SELECT or WITH" in str(exc_info.value)

    def test_rejects_truncate(self):
        """Test that TRUNCATE is rejected."""
        from lorchestra.sql_runner import validate_readonly_sql

        with pytest.raises(click.UsageError) as exc_info:
            validate_readonly_sql("TRUNCATE TABLE table")
        assert "must start with SELECT or WITH" in str(exc_info.value)

    def test_rejects_merge_in_subquery(self):
        """Test that MERGE keyword anywhere is rejected."""
        from lorchestra.sql_runner import validate_readonly_sql

        # Even in a CTE context, MERGE should be rejected
        with pytest.raises(click.UsageError) as exc_info:
            validate_readonly_sql("WITH t AS (SELECT 1) MERGE INTO table USING t")
        assert "non-read-only SQL" in str(exc_info.value)

    def test_rejects_multi_statement(self):
        """Test that multi-statement SQL is rejected."""
        from lorchestra.sql_runner import validate_readonly_sql

        with pytest.raises(click.UsageError) as exc_info:
            validate_readonly_sql("SELECT 1; SELECT 2")
        assert "Multi-statement SQL not allowed" in str(exc_info.value)

    def test_strips_sql_comments(self):
        """Test that SQL comments are stripped before validation."""
        from lorchestra.sql_runner import validate_readonly_sql

        # Should pass - INSERT is in a comment
        validate_readonly_sql("-- INSERT INTO table\nSELECT * FROM table")
        validate_readonly_sql("/* INSERT INTO table */ SELECT * FROM table")

    def test_handles_mixed_case(self):
        """Test that validation is case-insensitive."""
        from lorchestra.sql_runner import validate_readonly_sql

        validate_readonly_sql("SeLeCt * FROM table")
        validate_readonly_sql("WiTh cte AS (SELECT 1) SELECT * FROM cte")


class TestSubstitutePlaceholders:
    """Tests for substitute_placeholders function."""

    def test_substitutes_project_and_dataset(self):
        """Test basic placeholder substitution."""
        from lorchestra.sql_runner import substitute_placeholders

        env = {"GCP_PROJECT": "my-project", "EVENTS_BQ_DATASET": "my_dataset"}
        with patch.dict(os.environ, env, clear=False):
            result = substitute_placeholders(
                "SELECT * FROM `${PROJECT}.${DATASET}.table`"
            )
        assert result == "SELECT * FROM `my-project.my_dataset.table`"

    def test_substitutes_extra_placeholders(self):
        """Test extra placeholder substitution."""
        from lorchestra.sql_runner import substitute_placeholders

        env = {"GCP_PROJECT": "my-project", "EVENTS_BQ_DATASET": "my_dataset"}
        with patch.dict(os.environ, env, clear=False):
            result = substitute_placeholders(
                "SELECT * FROM table WHERE days < ${DAYS}",
                extra={"DAYS": "30"},
            )
        assert result == "SELECT * FROM table WHERE days < 30"

    def test_fails_without_gcp_project(self):
        """Test that missing GCP_PROJECT raises error."""
        from lorchestra.sql_runner import substitute_placeholders

        env = {"EVENTS_BQ_DATASET": "my_dataset"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(click.UsageError) as exc_info:
                substitute_placeholders("SELECT * FROM table")
            assert "GCP_PROJECT" in str(exc_info.value)

    def test_fails_without_events_bq_dataset(self):
        """Test that missing EVENTS_BQ_DATASET raises error."""
        from lorchestra.sql_runner import substitute_placeholders

        env = {"GCP_PROJECT": "my-project"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(click.UsageError) as exc_info:
                substitute_placeholders("SELECT * FROM table")
            assert "EVENTS_BQ_DATASET" in str(exc_info.value)


class TestRunSqlQuery:
    """Tests for run_sql_query function."""

    def test_executes_valid_query(self):
        """Test that valid query is executed."""
        from lorchestra.sql_runner import run_sql_query

        mock_client = MagicMock()
        mock_result = MagicMock()
        # Create schema fields with proper .name attribute
        field1 = MagicMock()
        field1.name = "col1"
        field2 = MagicMock()
        field2.name = "col2"
        mock_result.schema = [field1, field2]
        # Create a row that returns values by column name
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda s, k: "val1" if k == "col1" else "val2"
        mock_result.__iter__ = lambda self: iter([mock_row])
        mock_result.total_rows = 1
        mock_client.query.return_value.result.return_value = mock_result

        env = {"GCP_PROJECT": "my-project", "EVENTS_BQ_DATASET": "my_dataset"}
        with patch.dict(os.environ, env, clear=False):
            with patch("google.cloud.bigquery.Client", return_value=mock_client):
                run_sql_query("SELECT col1, col2 FROM table")

        mock_client.query.assert_called_once()

    def test_rejects_invalid_query(self):
        """Test that invalid query raises error before execution."""
        from lorchestra.sql_runner import run_sql_query

        env = {"GCP_PROJECT": "my-project", "EVENTS_BQ_DATASET": "my_dataset"}
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(click.UsageError):
                run_sql_query("INSERT INTO table VALUES (1)")
