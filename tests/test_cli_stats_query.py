"""Tests for lorchestra stats, query, and sql CLI commands."""
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner


class TestStatsCommands:
    """Tests for lorchestra stats commands."""

    def test_stats_canonical_help(self):
        """Test stats canonical command shows help."""
        from lorchestra.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["stats", "canonical", "--help"])

        assert result.exit_code == 0
        assert "canonical objects" in result.output.lower()

    def test_stats_raw_help(self):
        """Test stats raw command shows help."""
        from lorchestra.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["stats", "raw", "--help"])

        assert result.exit_code == 0
        assert "raw objects" in result.output.lower()

    def test_stats_jobs_help(self):
        """Test stats jobs command shows help."""
        from lorchestra.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["stats", "jobs", "--help"])

        assert result.exit_code == 0
        assert "--days" in result.output

    def test_stats_canonical_executes(self):
        """Test stats canonical command executes SQL."""
        from lorchestra.cli import main

        runner = CliRunner()

        env = {"GCP_PROJECT": "test-project", "EVENTS_BQ_DATASET": "test_dataset"}
        with patch.dict(os.environ, env, clear=False):
            with patch("lorchestra.sql_runner.run_sql_query") as mock_run:
                result = runner.invoke(main, ["stats", "canonical"])

        assert result.exit_code == 0
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "canonical_objects" in call_args
        assert "canonical_schema" in call_args

    def test_stats_raw_executes(self):
        """Test stats raw command executes SQL."""
        from lorchestra.cli import main

        runner = CliRunner()

        env = {"GCP_PROJECT": "test-project", "EVENTS_BQ_DATASET": "test_dataset"}
        with patch.dict(os.environ, env, clear=False):
            with patch("lorchestra.sql_runner.run_sql_query") as mock_run:
                result = runner.invoke(main, ["stats", "raw"])

        assert result.exit_code == 0
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "raw_objects" in call_args
        assert "validation_status" in call_args

    def test_stats_jobs_executes_with_days(self):
        """Test stats jobs command passes days parameter."""
        from lorchestra.cli import main

        runner = CliRunner()

        env = {"GCP_PROJECT": "test-project", "EVENTS_BQ_DATASET": "test_dataset"}
        with patch.dict(os.environ, env, clear=False):
            with patch("lorchestra.sql_runner.run_sql_query") as mock_run:
                result = runner.invoke(main, ["stats", "jobs", "--days", "14"])

        assert result.exit_code == 0
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["extra_placeholders"]["DAYS"] == "14"


class TestQueryCommand:
    """Tests for lorchestra query command."""

    def test_query_help(self):
        """Test query command shows help."""
        from lorchestra.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["query", "--help"])

        assert result.exit_code == 0
        assert "named SQL query" in result.output

    def test_query_executes_sql_file(self, tmp_path):
        """Test query command loads and executes SQL file."""
        from lorchestra.cli import main

        runner = CliRunner()

        # Create a temp queries directory with a test query
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        (queries_dir / "test-query.sql").write_text(
            "SELECT * FROM `${PROJECT}.${DATASET}.test_table`"
        )

        env = {"GCP_PROJECT": "test-project", "EVENTS_BQ_DATASET": "test_dataset"}
        with patch.dict(os.environ, env, clear=False):
            with patch("lorchestra.cli.QUERIES_DIR", queries_dir):
                with patch("lorchestra.sql_runner.run_sql_query") as mock_run:
                    result = runner.invoke(main, ["query", "test-query"])

        assert result.exit_code == 0
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "test_table" in call_args

    def test_query_not_found(self, tmp_path):
        """Test query command shows error for missing query."""
        from lorchestra.cli import main

        runner = CliRunner()

        # Create an empty queries directory
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()

        with patch("lorchestra.cli.QUERIES_DIR", queries_dir):
            result = runner.invoke(main, ["query", "nonexistent"])

        assert result.exit_code == 1
        assert "not found" in result.output

    def test_query_lists_available_queries(self, tmp_path):
        """Test query command lists available queries on error."""
        from lorchestra.cli import main

        runner = CliRunner()

        # Create a queries directory with some queries
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        (queries_dir / "query-a.sql").write_text("SELECT 1")
        (queries_dir / "query-b.sql").write_text("SELECT 2")

        with patch("lorchestra.cli.QUERIES_DIR", queries_dir):
            result = runner.invoke(main, ["query", "nonexistent"])

        assert result.exit_code == 1
        assert "query-a" in result.output
        assert "query-b" in result.output


class TestSqlCommand:
    """Tests for lorchestra sql command."""

    def test_sql_help(self):
        """Test sql command shows help."""
        from lorchestra.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["sql", "--help"])

        assert result.exit_code == 0
        assert "ad-hoc" in result.output.lower()

    def test_sql_with_argument(self):
        """Test sql command with SQL as argument."""
        from lorchestra.cli import main

        runner = CliRunner()

        env = {"GCP_PROJECT": "test-project", "EVENTS_BQ_DATASET": "test_dataset"}
        with patch.dict(os.environ, env, clear=False):
            with patch("lorchestra.sql_runner.run_sql_query") as mock_run:
                result = runner.invoke(main, ["sql", "SELECT 1"])

        assert result.exit_code == 0
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == "SELECT 1"

    def test_sql_with_stdin(self):
        """Test sql command with SQL from stdin."""
        from lorchestra.cli import main

        runner = CliRunner()

        env = {"GCP_PROJECT": "test-project", "EVENTS_BQ_DATASET": "test_dataset"}
        with patch.dict(os.environ, env, clear=False):
            with patch("lorchestra.sql_runner.run_sql_query") as mock_run:
                result = runner.invoke(main, ["sql"], input="SELECT 2\n")

        assert result.exit_code == 0
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == "SELECT 2"

    def test_sql_no_input_error(self):
        """Test sql command shows error when no SQL provided."""
        from lorchestra.cli import main

        runner = CliRunner()

        # CliRunner provides stdin by default, so we get "Empty SQL" error
        result = runner.invoke(main, ["sql"])

        assert result.exit_code != 0
        # Either "No SQL provided" (actual tty) or "Empty SQL" (CliRunner)
        assert "SQL" in result.output

    def test_sql_empty_input_error(self):
        """Test sql command shows error for empty SQL."""
        from lorchestra.cli import main

        runner = CliRunner()

        result = runner.invoke(main, ["sql"], input="\n")

        assert result.exit_code != 0
        assert "Empty SQL" in result.output

    def test_sql_rejects_mutating_query(self):
        """Test sql command rejects non-read-only SQL."""
        from lorchestra.cli import main

        runner = CliRunner()

        env = {"GCP_PROJECT": "test-project", "EVENTS_BQ_DATASET": "test_dataset"}
        with patch.dict(os.environ, env, clear=False):
            result = runner.invoke(main, ["sql", "INSERT INTO t VALUES (1)"])

        assert result.exit_code != 0
        assert "must start with SELECT or WITH" in result.output
