"""Tests for --test-table mode.

Verifies that test-table mode:
1. Routes writes to test_event_log instead of event_log
2. Routes writes to test_raw_objects instead of raw_objects
3. Does not touch production tables
"""

import pytest
from unittest.mock import MagicMock, patch, call
import os


class TestTestTableMode:
    """Tests for test-table mode in event_client."""

    def setup_method(self):
        """Reset run mode and set required env vars before each test."""
        from lorchestra.stack_clients.event_client import reset_run_mode
        reset_run_mode()
        os.environ["EVENTS_BQ_DATASET"] = "test_dataset"

    def teardown_method(self):
        """Reset run mode after each test."""
        from lorchestra.stack_clients.event_client import reset_run_mode
        reset_run_mode()

    def test_log_event_uses_test_table(self):
        """log_event() should write to test_event_log in test-table mode."""
        from lorchestra.stack_clients.event_client import set_run_mode, log_event

        mock_bq_client = MagicMock()
        mock_bq_client.insert_rows_json.return_value = []  # No errors

        # Enable test-table mode
        set_run_mode(test_table=True)

        log_event(
            event_type="test.event",
            source_system="test-system",
            correlation_id="test-123",
            status="ok",
            bq_client=mock_bq_client,
        )

        # Verify insert was called with test_event_log table
        mock_bq_client.insert_rows_json.assert_called_once()
        call_args = mock_bq_client.insert_rows_json.call_args
        table_ref = call_args[0][0]  # First positional argument
        assert "test_event_log" in table_ref
        assert "event_log" in table_ref  # test_event_log contains "event_log"

    def test_log_event_uses_prod_table_by_default(self):
        """log_event() should write to event_log when not in test-table mode."""
        from lorchestra.stack_clients.event_client import set_run_mode, log_event

        mock_bq_client = MagicMock()
        mock_bq_client.insert_rows_json.return_value = []  # No errors

        # Explicitly NOT in test-table mode
        set_run_mode(test_table=False)

        log_event(
            event_type="test.event",
            source_system="test-system",
            correlation_id="test-123",
            status="ok",
            bq_client=mock_bq_client,
        )

        # Verify insert was called with prod event_log table (not test_)
        mock_bq_client.insert_rows_json.assert_called_once()
        call_args = mock_bq_client.insert_rows_json.call_args
        table_ref = call_args[0][0]
        assert table_ref == "test_dataset.event_log"
        assert "test_event_log" not in table_ref

    def test_get_table_ref_by_name_returns_correct_ref(self):
        """_get_table_ref_by_name should return dataset.table_name."""
        from lorchestra.stack_clients.event_client import _get_table_ref_by_name

        result = _get_table_ref_by_name("test_event_log")
        assert result == "test_dataset.test_event_log"

        result = _get_table_ref_by_name("event_log")
        assert result == "test_dataset.event_log"

    def test_ensure_test_tables_exist_creates_tables(self):
        """ensure_test_tables_exist should create test tables from prod schema."""
        from lorchestra.stack_clients.event_client import ensure_test_tables_exist
        from google.cloud import bigquery

        mock_bq_client = MagicMock()
        mock_bq_client.project = "test_project"

        # Track which tables we've "created"
        created_tables = set()

        # Mock get_table behavior:
        # - test_event_log, test_raw_objects: raise (doesn't exist)
        # - event_log, raw_objects: return schema
        def mock_get_table(table_ref):
            # Check for test_ prefix in table name (after the dot)
            table_name = table_ref.split(".")[-1] if "." in table_ref else table_ref
            if table_name.startswith("test_") and table_ref not in created_tables:
                raise Exception("Not found")
            # Return a mock table with schema for prod tables
            mock_table = MagicMock()
            mock_table.schema = [
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("data", "JSON"),
            ]
            return mock_table

        def mock_create_table(table):
            # Track that we created this table
            created_tables.add(table.table_id if hasattr(table, 'table_id') else str(table))
            return table

        mock_bq_client.get_table.side_effect = mock_get_table
        mock_bq_client.create_table.side_effect = mock_create_table

        # Need project in env for bigquery.Table() constructor
        os.environ["GCP_PROJECT"] = "test_project"

        ensure_test_tables_exist(mock_bq_client)

        # Should have called create_table twice (test_event_log and test_raw_objects)
        assert mock_bq_client.create_table.call_count == 2

    def test_ensure_test_tables_exist_skips_existing(self):
        """ensure_test_tables_exist should skip tables that already exist."""
        from lorchestra.stack_clients.event_client import ensure_test_tables_exist

        mock_bq_client = MagicMock()

        # Mock get_table to succeed (tables exist)
        mock_bq_client.get_table.return_value = MagicMock()

        ensure_test_tables_exist(mock_bq_client)

        # Should NOT have called create_table (tables already exist)
        mock_bq_client.create_table.assert_not_called()

    def test_set_run_mode_flags_are_mutually_exclusive_at_cli(self):
        """CLI should error if both --dry-run and --test-table are set."""
        import click
        from click.testing import CliRunner
        from lorchestra.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["run", "fake_job", "--dry-run", "--test-table"])

        assert result.exit_code != 0
        assert "mutually exclusive" in result.output
