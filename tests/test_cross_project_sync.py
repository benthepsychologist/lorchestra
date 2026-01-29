"""Tests for CrossProjectSyncProcessor."""

import pytest
from unittest.mock import MagicMock

from lorchestra.processors.base import JobContext
from lorchestra.processors.projection import CrossProjectSyncProcessor


class TestCrossProjectSyncProcessor:
    """Tests for CrossProjectSyncProcessor."""

    @pytest.fixture
    def processor(self):
        return CrossProjectSyncProcessor()

    @pytest.fixture
    def mock_context(self, test_config):
        return JobContext(
            bq_client=MagicMock(),
            run_id="test-run-123",
            config=test_config,
            dry_run=False,
            test_table=False,
        )

    @pytest.fixture
    def mock_storage_client(self):
        client = MagicMock()
        client.execute_sql.return_value = {"rows_affected": 0, "total_rows": 42}
        return client

    @pytest.fixture
    def mock_event_client(self):
        return MagicMock()

    @pytest.fixture
    def base_job_spec(self):
        return {
            "job_id": "sync_molt_emails",
            "job_type": "sync_bq_cross_project",
            "source": {
                "query_name": "context_emails",
            },
            "sink": {
                "project": "molt-chatbot",
                "dataset": "molt",
                "table": "context_emails",
            },
        }

    def test_executes_ctas_with_correct_target(
        self, processor, mock_context, mock_storage_client, mock_event_client, base_job_spec
    ):
        """Processor executes CREATE OR REPLACE TABLE targeting the sink project."""
        processor.run(base_job_spec, mock_context, mock_storage_client, mock_event_client)

        mock_storage_client.execute_sql.assert_called_once()
        sql = mock_storage_client.execute_sql.call_args[0][0]
        assert sql.startswith("CREATE OR REPLACE TABLE `molt-chatbot.molt.context_emails`")

    def test_resolves_query_name_from_molt_projections(
        self, processor, mock_context, mock_storage_client, mock_event_client, base_job_spec
    ):
        """Processor loads SQL from molt_projections module via query_name."""
        processor.run(base_job_spec, mock_context, mock_storage_client, mock_event_client)

        sql = mock_storage_client.execute_sql.call_args[0][0]
        # The resolved SQL should contain canonical_objects reference with test config
        assert "canonical_objects" in sql
        assert "test-project" in sql
        assert "test_canonical" in sql

    def test_supports_inline_sql(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """Processor accepts inline SQL in source.sql."""
        job_spec = {
            "job_id": "sync_custom",
            "job_type": "sync_bq_cross_project",
            "source": {
                "sql": "SELECT 1 AS test_col",
            },
            "sink": {
                "project": "target-project",
                "dataset": "target_ds",
                "table": "target_table",
            },
        }

        processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

        sql = mock_storage_client.execute_sql.call_args[0][0]
        assert "CREATE OR REPLACE TABLE `target-project.target_ds.target_table`" in sql
        assert "SELECT 1 AS test_col" in sql

    def test_raises_if_no_sql_or_query_name(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """Processor raises ValueError if source has neither sql nor query_name."""
        job_spec = {
            "job_id": "sync_bad",
            "job_type": "sync_bq_cross_project",
            "source": {},
            "sink": {
                "project": "molt-chatbot",
                "dataset": "molt",
                "table": "context_emails",
            },
        }

        with pytest.raises(ValueError, match="source must have 'sql' or 'query_name'"):
            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

    def test_raises_on_unknown_query_name(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """Processor raises KeyError for unknown query_name."""
        job_spec = {
            "job_id": "sync_bad",
            "job_type": "sync_bq_cross_project",
            "source": {"query_name": "nonexistent_query"},
            "sink": {
                "project": "molt-chatbot",
                "dataset": "molt",
                "table": "context_emails",
            },
        }

        with pytest.raises(KeyError, match="Unknown molt projection"):
            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

    def test_dry_run_skips_execution(
        self, processor, mock_storage_client, mock_event_client, test_config, base_job_spec
    ):
        """Processor skips SQL execution in dry run mode."""
        context = JobContext(
            bq_client=MagicMock(),
            run_id="test-run-dry",
            config=test_config,
            dry_run=True,
            test_table=False,
        )

        processor.run(base_job_spec, context, mock_storage_client, mock_event_client)

        mock_storage_client.execute_sql.assert_not_called()

        # Should emit dry_run event
        event_calls = mock_event_client.log_event.call_args_list
        event_types = [c.kwargs["event_type"] for c in event_calls]
        assert "projection.cross_project.dry_run" in event_types

    def test_logs_started_and_completed_events(
        self, processor, mock_context, mock_storage_client, mock_event_client, base_job_spec
    ):
        """Processor emits started and completed lifecycle events."""
        processor.run(base_job_spec, mock_context, mock_storage_client, mock_event_client)

        assert mock_event_client.log_event.call_count == 2

        started = mock_event_client.log_event.call_args_list[0]
        assert started.kwargs["event_type"] == "projection.cross_project.started"
        assert started.kwargs["status"] == "success"
        assert started.kwargs["payload"]["job_id"] == "sync_molt_emails"

        completed = mock_event_client.log_event.call_args_list[1]
        assert completed.kwargs["event_type"] == "projection.cross_project.completed"
        assert completed.kwargs["status"] == "success"
        assert completed.kwargs["payload"]["total_rows"] == 42

    def test_logs_failure_event_on_error(
        self, processor, mock_context, mock_storage_client, mock_event_client, base_job_spec
    ):
        """Processor emits failure event and re-raises on error."""
        mock_storage_client.execute_sql.side_effect = RuntimeError("BQ permission denied")

        with pytest.raises(RuntimeError, match="BQ permission denied"):
            processor.run(base_job_spec, mock_context, mock_storage_client, mock_event_client)

        failed = mock_event_client.log_event.call_args_list[-1]
        assert failed.kwargs["event_type"] == "projection.cross_project.failed"
        assert failed.kwargs["status"] == "failed"
        assert "BQ permission denied" in failed.kwargs["error_message"]

    def test_all_three_molt_queries_execute(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """All three molt projection query_names resolve and execute."""
        for query_name, table in [
            ("context_emails", "context_emails"),
            ("context_calendar", "context_calendar"),
            ("context_actions", "context_actions"),
        ]:
            mock_storage_client.reset_mock()
            mock_event_client.reset_mock()

            job_spec = {
                "job_id": f"sync_molt_{query_name}",
                "job_type": "sync_bq_cross_project",
                "source": {"query_name": query_name},
                "sink": {
                    "project": "molt-chatbot",
                    "dataset": "molt",
                    "table": table,
                },
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            sql = mock_storage_client.execute_sql.call_args[0][0]
            assert f"CREATE OR REPLACE TABLE `molt-chatbot.molt.{table}`" in sql
