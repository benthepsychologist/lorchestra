"""Tests for --dry-run mode.

Verifies that dry-run mode:
1. Does not call any BigQuery write methods
2. Logs sample records and idem_keys
3. Consumes the iterator once for sampling
"""

import pytest
from unittest.mock import MagicMock, patch


class TestDryRunMode:
    """Tests for dry-run mode in event_client."""

    def setup_method(self):
        """Reset run mode before each test."""
        from lorchestra.stack_clients.event_client import reset_run_mode
        reset_run_mode()

    def teardown_method(self):
        """Reset run mode after each test."""
        from lorchestra.stack_clients.event_client import reset_run_mode
        reset_run_mode()

    def test_log_event_skips_bq_in_dry_run(self):
        """log_event() should not call BigQuery in dry-run mode."""
        from lorchestra.stack_clients.event_client import set_run_mode, log_event

        # Create a mock BQ client
        mock_bq_client = MagicMock()

        # Enable dry-run mode
        set_run_mode(dry_run=True)

        # Call log_event - should NOT call insert_rows_json
        log_event(
            event_type="test.event",
            source_system="test-system",
            correlation_id="test-123",
            status="ok",
            payload={"test": "data"},
            bq_client=mock_bq_client,
        )

        # Verify no BQ calls were made
        mock_bq_client.insert_rows_json.assert_not_called()

    def test_upsert_objects_skips_bq_in_dry_run(self):
        """upsert_objects() should not call BigQuery in dry-run mode."""
        from lorchestra.stack_clients.event_client import set_run_mode, upsert_objects

        # Create a mock BQ client
        mock_bq_client = MagicMock()

        # Enable dry-run mode
        set_run_mode(dry_run=True)

        # Test data
        test_objects = [
            {"id": "obj1", "name": "Object 1"},
            {"id": "obj2", "name": "Object 2"},
            {"id": "obj3", "name": "Object 3"},
        ]

        # Call upsert_objects - should NOT call load_table_from_json
        upsert_objects(
            objects=test_objects,
            source_system="test-system",
            object_type="test_object",
            correlation_id="test-123",
            idem_key_fn=lambda x: f"test:{x['id']}",
            bq_client=mock_bq_client,
        )

        # Verify no BQ calls were made
        mock_bq_client.load_table_from_json.assert_not_called()
        mock_bq_client.query.assert_not_called()

    def test_dry_run_consumes_iterator_once(self):
        """Dry-run should consume iterator once for sampling."""
        from lorchestra.stack_clients.event_client import set_run_mode, upsert_objects

        mock_bq_client = MagicMock()
        set_run_mode(dry_run=True)

        # Use a generator that tracks consumption
        consumed_count = 0

        def object_generator():
            nonlocal consumed_count
            for i in range(5):
                consumed_count += 1
                yield {"id": f"obj{i}", "name": f"Object {i}"}

        # Call upsert_objects with the generator
        upsert_objects(
            objects=object_generator(),
            source_system="test-system",
            object_type="test_object",
            correlation_id="test-123",
            idem_key_fn=lambda x: f"test:{x['id']}",
            bq_client=mock_bq_client,
        )

        # All 5 objects should have been consumed (for counting)
        assert consumed_count == 5

    def test_dry_run_samples_first_three_records(self, caplog):
        """Dry-run should log first 3 sample idem_keys."""
        import logging
        from lorchestra.stack_clients.event_client import set_run_mode, upsert_objects

        mock_bq_client = MagicMock()
        set_run_mode(dry_run=True)

        test_objects = [
            {"id": f"obj{i}", "name": f"Object {i}"}
            for i in range(10)
        ]

        with caplog.at_level(logging.INFO):
            upsert_objects(
                objects=test_objects,
                source_system="test-system",
                object_type="test_object",
                correlation_id="test-123",
                idem_key_fn=lambda x: f"test:{x['id']}",
                bq_client=mock_bq_client,
            )

        # Check that we logged the count
        assert "Would upsert 10 test_object objects" in caplog.text

        # Check that we logged sample idem_keys (first 3)
        assert "Sample 1 idem_key=test:obj0" in caplog.text
        assert "Sample 2 idem_key=test:obj1" in caplog.text
        assert "Sample 3 idem_key=test:obj2" in caplog.text

    def test_normal_mode_calls_bq(self):
        """Without dry-run, log_event should call BigQuery."""
        from lorchestra.stack_clients.event_client import set_run_mode, log_event
        import os

        # Set required env var
        os.environ["EVENTS_BQ_DATASET"] = "test_dataset"

        mock_bq_client = MagicMock()
        mock_bq_client.insert_rows_json.return_value = []  # No errors

        # Explicitly NOT in dry-run mode
        set_run_mode(dry_run=False)

        log_event(
            event_type="test.event",
            source_system="test-system",
            correlation_id="test-123",
            status="ok",
            bq_client=mock_bq_client,
        )

        # Should have called BQ
        mock_bq_client.insert_rows_json.assert_called_once()
