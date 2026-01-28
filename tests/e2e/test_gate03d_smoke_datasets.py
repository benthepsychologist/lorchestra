"""E2E tests for Gate 03d: Smoke Test Datasets + Table Prefix.

Tests the smoke namespace functionality across lorchestra and storacle.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from lorchestra.stack_clients.event_client import (
    _get_table_ref_by_name,
    set_run_mode,
    reset_run_mode,
    get_smoke_namespace,
)
from lorchestra.cli import _cleanup_smoke_dataset


class TestSmokeTableRouting:
    """Test that smoke mode routes tables correctly."""

    def setup_method(self):
        """Reset run mode before each test."""
        reset_run_mode()

    def teardown_method(self):
        """Reset run mode after each test."""
        reset_run_mode()

    def test_normal_mode_returns_standard_table_ref(self, monkeypatch):
        """Without smoke mode, returns standard dataset.table_name."""
        monkeypatch.setenv("EVENTS_BQ_DATASET", "raw")
        set_run_mode(dry_run=False, test_table=False, smoke_namespace=None)

        ref = _get_table_ref_by_name("event_log")
        assert ref == "raw.event_log"

    def test_smoke_mode_uses_smoke_dataset_and_prefix(self):
        """With smoke mode, uses smoke_<ns> dataset and prefixes table."""
        set_run_mode(dry_run=False, test_table=False, smoke_namespace="run_001")

        ref = _get_table_ref_by_name("event_log")
        assert ref == "smoke_run_001.smoke_run_001__event_log"

    def test_smoke_mode_with_raw_objects(self):
        """Smoke mode applies to raw_objects table too."""
        set_run_mode(dry_run=False, test_table=False, smoke_namespace="test_20260128")

        ref = _get_table_ref_by_name("raw_objects")
        assert ref == "smoke_test_20260128.smoke_test_20260128__raw_objects"

    def test_smoke_mode_ignores_dataset_override(self):
        """Smoke mode ignores explicit dataset parameter."""
        set_run_mode(dry_run=False, test_table=False, smoke_namespace="run_001")

        # Even with explicit dataset, smoke mode takes precedence
        ref = _get_table_ref_by_name("event_log", dataset="custom_dataset")
        assert ref == "smoke_run_001.smoke_run_001__event_log"

    def test_get_smoke_namespace_returns_current(self):
        """get_smoke_namespace returns the current namespace."""
        set_run_mode(smoke_namespace="my_run")
        assert get_smoke_namespace() == "my_run"

    def test_get_smoke_namespace_none_in_normal_mode(self):
        """get_smoke_namespace returns None in normal mode."""
        set_run_mode(smoke_namespace=None)
        assert get_smoke_namespace() is None


class TestStoracleSmokeConfig:
    """Test storacle config smoke namespace functionality."""

    def test_smoke_config_effective_datasets(self):
        """Smoke namespace changes effective datasets."""
        from storacle.config import StoracleConfig

        config = StoracleConfig(
            namespace_salt="test",
            wal_dataset="wal",
            ops_dataset="ops",
            smoke_namespace="run_001",
        )

        assert config.effective_wal_dataset == "smoke_run_001"
        assert config.effective_ops_dataset == "smoke_run_001"
        assert config.table_prefix == "smoke_run_001__"

    def test_smoke_config_normal_mode(self):
        """Without smoke, effective datasets equal configured datasets."""
        from storacle.config import StoracleConfig

        config = StoracleConfig(
            namespace_salt="test",
            wal_dataset="my_wal",
            ops_dataset="my_ops",
        )

        assert config.effective_wal_dataset == "my_wal"
        assert config.effective_ops_dataset == "my_ops"
        assert config.table_prefix == ""

    def test_smoke_config_from_env(self, monkeypatch):
        """Smoke namespace loaded from STORACLE_SMOKE_NAMESPACE."""
        from storacle.config import StoracleConfig

        monkeypatch.setenv("STORACLE_NAMESPACE_SALT", "test-salt")
        monkeypatch.setenv("STORACLE_SMOKE_NAMESPACE", "env_run_001")

        config = StoracleConfig.from_env()
        assert config.smoke_namespace == "env_run_001"
        assert config.effective_wal_dataset == "smoke_env_run_001"

    def test_smoke_namespace_validation(self):
        """Invalid smoke namespace characters raise ValueError."""
        from storacle.config import StoracleConfig

        with pytest.raises(ValueError, match="lowercase letters"):
            StoracleConfig(namespace_salt="test", smoke_namespace="Invalid-Name")


class TestSmokeCleanup:
    """Test smoke dataset cleanup functionality."""

    def test_cleanup_refuses_non_smoke_datasets(self):
        """Cleanup refuses to delete non-smoke datasets."""
        mock_client = MagicMock()

        with pytest.raises(ValueError, match="only smoke_"):
            _cleanup_smoke_dataset(mock_client, "my-project", "production_data")

    def test_cleanup_accepts_smoke_datasets(self):
        """Cleanup accepts smoke_* datasets."""
        mock_client = MagicMock()

        # Should not raise
        _cleanup_smoke_dataset(mock_client, "my-project", "smoke_run_001")

        mock_client.delete_dataset.assert_called_once_with(
            "my-project.smoke_run_001",
            delete_contents=True,
            not_found_ok=False,
        )

    def test_cleanup_handles_not_found(self):
        """Cleanup returns False when dataset doesn't exist."""
        from google.cloud.exceptions import NotFound

        mock_client = MagicMock()
        mock_client.delete_dataset.side_effect = NotFound("Dataset not found")

        result = _cleanup_smoke_dataset(mock_client, "my-project", "smoke_old_run")
        assert result is False


class TestStoracleRPCWithSmoke:
    """Test that storacle RPC uses smoke config correctly."""

    def test_build_bq_store_uses_table_prefix(self, monkeypatch):
        """_build_bq_store applies table_prefix from config."""
        from storacle.rpc import _build_bq_store
        from storacle.config import StoracleConfig

        config = StoracleConfig(
            namespace_salt="test",
            project="test-project",
            smoke_namespace="test_run",
        )

        with patch("storacle.rpc.bigquery.Client") as mock_bq:
            mock_client = MagicMock()
            mock_bq.return_value = mock_client

            store = _build_bq_store(config)

            # Verify store was created with the smoke table prefix
            assert store.test_prefix == "smoke_test_run__"

    def test_build_wal_client_uses_effective_dataset(self, monkeypatch):
        """_build_wal_client uses effective_wal_dataset."""
        from storacle.rpc import _build_wal_client
        from storacle.config import StoracleConfig

        config = StoracleConfig(
            namespace_salt="test",
            smoke_namespace="smoke_run",
        )

        mock_store = MagicMock()
        wal_client = _build_wal_client(mock_store, config)

        # WalClient should be initialized with smoke dataset
        assert wal_client.dataset == "smoke_smoke_run"


class TestCLISmokeFlags:
    """Test CLI flag validation for smoke mode."""

    def test_smoke_and_test_table_mutually_exclusive(self):
        """--smoke-namespace and --test-table cannot be used together."""
        from click.testing import CliRunner
        from lorchestra.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["run", "fake_job", "--smoke-namespace", "test", "--test-table"],
            catch_exceptions=False,
        )

        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()

    def test_smoke_and_dry_run_mutually_exclusive(self):
        """--smoke-namespace and --dry-run cannot be used together."""
        from click.testing import CliRunner
        from lorchestra.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["run", "fake_job", "--smoke-namespace", "test", "--dry-run"],
            catch_exceptions=False,
        )

        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()

    def test_cleanup_requires_smoke_namespace(self):
        """--clean-up requires --smoke-namespace."""
        from click.testing import CliRunner
        from lorchestra.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["run", "fake_job", "--clean-up"],
            catch_exceptions=False,
        )

        assert result.exit_code != 0
        assert "requires --smoke-namespace" in result.output.lower()


class TestBigQueryEventStoreDataset:
    """Test BigQueryEventStore dataset operations."""

    def test_ensure_dataset_creates_if_missing(self):
        """ensure_dataset creates dataset when it doesn't exist."""
        from google.cloud.exceptions import NotFound
        from storacle.adapters.bq import BigQueryEventStore

        mock_client = MagicMock()
        mock_client.get_dataset.side_effect = NotFound("Not found")

        store = BigQueryEventStore(mock_client, "test-project")
        result = store.ensure_dataset("smoke_test")

        assert result is True
        mock_client.create_dataset.assert_called_once()

    def test_ensure_dataset_skips_if_exists(self):
        """ensure_dataset does not create if dataset exists."""
        from storacle.adapters.bq import BigQueryEventStore

        mock_client = MagicMock()
        # get_dataset succeeds = dataset exists
        mock_client.get_dataset.return_value = MagicMock()

        store = BigQueryEventStore(mock_client, "test-project")
        result = store.ensure_dataset("smoke_test")

        assert result is False
        mock_client.create_dataset.assert_not_called()

    def test_delete_dataset_refuses_non_smoke(self):
        """delete_dataset refuses non-smoke datasets."""
        from storacle.adapters.bq import BigQueryEventStore

        mock_client = MagicMock()
        store = BigQueryEventStore(mock_client, "test-project")

        with pytest.raises(ValueError, match="only smoke_"):
            store.delete_dataset("production_data")

    def test_delete_dataset_accepts_smoke(self):
        """delete_dataset accepts smoke_* datasets."""
        from storacle.adapters.bq import BigQueryEventStore

        mock_client = MagicMock()
        store = BigQueryEventStore(mock_client, "test-project")

        result = store.delete_dataset("smoke_run_001")

        assert result is True
        mock_client.delete_dataset.assert_called_once()
