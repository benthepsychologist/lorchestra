"""Tests for job discovery and execution."""
from unittest.mock import MagicMock, patch
import pytest


def test_discover_jobs():
    """Test job discovery from entrypoints."""
    with patch('lorchestra.jobs.entry_points') as mock_eps:
        # Mock entrypoint
        ep = MagicMock()
        ep.name = "test_job"
        ep.value = "testpkg.jobs:test_job"
        ep.load.return_value = lambda: None

        # Create mock that returns iterable with select method
        mock_group = MagicMock()
        mock_group.select.return_value = [ep]
        mock_eps.return_value = mock_group

        from lorchestra.jobs import discover_jobs
        jobs = discover_jobs()

        assert "testpkg" in jobs
        assert "test_job" in jobs["testpkg"]


def test_discover_jobs_multiple_packages():
    """Test discovering jobs from multiple packages."""
    with patch('lorchestra.jobs.entry_points') as mock_eps:
        # Mock entrypoints from different packages
        ep1 = MagicMock()
        ep1.name = "extract_gmail"
        ep1.value = "ingester.jobs.email:extract_gmail"
        ep1.load.return_value = lambda: None

        ep2 = MagicMock()
        ep2.name = "canonicalize_email"
        ep2.value = "canonizer.jobs.email:canonicalize_email"
        ep2.load.return_value = lambda: None

        mock_group = MagicMock()
        mock_group.select.return_value = [ep1, ep2]
        mock_eps.return_value = mock_group

        from lorchestra.jobs import discover_jobs
        jobs = discover_jobs()

        assert "ingester" in jobs
        assert "canonizer" in jobs
        assert "extract_gmail" in jobs["ingester"]
        assert "canonicalize_email" in jobs["canonizer"]


def test_get_job_unknown_package():
    """Test error for unknown package."""
    with patch('lorchestra.jobs.discover_jobs') as mock_discover:
        mock_discover.return_value = {}

        from lorchestra.jobs import get_job

        with pytest.raises(ValueError, match="Unknown package"):
            get_job("nonexistent", "some_job")


def test_get_job_unknown_job():
    """Test error for unknown job in known package."""
    with patch('lorchestra.jobs.discover_jobs') as mock_discover:
        mock_discover.return_value = {"testpkg": {}}

        from lorchestra.jobs import get_job

        with pytest.raises(ValueError, match="Unknown job"):
            get_job("testpkg", "nonexistent_job")


def test_get_job_success():
    """Test successful job retrieval."""
    mock_func = lambda: None

    with patch('lorchestra.jobs.discover_jobs') as mock_discover:
        mock_discover.return_value = {"testpkg": {"test_job": mock_func}}

        from lorchestra.jobs import get_job

        result = get_job("testpkg", "test_job")
        assert result == mock_func


def test_execute_job():
    """Test job execution."""
    from lorchestra.jobs import execute_job

    mock_job = MagicMock()
    mock_bq = MagicMock()

    with patch('lorchestra.jobs.get_job', return_value=mock_job):
        execute_job("pkg", "job", mock_bq, account="test")

    mock_job.assert_called_once_with(bq_client=mock_bq, account="test")


def test_execute_job_with_error():
    """Test job execution with error handling."""
    from lorchestra.jobs import execute_job

    mock_job = MagicMock()
    mock_job.side_effect = RuntimeError("Job failed")
    mock_bq = MagicMock()

    with patch('lorchestra.jobs.get_job', return_value=mock_job):
        with pytest.raises(RuntimeError, match="Job failed"):
            execute_job("pkg", "job", mock_bq)


def test_execute_job_logs_info():
    """Test that job execution logs info messages."""
    from lorchestra.jobs import execute_job
    import logging

    mock_job = MagicMock()
    mock_bq = MagicMock()

    with patch('lorchestra.jobs.get_job', return_value=mock_job):
        with patch('lorchestra.jobs.logger') as mock_logger:
            execute_job("pkg", "job", mock_bq)

            # Verify info logs were called
            assert mock_logger.info.call_count == 2
            mock_logger.info.assert_any_call("Starting job: pkg/job")
            mock_logger.info.assert_any_call("Job completed: pkg/job")


def test_execute_job_logs_error_on_failure():
    """Test that job execution logs errors on failure."""
    from lorchestra.jobs import execute_job

    mock_job = MagicMock()
    mock_job.side_effect = RuntimeError("Job failed")
    mock_bq = MagicMock()

    with patch('lorchestra.jobs.get_job', return_value=mock_job):
        with patch('lorchestra.jobs.logger') as mock_logger:
            with pytest.raises(RuntimeError):
                execute_job("pkg", "job", mock_bq)

            # Verify error log was called
            mock_logger.error.assert_called_once()
            call_args = mock_logger.error.call_args
            assert "Job failed: pkg/job" in call_args[0][0]
