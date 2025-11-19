"""Tests for lorc run-job and lorc jobs commands."""
from click.testing import CliRunner
from unittest.mock import patch, MagicMock


def test_run_job_command():
    """Test lorchestra run-job command."""
    from lorchestra.cli import main

    runner = CliRunner()

    with patch('lorchestra.jobs.execute_job') as mock_exec:
        with patch('google.cloud.bigquery.Client') as mock_bq_client:
            result = runner.invoke(main, ['run-job', 'pkg', 'job', '--account', 'test'])

    assert result.exit_code == 0
    assert "✓ pkg/job completed" in result.output
    mock_exec.assert_called_once()

    # Verify BQ client was created
    mock_bq_client.assert_called_once()


def test_run_job_with_all_options():
    """Test lorchestra run-job with all options."""
    from lorchestra.cli import main

    runner = CliRunner()

    with patch('lorchestra.jobs.execute_job') as mock_exec:
        with patch('google.cloud.bigquery.Client'):
            result = runner.invoke(main, [
                'run-job', 'ingester', 'extract_gmail',
                '--account', 'acct1',
                '--since', '7d',
                '--until', '2025-11-18'
            ])

    assert result.exit_code == 0
    mock_exec.assert_called_once()

    # Verify only known options were passed
    call_kwargs = mock_exec.call_args[1]
    assert 'account' in call_kwargs
    assert 'since' in call_kwargs
    assert 'until' in call_kwargs


def test_run_job_failure():
    """Test lorchestra run-job command with failure."""
    from lorchestra.cli import main

    runner = CliRunner()

    with patch('lorchestra.jobs.execute_job') as mock_exec:
        with patch('google.cloud.bigquery.Client'):
            mock_exec.side_effect = RuntimeError("Job failed")
            result = runner.invoke(main, ['run-job', 'pkg', 'job'])

    assert result.exit_code == 1
    assert "✗ pkg/job failed" in result.output


def test_jobs_list_command():
    """Test lorchestra jobs list command."""
    from lorchestra.cli import main

    runner = CliRunner()

    with patch('lorchestra.jobs.discover_jobs') as mock_discover:
        mock_discover.return_value = {
            "ingester": {"extract_gmail": lambda: None, "extract_exchange": lambda: None},
            "canonizer": {"canonicalize_email": lambda: None}
        }

        result = runner.invoke(main, ['jobs', 'list'])

    assert result.exit_code == 0
    assert "ingester:" in result.output
    assert "extract_gmail" in result.output
    assert "extract_exchange" in result.output
    assert "canonizer:" in result.output
    assert "canonicalize_email" in result.output


def test_jobs_list_with_package_filter():
    """Test lorchestra jobs list with package filter."""
    from lorchestra.cli import main

    runner = CliRunner()

    with patch('lorchestra.jobs.discover_jobs') as mock_discover:
        mock_discover.return_value = {
            "ingester": {"extract_gmail": lambda: None},
            "canonizer": {"canonicalize_email": lambda: None}
        }

        result = runner.invoke(main, ['jobs', 'list', 'ingester'])

    assert result.exit_code == 0
    assert "Jobs in ingester:" in result.output
    assert "extract_gmail" in result.output
    assert "canonizer" not in result.output


def test_jobs_list_unknown_package():
    """Test lorchestra jobs list with unknown package."""
    from lorchestra.cli import main

    runner = CliRunner()

    with patch('lorchestra.jobs.discover_jobs') as mock_discover:
        mock_discover.return_value = {}

        result = runner.invoke(main, ['jobs', 'list', 'nonexistent'])

    assert result.exit_code == 1
    assert "Unknown package: nonexistent" in result.output


def test_jobs_show_command():
    """Test lorchestra jobs show command."""
    from lorchestra.cli import main

    runner = CliRunner()

    def mock_job():
        """Test job docstring."""
        pass

    with patch('lorchestra.jobs.get_job') as mock_get:
        mock_get.return_value = mock_job

        result = runner.invoke(main, ['jobs', 'show', 'pkg', 'test_job'])

    assert result.exit_code == 0
    assert "pkg/test_job" in result.output
    assert "Location:" in result.output
    assert "Signature:" in result.output
    assert "Test job docstring" in result.output


def test_jobs_show_unknown_job():
    """Test lorchestra jobs show with unknown job."""
    from lorchestra.cli import main

    runner = CliRunner()

    with patch('lorchestra.jobs.get_job') as mock_get:
        mock_get.side_effect = ValueError("Unknown job: pkg/nonexistent")

        result = runner.invoke(main, ['jobs', 'show', 'pkg', 'nonexistent'])

    assert result.exit_code != 0
