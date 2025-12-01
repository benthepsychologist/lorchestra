"""Tests for lorchestra run and jobs commands."""
import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner


def test_run_job_command(tmp_path):
    """Test lorchestra run command."""
    from lorchestra.cli import main

    runner = CliRunner()

    # Create a temp definitions directory with a test job spec
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()
    (defs_dir / "test_job.json").write_text(json.dumps({
        "job_id": "test_job",
        "job_type": "ingest",
        "source": {"stream": "test.messages", "identity": "test:acct1"},
        "sink": {"source_system": "test", "connection_name": "test-acct1", "object_type": "message"},
    }))

    # Mock the job_runner
    env = {"EVENTS_BQ_DATASET": "test_dataset"}

    with patch.dict(os.environ, env):
        with patch('lorchestra.cli.DEFINITIONS_DIR', defs_dir):
            with patch('lorchestra.job_runner.run_job') as mock_run:
                with patch('google.cloud.bigquery.Client'):
                    with patch('lorchestra.stack_clients.event_client.ensure_test_tables_exist'):
                        result = runner.invoke(main, ['run', 'test_job'])

    assert result.exit_code == 0, f"Expected exit_code 0 but got {result.exit_code}: {result.output}"
    assert "completed" in result.output
    mock_run.assert_called_once()


def test_run_job_with_dry_run(tmp_path):
    """Test lorchestra run with --dry-run option."""
    from lorchestra.cli import main

    runner = CliRunner()

    # Create a temp definitions directory with a test job spec
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()
    (defs_dir / "test_job.json").write_text(json.dumps({
        "job_id": "test_job",
        "job_type": "ingest",
        "source": {"stream": "test.messages", "identity": "test:acct1"},
        "sink": {"source_system": "test", "connection_name": "test-acct1", "object_type": "message"},
    }))

    env = {"EVENTS_BQ_DATASET": "test_dataset"}

    with patch.dict(os.environ, env):
        with patch('lorchestra.cli.DEFINITIONS_DIR', defs_dir):
            with patch('lorchestra.job_runner.run_job') as mock_run:
                with patch('google.cloud.bigquery.Client'):
                    result = runner.invoke(main, ['run', 'test_job', '--dry-run'])

    assert result.exit_code == 0
    assert "DRY RUN MODE" in result.output
    mock_run.assert_called_once()
    # Verify dry_run was passed
    call_kwargs = mock_run.call_args[1]
    assert call_kwargs.get('dry_run') is True


def test_run_job_with_test_table(tmp_path):
    """Test lorchestra run with --test-table option."""
    from lorchestra.cli import main

    runner = CliRunner()

    # Create a temp definitions directory with a test job spec
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()
    (defs_dir / "test_job.json").write_text(json.dumps({
        "job_id": "test_job",
        "job_type": "ingest",
        "source": {"stream": "test.messages", "identity": "test:acct1"},
        "sink": {"source_system": "test", "connection_name": "test-acct1", "object_type": "message"},
    }))

    env = {"EVENTS_BQ_DATASET": "test_dataset"}

    with patch.dict(os.environ, env):
        with patch('lorchestra.cli.DEFINITIONS_DIR', defs_dir):
            with patch('lorchestra.job_runner.run_job') as mock_run:
                with patch('google.cloud.bigquery.Client'):
                    with patch('lorchestra.stack_clients.event_client.ensure_test_tables_exist'):
                        result = runner.invoke(main, ['run', 'test_job', '--test-table'])

    assert result.exit_code == 0
    assert "TEST TABLE MODE" in result.output
    mock_run.assert_called_once()
    # Verify test_table was passed
    call_kwargs = mock_run.call_args[1]
    assert call_kwargs.get('test_table') is True


def test_run_job_failure(tmp_path):
    """Test lorchestra run command with failure."""
    from lorchestra.cli import main

    runner = CliRunner()

    # Create a temp definitions directory with a test job spec
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()
    (defs_dir / "test_job.json").write_text(json.dumps({
        "job_id": "test_job",
        "job_type": "ingest",
        "source": {"stream": "test.messages", "identity": "test:acct1"},
        "sink": {"source_system": "test", "connection_name": "test-acct1", "object_type": "message"},
    }))

    env = {"EVENTS_BQ_DATASET": "test_dataset"}

    with patch.dict(os.environ, env):
        with patch('lorchestra.cli.DEFINITIONS_DIR', defs_dir):
            with patch('lorchestra.job_runner.run_job') as mock_run:
                mock_run.side_effect = RuntimeError("Job failed")
                with patch('google.cloud.bigquery.Client'):
                    result = runner.invoke(main, ['run', 'test_job'])

    assert result.exit_code == 1
    assert "failed" in result.output


def test_run_job_unknown_job(tmp_path):
    """Test lorchestra run with unknown job."""
    from lorchestra.cli import main

    runner = CliRunner()

    # Create an empty definitions directory
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()

    with patch('lorchestra.cli.DEFINITIONS_DIR', defs_dir):
        result = runner.invoke(main, ['run', 'nonexistent_job'])

    assert result.exit_code == 1
    assert "Unknown job" in result.output


def test_jobs_list_command(tmp_path):
    """Test lorchestra jobs list command."""
    from lorchestra.cli import main

    runner = CliRunner()

    # Create a temp definitions directory with test job definitions
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()
    (defs_dir / "ingest_gmail.json").write_text(json.dumps({
        "job_id": "ingest_gmail",
        "job_type": "ingest",
    }))
    (defs_dir / "ingest_exchange.json").write_text(json.dumps({
        "job_id": "ingest_exchange",
        "job_type": "ingest",
    }))
    (defs_dir / "canonize_email.json").write_text(json.dumps({
        "job_id": "canonize_email",
        "job_type": "canonize",
    }))

    with patch('lorchestra.cli.DEFINITIONS_DIR', defs_dir):
        result = runner.invoke(main, ['jobs', 'list'])

    assert result.exit_code == 0
    assert "ingest:" in result.output
    assert "ingest_gmail" in result.output
    assert "ingest_exchange" in result.output
    assert "canonize:" in result.output
    assert "canonize_email" in result.output


def test_jobs_list_with_type_filter(tmp_path):
    """Test lorchestra jobs list with --type filter."""
    from lorchestra.cli import main

    runner = CliRunner()

    # Create a temp definitions directory with test job definitions
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()
    (defs_dir / "ingest_gmail.json").write_text(json.dumps({
        "job_id": "ingest_gmail",
        "job_type": "ingest",
    }))
    (defs_dir / "canonize_email.json").write_text(json.dumps({
        "job_id": "canonize_email",
        "job_type": "canonize",
    }))

    with patch('lorchestra.cli.DEFINITIONS_DIR', defs_dir):
        result = runner.invoke(main, ['jobs', 'list', '--type', 'ingest'])

    assert result.exit_code == 0
    assert "ingest:" in result.output
    assert "ingest_gmail" in result.output
    assert "canonize" not in result.output


def test_jobs_list_unknown_type(tmp_path):
    """Test lorchestra jobs list with unknown type."""
    from lorchestra.cli import main

    runner = CliRunner()

    # Create a temp definitions directory with test job definitions
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()
    (defs_dir / "ingest_gmail.json").write_text(json.dumps({
        "job_id": "ingest_gmail",
        "job_type": "ingest",
    }))

    with patch('lorchestra.cli.DEFINITIONS_DIR', defs_dir):
        result = runner.invoke(main, ['jobs', 'list', '--type', 'nonexistent'])

    assert result.exit_code == 0  # Not an error, just no jobs found
    assert "No jobs of type" in result.output


def test_jobs_show_command(tmp_path):
    """Test lorchestra jobs show command."""
    from lorchestra.cli import main

    runner = CliRunner()

    # Create a temp definitions directory with a test job spec
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()
    (defs_dir / "test_job.json").write_text(json.dumps({
        "job_id": "test_job",
        "job_type": "ingest",
        "source": {"stream": "test.messages"},
    }))

    with patch('lorchestra.cli.DEFINITIONS_DIR', defs_dir):
        result = runner.invoke(main, ['jobs', 'show', 'test_job'])

    assert result.exit_code == 0
    assert "Job: test_job" in result.output
    assert "Type: ingest" in result.output
    assert '"job_type": "ingest"' in result.output


def test_jobs_show_unknown_job(tmp_path):
    """Test lorchestra jobs show with unknown job."""
    from lorchestra.cli import main

    runner = CliRunner()

    # Create an empty definitions directory
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()

    with patch('lorchestra.cli.DEFINITIONS_DIR', defs_dir):
        result = runner.invoke(main, ['jobs', 'show', 'nonexistent'])

    assert result.exit_code == 1
    assert "Unknown job" in result.output


def test_mutually_exclusive_options(tmp_path):
    """Test that --dry-run and --test-table are mutually exclusive."""
    from lorchestra.cli import main

    runner = CliRunner()

    # Create a temp definitions directory with a test job spec
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()
    (defs_dir / "test_job.json").write_text(json.dumps({
        "job_id": "test_job",
        "job_type": "ingest",
    }))

    env = {"EVENTS_BQ_DATASET": "test_dataset"}

    with patch.dict(os.environ, env):
        with patch('lorchestra.cli.DEFINITIONS_DIR', defs_dir):
            with patch('google.cloud.bigquery.Client'):
                result = runner.invoke(main, ['run', 'test_job', '--dry-run', '--test-table'])

    assert result.exit_code != 0
    assert "mutually exclusive" in result.output
