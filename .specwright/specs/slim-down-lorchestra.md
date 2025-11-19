---
version: "0.1"
tier: C
title: Slim Down lorchestra - Thin Orchestrator
owner: benthepsychologist
goal: Remove vault code and implement minimal job orchestration
labels: [orchestrator, cleanup, vault-removal]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-11-18T21:30:00.000000+00:00
updated: 2025-11-18T21:30:00.000000+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/slim-orchestrator"
---

# Slim Down lorchestra - Thin Orchestrator

## Objective

**Slim down lorchestra** to be a thin job orchestrator only. Remove all vault storage code, heavy domain logic, and tool adapters. Implement minimal job discovery and execution via setuptools entrypoints.

**What lorchestra becomes:**
- Job discovery via entrypoints (`lorc jobs list`)
- Job execution (`lorc run package job --account X`)
- event_client hosting (`lorchestra.stack_clients.event_client`)
- Basic logging and config

**What gets removed:**
- Vault storage (extract.py, LATEST pointers, manifests)
- Tool adapters (meltano.py, canonizer.py, vector_projector.py)
- Domain stages (canonize.py, index.py)
- Heavy CLI commands

## Dependencies

**BLOCKS:** Requires `refactor-and-event_client-spec.md` (Spec 1) completed first.

Required: `lorchestra.stack_clients.event_client.emit()` must exist and be tested.

## Acceptance Criteria

### Job Orchestration (NEW)
- [ ] `lorchestra/jobs.py` created with `discover_jobs()`, `get_job()`, `execute_job()`
- [ ] `lorc run package job` command with common options (--account, --since, --until)
- [ ] `lorc jobs list [package]` command
- [ ] `lorc jobs show package job` command
- [ ] BigQuery client created once per `lorc run` invocation, passed to job

### Vault and Domain Code Removal (DELETE)
- [ ] `lorchestra/stages/` directory DELETED
- [ ] `lorchestra/tools/` directory DELETED
- [ ] `lorchestra/pipeline.py` DELETED (or heavily simplified if kept)
- [ ] Old CLI commands removed (`extract`, `canonize`, `index`, etc.)

### Quality
- [ ] All tests pass
- [ ] Linting passes (ruff check)
- [ ] Job discovery tests (mock entrypoints)
- [ ] CLI tests for `lorc run` and `lorc jobs`

### Documentation
- [ ] README updated to reflect thin orchestrator role
- [ ] `docs/jobs.md` created with job ABI documentation
- [ ] Note: actual jobs live in external packages (ingester, canonizer, etc.)

## Architecture

### Job Discovery via Entrypoints

External packages register jobs in their `pyproject.toml`:

```toml
# Example: ingester/pyproject.toml
[project.entry-points."lorchestra.jobs"]
extract_gmail = "ingester.jobs.email:extract_gmail"
extract_exchange = "ingester.jobs.email:extract_exchange"

# Example: canonizer/pyproject.toml
[project.entry-points."lorchestra.jobs"]
canonicalize_email = "canonizer.jobs.email:canonicalize_email"
```

lorchestra discovers these via `importlib.metadata.entry_points()`.

**Package naming convention:**
- The "package" argument in `lorc run package job` is derived from the first part of the module path
- Example: `"ingester.jobs.email:extract_gmail"` → package = `"ingester"`
- This is a **naming convention**, not a deep truth - just a convenient way to namespace jobs
- All entrypoints are in the same group (`lorchestra.jobs`), regardless of package

### Job Signature ABI

All jobs must follow this signature:

```python
from google.cloud import bigquery

def job_name(
    *,
    bq_client: bigquery.Client,
    **kwargs  # Job-specific params (account, since, etc.)
) -> None:
    """
    Job docstring.

    Raises on failure, returns None on success.
    """
    ...
```

**Note on `bq_client` parameter:**
- All jobs receive `bq_client` even if they don't write events
- This is slightly inelegant but acceptable for v0 (everything important uses BQ)
- Jobs that don't need it can ignore it
- Future: Could make optional or abstract to "primary data sink handle"

### CLI Signature

```bash
# Run a job
lorc run package-name job-name --account X --since Y

# List all jobs
lorc jobs list

# List jobs from one package
lorc jobs list ingester

# Show job details
lorc jobs show ingester extract_gmail
```

## Plan

### Step 1: Implement Job Orchestration

#### 1.1 Create `lorchestra/jobs.py`

```python
"""Job discovery and execution via entrypoints."""
from importlib.metadata import entry_points
from typing import Dict, Callable
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)

def discover_jobs() -> Dict[str, Dict[str, Callable]]:
    """
    Discover all jobs from lorchestra.jobs entrypoints.

    Returns:
        {
            "ingester": {"extract_gmail": <func>, ...},
            "canonizer": {"canonicalize_email": <func>, ...},
        }
    """
    jobs = {}

    # Use .select() for Python 3.10+ compatibility
    eps = entry_points()
    for ep in eps.select(group="lorchestra.jobs"):
        # ep.name: "extract_gmail"
        # ep.value: "ingester.jobs.email:extract_gmail"

        # Extract package name from module path (convention)
        module_path, _, _ = ep.value.partition(":")
        package_name = module_path.split(".")[0]  # "ingester.jobs.email" → "ingester"

        job_func = ep.load()

        if package_name not in jobs:
            jobs[package_name] = {}
        jobs[package_name][ep.name] = job_func

    return jobs

def get_job(package: str, job_name: str) -> Callable:
    """Get a specific job function."""
    all_jobs = discover_jobs()
    if package not in all_jobs:
        raise ValueError(f"Unknown package: {package}")
    if job_name not in all_jobs[package]:
        raise ValueError(f"Unknown job: {package}/{job_name}")
    return all_jobs[package][job_name]

def execute_job(
    package: str,
    job_name: str,
    bq_client: bigquery.Client,
    **kwargs
) -> None:
    """Execute a job with error handling."""
    job_func = get_job(package, job_name)

    logger.info(f"Starting job: {package}/{job_name}")
    try:
        job_func(bq_client=bq_client, **kwargs)
        logger.info(f"Job completed: {package}/{job_name}")
    except Exception as e:
        logger.error(
            f"Job failed: {package}/{job_name}",
            exc_info=True,
            extra={"package": package, "job": job_name}
        )
        raise
```

**Tests:**
- Mock `entry_points()` to return fake jobs
- Verify `discover_jobs()` structure
- Test `get_job()` errors for unknown package/job
- Test `execute_job()` calls job with correct args

#### 1.2 Update CLI - Add `lorc run` command

Update `lorchestra/cli.py`:

```python
import click
from google.cloud import bigquery
from lorchestra.jobs import execute_job

@cli.command()
@click.argument("package")
@click.argument("job")
@click.option("--account", help="Account identifier")
@click.option("--since", help="Start time (ISO or relative)")
@click.option("--until", help="End time (ISO)")
def run(package: str, job: str, **kwargs):
    """
    Run a job from an installed package.

    Examples:
        lorc run ingester extract_gmail --account acct1-personal
        lorc run canonizer canonicalize_email
    """
    # Create BQ client once
    bq_client = bigquery.Client()

    # IMPORTANT: Only pass known, explicit options to jobs
    # Don't pass the entire kwargs blob - keeps job interface stable
    known_options = ["account", "since", "until"]
    job_kwargs = {k: v for k, v in kwargs.items() if k in known_options and v is not None}

    # Execute
    try:
        execute_job(package, job, bq_client, **job_kwargs)
        click.echo(f"✓ {package}/{job} completed")
    except Exception as e:
        click.echo(f"✗ {package}/{job} failed: {e}", err=True)
        raise SystemExit(1)
```

**Design note:** Only pass explicit, known options (`account`, `since`, `until`) to jobs. This prevents `lorc run` from becoming a grab-bag of arbitrary CLI flags and keeps the job interface stable.

**Tests:**
- Mock `execute_job`, verify it's called with correct args
- Test CLI argument parsing
- Test error handling

#### 1.3 Update CLI - Add `lorc jobs` commands

```python
@cli.group()
def jobs():
    """Manage and inspect jobs."""
    pass

@jobs.command("list")
@click.argument("package", required=False)
def list_jobs(package: str = None):
    """List available jobs."""
    from lorchestra.jobs import discover_jobs

    all_jobs = discover_jobs()

    if package:
        if package not in all_jobs:
            click.echo(f"Unknown package: {package}", err=True)
            raise SystemExit(1)
        click.echo(f"Jobs in {package}:")
        for job_name in sorted(all_jobs[package].keys()):
            click.echo(f"  {job_name}")
    else:
        for pkg in sorted(all_jobs.keys()):
            click.echo(f"{pkg}:")
            for job_name in sorted(all_jobs[pkg].keys()):
                click.echo(f"  {job_name}")

@jobs.command("show")
@click.argument("package")
@click.argument("job")
def show_job(package: str, job: str):
    """Show job details."""
    from lorchestra.jobs import get_job
    import inspect

    job_func = get_job(package, job)
    click.echo(f"{package}/{job}")
    click.echo(f"Location: {inspect.getfile(job_func)}")
    click.echo(f"Signature: {inspect.signature(job_func)}")
    if job_func.__doc__:
        click.echo(f"\n{job_func.__doc__}")
```

### Step 2: Remove Vault and Domain Code

#### 2.1 Create Safety Tag

Before deletion, create a tag for archeological purposes:

```bash
git tag -a pre-slim-lorchestra -m "Snapshot before removing vault/stages/tools code"
git push origin pre-slim-lorchestra
```

This allows future recovery of old code without digging through reflogs.

#### 2.2 Delete Files

Delete these files/directories entirely:

```bash
rm -rf lorchestra/stages/
rm -rf lorchestra/tools/
rm -f lorchestra/pipeline.py  # Or simplify heavily if needed
```

Specifically:
- `lorchestra/stages/extract.py`
- `lorchestra/stages/canonize.py`
- `lorchestra/stages/index.py`
- `lorchestra/stages/base.py`
- `lorchestra/tools/meltano.py`
- `lorchestra/tools/canonizer.py`
- `lorchestra/tools/vector_projector.py`
- `lorchestra/tools/base.py`

#### 2.2 Update CLI - Remove Old Commands

Remove from `lorchestra/cli.py`:
- `extract` command (vault-based extraction)
- `canonize` command
- `index` command
- `list extractors/transforms/mappings` commands (if tied to vault)

Keep:
- `config` commands (may be useful)
- `run` command (NEW)
- `jobs` commands (NEW)

#### 2.3 Update Config

If `config/pipeline.yaml` has vault-specific settings, remove them:

```yaml
# REMOVE:
vault:
  base_path: vault/
  ...

stages:
  extract:
    vault: ...
  canonize:
    vault: ...

# KEEP (if useful):
logging:
  level: INFO
  format: json
```

### Step 3: Documentation

#### 3.1 Create `docs/jobs.md`

```markdown
# Job Development Guide

## Overview

Jobs are Python functions in external packages that perform work and emit events.
lorchestra discovers and executes jobs via setuptools entrypoints.

## Job Signature

All jobs must follow this signature:

\```python
from google.cloud import bigquery

def job_name(
    *,
    bq_client: bigquery.Client,
    **kwargs  # Job-specific parameters
) -> None:
    """Job description."""
    # Perform work
    # Emit events via event_client (lives in lorchestra)
    from lorchestra.stack_clients.event_client import emit

    emit(
        event_type="email.received",
        payload={"subject": "...", "from": "..."},
        source="ingester/gmail/acct1-personal",
        bq_client=bq_client
    )
\```

**Important:**
- Jobs MUST use `lorchestra.stack_clients.event_client.emit()` to write events
- Jobs receive `bq_client` from lorchestra and pass it to `emit()`
- This is the standard pattern for all event-writing jobs

## Registering a Job

In your package's `pyproject.toml`:

\```toml
[project.entry-points."lorchestra.jobs"]
my_job = "mypackage.jobs.module:my_job"
\```

Install your package:

\```bash
pip install -e /path/to/mypackage
\```

Run your job:

\```bash
lorc run mypackage my_job --account X
\```

## Examples

See:
- `ingester` package for extraction jobs
- `canonizer` package for canonization jobs
- `final-form` package for scoring jobs
```

#### 3.2 Update README

Update `README.md` to reflect new minimal scope:

```markdown
# lorchestra

Minimal job orchestrator for event-driven data pipelines.

## What lorchestra does

- **Job discovery**: Finds jobs registered via setuptools entrypoints
- **Job execution**: Runs jobs with `lorc run package job`
- **Event client**: Provides `event_client.emit()` for writing events to BigQuery

## What lorchestra does NOT do

- **Extraction**: Handled by `ingester` package
- **Canonization**: Handled by `canonizer` package
- **Scoring**: Handled by `final-form` package

## Usage

List available jobs:

\```bash
lorc jobs list
\```

Run a job:

\```bash
lorc run ingester extract_gmail --account acct1-personal --since 7d
\```

## Job Development

See `docs/jobs.md` for job development guide.
```

### Step 4: Testing

#### 4.1 Unit Tests

Create `tests/test_jobs.py`:

```python
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
        mock_eps.return_value = [ep]

        from lorchestra.jobs import discover_jobs
        jobs = discover_jobs()

        assert "testpkg" in jobs
        assert "test_job" in jobs["testpkg"]

def test_get_job_unknown_package():
    """Test error for unknown package."""
    from lorchestra.jobs import get_job

    with pytest.raises(ValueError, match="Unknown package"):
        get_job("nonexistent", "some_job")

def test_execute_job():
    """Test job execution."""
    from lorchestra.jobs import execute_job

    mock_job = MagicMock()
    mock_bq = MagicMock()

    with patch('lorchestra.jobs.get_job', return_value=mock_job):
        execute_job("pkg", "job", mock_bq, account="test")

    mock_job.assert_called_once_with(bq_client=mock_bq, account="test")
```

#### 4.2 CLI Tests

Create `tests/test_cli_jobs.py`:

```python
"""Tests for lorc run and lorc jobs commands."""
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

def test_lorc_run():
    """Test lorc run command."""
    from lorchestra.cli import cli

    runner = CliRunner()

    with patch('lorchestra.cli.execute_job') as mock_exec:
        result = runner.invoke(cli, ['run', 'pkg', 'job', '--account', 'test'])

    assert result.exit_code == 0
    mock_exec.assert_called_once()

def test_lorc_jobs_list():
    """Test lorc jobs list command."""
    from lorchestra.cli import cli

    with patch('lorchestra.jobs.discover_jobs') as mock_discover:
        mock_discover.return_value = {
            "ingester": {"extract_gmail": lambda: None}
        }

        runner = CliRunner()
        result = runner.invoke(cli, ['jobs', 'list'])

    assert result.exit_code == 0
    assert "ingester" in result.output
    assert "extract_gmail" in result.output
```

## Files to Create/Modify

### Created
1. `lorchestra/jobs.py` - Job discovery and execution
2. `tests/test_jobs.py` - Job orchestration tests
3. `tests/test_cli_jobs.py` - CLI tests
4. `docs/jobs.md` - Job development guide

### Modified
5. `lorchestra/cli.py` - Add `run` and `jobs` commands, remove old commands
6. `README.md` - Update to reflect thin orchestrator role
7. `config/pipeline.yaml` - Remove vault-specific config (if exists)

### Deleted
8. `lorchestra/stages/` - Entire directory
9. `lorchestra/tools/` - Entire directory
10. `lorchestra/pipeline.py` - If heavily vault-dependent

## Timeline

**Tier C - 2-3 days**

- Step 1 (Job orchestration): 1-1.5 days
  - jobs.py module: 0.5 days
  - CLI commands: 0.5 days
  - Tests: 0.5 days

- Step 2 (Cleanup): 0.5 days
  - Delete files: 0.25 days
  - Update config: 0.25 days

- Step 3 (Documentation): 0.5 days

- Step 4 (Additional testing): 0.5 days

**Total: 2.5-3 days**

## Success Metrics

1. **lorchestra is thin:**
   - No vault code remains
   - No domain logic remains (extraction, canonization, indexing)
   - Core files: cli.py, jobs.py, event_client.py, config.py, utils.py

2. **Job orchestration works:**
   - `lorc jobs list` shows jobs from installed packages
   - `lorc run package job` executes successfully
   - BQ client is created once and passed to jobs

3. **Tests pass:**
   - All unit tests pass
   - CLI tests pass
   - Linting passes

4. **Documentation clear:**
   - README accurately describes minimal scope
   - docs/jobs.md provides clear job development guide

## Notes

- **ingester refactor is separate**: This spec does NOT touch ingester package
- **Jobs come from external packages**: lorchestra only discovers and runs them
- **event_client is provided**: Jobs use `lorchestra.stack_clients.event_client.emit()`
- **BQ table must exist**: Environment vars EVENTS_BQ_DATASET and EVENTS_BQ_TABLE must be set
