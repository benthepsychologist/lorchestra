# lorchestra - Lightweight Job Orchestrator

**Lightweight job orchestrator** that discovers and runs jobs from installed packages via Python entrypoints. Jobs emit events to BigQuery for tracking and observability.

## 🎯 Overview

lorchestra is a minimalist orchestrator that:

1. **Discovers jobs** via Python entrypoints (`lorchestra.jobs.<package_name>`)
2. **Runs jobs** with a common interface (BigQuery client + kwargs)
3. **Tracks execution** via BigQuery event emission

```
┌─────────────────────┐
│  Installed Package  │
│  (via entrypoints)  │
└─────────┬───────────┘
          │
          ↓
    ┌─────────────┐
    │ lorchestra  │  ← Discovers & runs jobs
    └──────┬──────┘
           │
           ↓
    ┌──────────────┐
    │   BigQuery   │  ← Event emission
    └──────────────┘
```

### Key Features

- ✅ **Entrypoint-based discovery**: Jobs registered via `pyproject.toml`
- ✅ **Common interface**: All jobs receive `bq_client` and optional kwargs
- ✅ **Event emission**: Jobs emit structured events to BigQuery
- ✅ **CLI tools**: List, inspect, and run jobs
- ✅ **Minimal dependencies**: Just Click, BigQuery client, and logging

## 🚀 Quick Start

### Installation

```bash
cd /home/user/lorchestra

# Create virtual environment
uv venv
source .venv/bin/activate

# Install lorchestra
uv pip install -e .

# Verify installation
lorchestra --version
```

### Basic Usage

```bash
# List all available jobs
lorchestra jobs list

# List jobs for a specific package
lorchestra jobs list ingester

# Show job details
lorchestra jobs show ingester extract_gmail

# Run a job
lorchestra run-job ingester extract_gmail --account acct1-personal

# Run with time range
lorchestra run-job ingester extract_gmail --since 2024-01-01 --until 2024-01-31
lorchestra run-job ingester extract_gmail --since -7d
```

## 📖 Documentation

Comprehensive documentation in the `docs/` directory:

- **[docs/jobs.md](docs/jobs.md)** - Job development guide
- **[docs/architecture.md](docs/architecture.md)** - System architecture (historical)
- **[docs/configuration.md](docs/configuration.md)** - Configuration reference (historical)

## 🔧 Writing Jobs

### Job Interface

Jobs must follow this interface:

```python
def my_job(bq_client, **kwargs):
    """
    Job description.

    Args:
        bq_client: BigQuery client for event emission
        **kwargs: Optional args (account, since, until)
    """
    # Your job logic here
    pass
```

### Registering Jobs

Add to your package's `pyproject.toml`:

```toml
[project.entry-points."lorchestra.jobs.my_package"]
my_job = "my_package.jobs:my_job"
```

### Complete Example

**my_package/jobs.py:**
```python
import logging
from lorchestra.stack_clients.event_client import emit

logger = logging.getLogger(__name__)

def extract_gmail(bq_client, account=None, since=None, until=None):
    """Extract emails from Gmail."""
    logger.info(f"Extracting Gmail for account={account}")

    emit(
        bq_client=bq_client,
        event_type="extraction.started",
        source="my_package/extract_gmail",
        data={"account": account, "since": since}
    )

    try:
        # Extraction logic
        emails = fetch_emails(account, since, until)

        emit(
            bq_client=bq_client,
            event_type="extraction.completed",
            source="my_package/extract_gmail",
            data={"records_count": len(emails)}
        )

        logger.info(f"Extracted {len(emails)} emails")

    except Exception as e:
        emit(
            bq_client=bq_client,
            event_type="extraction.failed",
            source="my_package/extract_gmail",
            data={"error": str(e)}
        )
        raise
```

**pyproject.toml:**
```toml
[project.entry-points."lorchestra.jobs.my_package"]
extract_gmail = "my_package.jobs:extract_gmail"
```

See **[docs/jobs.md](docs/jobs.md)** for complete guide.

## 📊 CLI Commands

### `lorchestra run-job`

Run a job from an installed package.

```bash
lorchestra run-job PACKAGE JOB [OPTIONS]

Options:
  --account TEXT    Account identifier
  --since TEXT      Start time (ISO or relative like "-7d")
  --until TEXT      End time (ISO format)
```

**Examples:**

```bash
# Basic usage
lorchestra run-job ingester extract_gmail

# With account
lorchestra run-job ingester extract_gmail --account acct1-personal

# With time range
lorchestra run-job ingester extract_gmail --since 2024-01-01 --until 2024-01-31

# Relative time
lorchestra run-job ingester extract_gmail --since -7d
```

### `lorchestra jobs list`

List available jobs.

```bash
lorchestra jobs list [PACKAGE]
```

**Examples:**

```bash
# List all jobs
lorchestra jobs list

# Output:
# ingester:
#   extract_gmail
#   extract_slack
# canonizer:
#   canonicalize_email

# List jobs for specific package
lorchestra jobs list ingester

# Output:
# Jobs in ingester:
#   extract_gmail
#   extract_slack
```

### `lorchestra jobs show`

Show job details.

```bash
lorchestra jobs show PACKAGE JOB
```

**Example:**

```bash
$ lorchestra jobs show ingester extract_gmail
ingester/extract_gmail
Location: /path/to/my_package/jobs.py
Signature: (bq_client, account=None, since=None, until=None)

Extract emails from Gmail.
```

## 🎯 Current Status

### ✅ Implemented (v0.1.0)

- [x] **Job discovery** via entrypoints
- [x] **Job execution** with error handling
- [x] **CLI interface**: run-job, jobs list, jobs show
- [x] **Event emission** to BigQuery
- [x] **Structured logging**
- [x] **Comprehensive tests** (26 tests, all passing)

### 🔧 Configuration

Jobs use environment variables for BigQuery event emission:

- `BQ_EVENTS_DATASET` - BigQuery dataset name (e.g., "events")
- `BQ_EVENTS_TABLE` - BigQuery table name (e.g., "job_events")

No other configuration is required. Jobs are discovered automatically via entrypoints.

## 🛠️ Development

### Project Structure

```
lorchestra/
├── lorchestra/                 # Main package
│   ├── __init__.py            # Package entry point
│   ├── cli.py                 # CLI interface (105 lines)
│   ├── jobs.py                # Job discovery & execution
│   ├── config.py              # Configuration utilities
│   ├── utils.py               # Utilities
│   └── stack_clients/         # BigQuery event client
│       └── event_client/
├── docs/
│   └── jobs.md                # Job development guide
├── tests/                     # Tests
│   ├── test_jobs.py          # Job system tests (9 tests)
│   ├── test_cli_jobs.py      # CLI tests (8 tests)
│   └── test_event_client.py  # Event client tests (9 tests)
└── README.md                  # This file
```

### Running Tests

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
python -m pytest tests/ -v

# Run with coverage
pytest --cov=lorchestra tests/
```

### Code Style

```bash
# Lint code
ruff check lorchestra/

# Fix linting issues
ruff check lorchestra/ --fix
```

## 🐛 Troubleshooting

### Job Not Found

**Issue:** "Unknown package" or "Unknown job"

**Solution:**
1. Check job is registered in `pyproject.toml`:
   ```toml
   [project.entry-points."lorchestra.jobs.my_package"]
   my_job = "my_package.jobs:my_job"
   ```
2. Reinstall package:
   ```bash
   uv pip install -e .
   ```
3. List jobs to verify:
   ```bash
   lorchestra jobs list
   ```

### BigQuery Event Emission Fails

**Issue:** Events not appearing in BigQuery

**Solution:**
1. Check environment variables are set:
   ```bash
   echo $BQ_EVENTS_DATASET
   echo $BQ_EVENTS_TABLE
   ```
2. Verify BigQuery authentication:
   ```bash
   gcloud auth application-default login
   ```
3. Check BigQuery table exists and has correct schema

### Job Execution Fails

**Issue:** Job raises exception

**Solution:**
1. Check job logs for detailed error
2. Run job with verbose logging
3. Verify job parameters are correct
4. Test job function directly in Python

## 📚 Related Repositories

- **[meltano-ingest](../meltano-ingest/)** - Data extraction (can define jobs)
- **[canonizer](../canonizer/)** - JSON transformation (can define jobs)
- **[vector-projector](../vector-projector/)** - Local document store (can define jobs)

## 🤝 Contributing

### Quick Contribution Guide

1. Create feature branch
2. Implement changes with tests
3. Run `ruff check` for linting
4. Update documentation
5. Submit pull request

### Code Style

- Use Python 3.12+ features
- Type hints for all functions
- Docstrings in Google format
- Ruff linting

## 📝 License

Proprietary - For internal use only.

## 📞 Support

For issues or questions:

- **Documentation**: See `docs/jobs.md`
- **Tests**: See `tests/` for examples
- **Architecture**: See `docs/architecture.md` (historical)

## 🎓 Migration Notes

This is a **slimmed-down version** of lorchestra. Previous versions included:
- Pipeline orchestrator with stages (extract → canonize → index)
- Vault storage with LATEST pointers
- Complex CLI with many commands

The new version is **minimal** and **focused**:
- Just job discovery and execution
- No built-in stages or pipeline
- Minimal CLI (run-job, jobs list, jobs show)

**Migration path:**
- Old pipeline stages → Create as jobs in separate packages
- Old `lorchestra run` → `lorchestra run-job <package> <job>`
- Old vault/stages → Implement in job packages as needed

See git tag `pre-slim-lorchestra` for old code.

---

**Version:** 0.1.0
**Status:** Production-ready
**Last Updated:** 2025-11-19
