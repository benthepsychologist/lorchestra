# Contributing to lorchestra

Thank you for contributing to lorch! This guide will help you get started with development.

## 📋 Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Code Style](#code-style)
- [Testing](#testing)
- [Making Changes](#making-changes)
- [Pull Request Process](#pull-request-process)
- [Security Guidelines](#security-guidelines)

---

## 🚀 Getting Started

### Prerequisites

- Python 3.12+
- `uv` package manager
- Git
- Access to component repositories:
  - `/home/user/meltano-ingest`
  - `/home/user/canonizer`
  - `/home/user/vector-projector`
  - `/home/user/transforms`

### Development Environment

```bash
# Clone repository (if not already local)
cd /home/user/lorch

# Create virtual environment
uv venv
source .venv/bin/activate

# Install in editable mode with dev dependencies
uv pip install -e ".[dev]"

# Verify installation
lorchestra --version
```

---

## 🛠️ Development Setup

### Project Structure

```
lorchestra/
├── lorchestra/                  # Main package
│   ├── __init__.py
│   ├── cli.py             # CLI interface
│   ├── pipeline.py        # Pipeline orchestrator
│   ├── config.py          # Configuration management
│   ├── utils.py           # Utility functions
│   └── stages/            # Pipeline stages
│       ├── base.py        # Base classes
│       ├── extract.py     # Extract stage
│       ├── canonize.py    # Canonize stage
│       └── index.py       # Index stage
├── config/
│   └── pipeline.yaml      # Default configuration
├── docs/                   # Documentation
├── tests/                  # Test suite (to be created)
├── logs/                   # Runtime logs (gitignored)
├── pyproject.toml         # Python project config
├── requirements.txt       # Dependencies
├── README.md              # Main documentation
├── NEXT_STEPS.md          # Roadmap
└── CONTRIBUTING.md        # This file
```

### Key Dependencies

- **click** - CLI framework
- **pyyaml** - Configuration parsing
- **rich** - Terminal formatting
- **python-dotenv** - Environment variables

---

## 🎨 Code Style

### Python Style Guide

We follow **PEP 8** with these tools:

- **Black** - Code formatting (line length: 100)
- **Ruff** - Linting
- **Type hints** - Required for all functions

### Formatting Code

```bash
# Format with Black
black lorchestra/

# Lint with Ruff
ruff check lorchestra/

# Fix linting issues automatically
ruff check --fix lorchestra/
```

### Type Hints

**Required** for all function signatures:

```python
def transform_data(input_file: Path, output_dir: Path) -> List[Path]:
    """
    Transform data from input to output.

    Args:
        input_file: Path to input JSONL file
        output_dir: Directory for output files

    Returns:
        List of output file paths

    Raises:
        FileNotFoundError: If input file doesn't exist
    """
    pass
```

### Docstrings

Use **Google style** docstrings:

```python
def my_function(param1: str, param2: int) -> bool:
    """
    Short description of function.

    Longer description if needed. Can span multiple lines and include
    detailed information about the function's behavior.

    Args:
        param1: Description of first parameter
        param2: Description of second parameter

    Returns:
        Description of return value

    Raises:
        ValueError: When param2 is negative
        FileNotFoundError: When required file is missing

    Example:
        >>> my_function("test", 42)
        True
    """
    pass
```

### Naming Conventions

- **Classes:** `PascalCase` (e.g., `PipelineResult`)
- **Functions:** `snake_case` (e.g., `run_pipeline`)
- **Constants:** `UPPER_SNAKE_CASE` (e.g., `MAX_RETRIES`)
- **Private:** prefix with `_` (e.g., `_internal_function`)

---

## 🧪 Testing

### Writing Tests

Tests go in `tests/` directory:

```
tests/
├── unit/                   # Unit tests
│   ├── test_config.py
│   ├── test_utils.py
│   └── test_stages/
│       ├── test_extract.py
│       ├── test_canonize.py
│       └── test_index.py
├── integration/            # Integration tests
│   └── test_pipeline.py
└── fixtures/               # Test data
    └── pipeline_config.yaml
```

### Test Requirements

- **Coverage:** Aim for >80% code coverage
- **Fixtures:** Use test fixtures, not real PHI data
- **Isolation:** Tests should be independent
- **Speed:** Unit tests should run in <1s each

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/unit/test_config.py

# Run with coverage
pytest --cov=lorchestra --cov-report=html tests/

# View coverage report
open htmlcov/index.html
```

### Example Test

```python
import pytest
from pathlib import Path
from lorchestra.config import load_config, ConfigError

def test_config_loads_valid_file():
    """Test that valid config file loads successfully."""
    config = load_config(Path("tests/fixtures/pipeline_config.yaml"))
    assert config.name == "test-pipeline"
    assert len(config.stages) == 3

def test_config_raises_on_invalid_file():
    """Test that invalid config raises ConfigError."""
    with pytest.raises(ConfigError):
        load_config(Path("tests/fixtures/invalid_config.yaml"))
```

---

## 🔧 Making Changes

### Development Workflow

1. **Create feature branch:**
   ```bash
   git checkout -b feature/add-new-transform
   ```

2. **Make changes:**
   - Write code following style guidelines
   - Add docstrings and type hints
   - Write tests for new functionality

3. **Test locally:**
   ```bash
   # Format code
   black lorchestra/

   # Run linter
   ruff check lorchestra/

   # Run tests
   pytest tests/

   # Test manually
   lorchestra run --dry-run
   ```

4. **Commit changes:**
   ```bash
   git add .
   git commit -m "feat: add Exchange to canonical transform

   - Created exchange_to_canonical_v1.jsonata
   - Added metadata file with checksum
   - Updated pipeline config with new mapping
   - Added unit tests for transform validation"
   ```

### Commit Message Format

Follow **Conventional Commits**:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `style:` - Code style changes (formatting)
- `refactor:` - Code refactoring
- `test:` - Adding tests
- `chore:` - Maintenance tasks

**Examples:**

```
feat(canonize): add Exchange transform

Added JSONata transform for Exchange messages to canonical email format.
Includes metadata file with schema versions and checksum.

Closes #42

fix(pipeline): handle missing input files gracefully

Previously would crash if no input files found. Now logs warning
and continues to next stage.

docs(readme): update installation instructions

Added troubleshooting section for common setup issues.
```

---

## 📬 Pull Request Process

### Before Submitting

- [ ] Code follows style guidelines (Black + Ruff)
- [ ] All tests pass (`pytest tests/`)
- [ ] New functionality has tests
- [ ] Documentation updated (if needed)
- [ ] Commit messages follow convention
- [ ] No PHI data in commits

### Submitting PR

1. **Push branch:**
   ```bash
   git push origin feature/add-new-transform
   ```

2. **Create PR with description:**
   ```markdown
   ## Description
   Added Exchange to canonical email transform

   ## Changes
   - Created exchange_to_canonical_v1.jsonata
   - Added metadata file
   - Updated pipeline config
   - Added unit tests

   ## Testing
   - Tested with sample Exchange messages
   - All unit tests pass
   - Integration test with full pipeline

   ## Checklist
   - [x] Code formatted with Black
   - [x] Linted with Ruff
   - [x] Tests added and passing
   - [x] Documentation updated
   - [x] No PHI data in commits
   ```

3. **Wait for review**

4. **Address feedback**

5. **Merge when approved**

---

## 🔒 Security Guidelines

### PHI Data Protection

**CRITICAL:** This project handles Protected Health Information (HIPAA-regulated).

### Rules

1. **Never log PHI data:**
   ```python
   # ❌ BAD
   logger.info(f"Processing message: {message.content}")

   # ✅ GOOD
   logger.info(f"Processing message: {message.id}")
   ```

2. **Sanitize error messages:**
   ```python
   # ❌ BAD
   raise ValueError(f"Invalid email: {email}")

   # ✅ GOOD
   from lorchestra.utils import sanitize_error_message
   raise ValueError(sanitize_error_message(f"Invalid email: {email}"))
   ```

3. **Validate permissions:**
   ```python
   # Always validate before PHI operations
   from lorchestra.utils import validate_phi_permissions
   validate_phi_permissions(phi_dir, "700")
   ```

4. **No PHI in git:**
   - Never commit `.jsonl` files
   - Never commit database exports
   - Never commit email archives
   - Never commit token caches

5. **File permissions:**
   ```python
   # Set restrictive permissions on PHI files
   phi_file.chmod(0o600)  # Owner read/write only
   phi_dir.chmod(0o700)   # Owner access only
   ```

### Security Checklist

Before committing:

- [ ] No PHI data in code or comments
- [ ] Error messages sanitized
- [ ] File permissions set correctly
- [ ] No credentials in code
- [ ] No test data contains real PHI

---

## 🐛 Debugging

### Enable Debug Logging

```bash
lorchestra run --verbose
```

### Check Logs

```bash
# Pipeline logs
tail -f logs/pipeline-*.log

# Pretty-print JSON logs
tail -f logs/pipeline-*.log | jq .

# Filter by level
tail -f logs/pipeline-*.log | jq 'select(.level == "ERROR")'
```

### Interactive Debugging

```python
# Add breakpoint in code
import pdb; pdb.set_trace()

# Or use ipdb for better UX
import ipdb; ipdb.set_trace()
```

---

## 📚 Additional Resources

### Internal Documentation

- **[README.md](README.md)** - Project overview
- **[NEXT_STEPS.md](NEXT_STEPS.md)** - Roadmap and TODOs
- **[docs/architecture.md](docs/architecture.md)** - System design
- **[docs/configuration.md](docs/configuration.md)** - Config reference
- **[docs/cloud-migration.md](docs/cloud-migration.md)** - Cloud migration

### External Resources

- [PEP 8 Style Guide](https://pep8.org/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- [Conventional Commits](https://www.conventionalcommits.org/)
- [HIPAA Security Rule](https://www.hhs.gov/hipaa/for-professionals/security/index.html)

---

## 🎯 Quick Reference

### Common Commands

```bash
# Development
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"

# Code quality
black lorchestra/
ruff check --fix lorchestra/

# Testing
pytest tests/
pytest --cov=lorchestra tests/

# Running
lorchestra run --dry-run
lorchestra validate
lorchestra status

# Debugging
lorchestra run --verbose --stage canonize
tail -f logs/pipeline-*.log | jq .
```

### Need Help?

- Check [README.md](README.md) for usage
- Check [NEXT_STEPS.md](NEXT_STEPS.md) for TODOs
- Check [docs/](docs/) for architecture
- Review existing code for examples

---

**Thank you for contributing to lorch!** 🎉
