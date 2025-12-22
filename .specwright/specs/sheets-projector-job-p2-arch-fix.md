---
version: "0.1"
tier: B
title: "Sheets Projector Job Part 2: Architecture Fix"
owner: claude
goal: "Remove gorch import from lorchestra - enforce clean dependency boundaries"
labels: [architecture, refactor, security]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-19T16:01:10.028220+00:00
updated: 2025-12-19T16:01:10.028220+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/sheets-projector-job-p2-arch-fix"
---

# Sheets Projector Job Part 2: Architecture Fix

## Objective

> Remove gorch import from lorchestra by moving the sheets write service and factory to gorch. Lorchestra loads factory by string via importlib with allowlist security.

## Acceptance Criteria

- [ ] lorchestra has zero gorch imports (`grep -r "from gorch\|import gorch" lorchestra/` returns nothing)
- [ ] GorchSheetsWriteService in gorch implements write_table() protocol (duck-typed)
- [ ] load_service_factory() has allowlist restriction (security)
- [ ] Factory args come from job spec (no hardcoded "gdrive")
- [ ] All existing tests pass
- [ ] Security regression test: loader rejects non-allowed prefixes
- [ ] `lorchestra run proj_sheets_proj_clients --dry-run` passes (runtime smoke test)

## Context

### Background

Part 1 of sheets-projector-job introduced a ProjectionistProcessor that imported gorch at runtime via a factory function. This violates the architectural constraint that lorchestra must not import gorch.

### Architecture Doctrine (enforced)

- **gorch**: low-level Google client harmonizer (no deps on higher layers)
- **storacle**: auth-agnostic I/O adapter (receives pre-authenticated clients)
- **lorchestra**: orchestrator (never imports gorch)

### Constraints

- gorch must not depend on storacle (no layer inversion)
- Factory loader must have allowlist (security)
- No hardcoded auth account names in lorchestra

## Plan

### Step 1: Add GorchSheetsWriteService to gorch [G1: gorch Ready]

**Role:** agentic

**Prompt:**

**Pre-check:** Confirm `/workspace/gorch/src/gorch/sheets/__init__.py` exists (required for string import). If not, create empty file.

Create `/workspace/gorch/src/gorch/sheets/services.py` with `GorchSheetsWriteService`:
- Duck-typed (does not import storacle)
- Implements write_table(spreadsheet_id, sheet_name, values, strategy)
- Has from_authctl() class method
- Wraps SheetsClient.clear_and_update() and append_values()

Create `/workspace/gorch/src/gorch/sheets/factories.py` with `build_sheets_write_service(account)`:
- Returns GorchSheetsWriteService.from_authctl(account)

Note: No updates to gorch/__init__.py exports - string import works without explicit exports.

**Storacle note:** `storacle.clients.sheets.SheetsWriteService` remains for other contexts (non-gorch clients). Two similar services is acceptable debt; deprecate storacle's version later if unused.

**Allowed Paths:**

- /workspace/gorch/src/gorch/sheets/__init__.py
- /workspace/gorch/src/gorch/sheets/services.py
- /workspace/gorch/src/gorch/sheets/factories.py
- /workspace/gorch/tests/test_sheets_services.py
- /workspace/gorch/tests/test_sheets_factories.py

**Verification Commands:**

```bash
cd /workspace/gorch && ruff check src/
cd /workspace/gorch && pytest tests/test_sheets_services.py tests/test_sheets_factories.py -v
```

**Outputs:**

- /workspace/gorch/src/gorch/sheets/services.py
- /workspace/gorch/src/gorch/sheets/factories.py
- /workspace/gorch/tests/test_sheets_services.py
- /workspace/gorch/tests/test_sheets_factories.py

### Step 2: Add secure factory loader to lorchestra [G2: lorchestra Ready]

**Role:** agentic

**Prompt:**

Add to `/workspace/lorchestra/lorchestra/job_runner.py`:

**Import safety note:** Currently safe - processors imported lazily inside `run_job()`, not at module top-level. If circular import occurs after this change, move loader to `lorchestra/util/factory_loader.py` and import from there.

```python
import importlib

# Allowlist for service factory modules - security restriction
# Only exact matches or submodules allowed (prefix + ".")
# Add new modules only when needed - don't pre-authorize
ALLOWED_FACTORY_MODULES = [
    "gorch.sheets.factories",
]


def _is_allowed_module(module_path: str) -> bool:
    """Check if module is in allowlist (exact match or submodule)."""
    for allowed in ALLOWED_FACTORY_MODULES:
        if module_path == allowed or module_path.startswith(allowed + "."):
            return True
    return False


def load_service_factory(factory_path: str):
    """Load a service factory by dotted path string.

    Only allows factories from allowlisted modules (exact match or submodules).

    Args:
        factory_path: e.g. "gorch.sheets.factories:build_sheets_write_service"

    Raises:
        ValueError: If path not in allowlist or malformed
        ImportError: If module not found
        AttributeError: If function not found in module
        TypeError: If attribute is not callable
    """
    if ":" not in factory_path:
        raise ValueError(f"Factory path must be 'module:function', got: {factory_path}")

    module_path, func_name = factory_path.rsplit(":", 1)

    # Security: restrict to allowed modules (exact or submodule)
    if not _is_allowed_module(module_path):
        raise ValueError(
            f"Factory module '{module_path}' not in allowlist. "
            f"Allowed: {ALLOWED_FACTORY_MODULES}"
        )

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise ImportError(f"Cannot import factory module '{module_path}': {e}") from e

    try:
        factory = getattr(module, func_name)
    except AttributeError as e:
        raise AttributeError(f"Factory function '{func_name}' not found in '{module_path}': {e}") from e

    if not callable(factory):
        raise TypeError(f"{factory_path} is not callable")

    return factory
```

Update `/workspace/lorchestra/lorchestra/processors/projectionist.py`:
- Remove create_sheets_service() function entirely
- Remove `from storacle.clients.sheets import SheetsWriteService`
- Add `from lorchestra.job_runner import load_service_factory` at module top
- Change sheets_service creation to use factory from job spec

Update job definition `/workspace/lorchestra/lorchestra/jobs/definitions/projection/sync/proj_sheets_proj_clients.json`:
- Add sheets_service_factory path
- Add sheets_service_factory_args

**Allowed Paths:**

- /workspace/lorchestra/lorchestra/job_runner.py
- /workspace/lorchestra/lorchestra/processors/projectionist.py
- /workspace/lorchestra/lorchestra/jobs/definitions/projection/sync/proj_sheets_proj_clients.json
- /workspace/lorchestra/tests/test_sheets_projection_processor.py
- /workspace/lorchestra/tests/test_job_runner.py

**Verification Commands:**

```bash
cd /workspace/lorchestra && ruff check lorchestra/
grep -r "from gorch\|import gorch" /workspace/lorchestra/lorchestra/ && exit 1 || echo "No gorch imports"
cd /workspace/lorchestra && pytest tests/test_sheets_projection_processor.py tests/test_job_runner.py -v
```

**Tests for load_service_factory (in tests/test_job_runner.py):**
1. Rejects non-allowed prefixes (security regression test)
2. Raises TypeError when attribute exists but is not callable
3. Raises ImportError with readable message when module doesn't exist
4. Raises AttributeError with readable message when function doesn't exist
5. Raises ValueError when path is malformed (no colon)

**Outputs:**

- /workspace/lorchestra/lorchestra/job_runner.py (modified)
- /workspace/lorchestra/lorchestra/processors/projectionist.py (modified)
- /workspace/lorchestra/tests/test_job_runner.py (new: security + error handling tests)

### Step 3: Integration test [G3: Integration Complete]

**Prompt:**

Run full verification:
- ruff check on all packages (gorch + lorchestra)
- All tests pass (gorch + lorchestra)
- Verify no gorch imports in lorchestra
- Dry-run the job

**Commands:**

Note: All tests run in lorchestra's venv which has gorch installed as editable dependency.

```bash
# Activate shared venv
source /workspace/lorchestra/.venv/bin/activate

# gorch checks
cd /workspace/gorch && ruff check src/
cd /workspace/gorch && pytest tests/ -v

# lorchestra checks
cd /workspace/lorchestra && ruff check lorchestra/
cd /workspace/lorchestra && pytest tests/ -v

# Zero gorch imports in lorchestra
grep -r "from gorch\|import gorch" /workspace/lorchestra/lorchestra/ && exit 1 || echo "Clean: no gorch imports"

# Dry-run
cd /workspace/lorchestra && lorchestra run proj_sheets_proj_clients --dry-run
```

## Models & Tools

**Tools:** bash, pytest, ruff

**Models:** (to be filled by defaults)

## Repository

**Branch:** `feat/sheets-projector-job-p2-arch-fix`

**Merge Strategy:** squash
