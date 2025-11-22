# Lorchestra Slim-Down Progress Report

## Current Status: Step 1 Complete ✅

### What's Been Completed

**Step 1: Job Orchestration Implementation** ✅
- ✅ Created `lorchestra/jobs.py` with:
  - `discover_jobs()` - discovers jobs via entrypoints
  - `get_job()` - retrieves specific job
  - `execute_job()` - executes job with error handling

- ✅ Added new CLI commands to `lorchestra/cli.py`:
  - `lorchestra run-job package job --account X --since Y --until Z`
  - `lorchestra jobs list [package]`
  - `lorchestra jobs show package job`

- ✅ Created comprehensive test suite:
  - `tests/test_jobs.py` - 9 tests for job discovery/execution (all passing)
  - `tests/test_cli_jobs.py` - 8 tests for CLI commands (all passing)

- ✅ Added `google-cloud-bigquery>=3.0` dependency to `pyproject.toml`

- ✅ Committed event_client implementation from previous spec (commit 89954ac)

- ✅ Created safety tag: `pre-slim-lorchestra` (before deletion)

### What Still Needs to Be Done

**Step 2: Remove Vault and Domain Code** (NOT STARTED)
- [ ] Delete `lorchestra/stages/` directory
- [ ] Delete `lorchestra/tools/` directory
- [ ] Delete or simplify `lorchestra/pipeline.py`
- [ ] Remove old CLI commands from `lorchestra/cli.py`:
  - `run` (old pipeline runner)
  - `extract`
  - `status`
  - `validate`
  - `clean`
  - `list` group (old one for extractors/transforms)
  - `config` group (references deleted tools)
  - `tools` group (references deleted adapters)

**Step 3: Documentation** (NOT STARTED)
- [ ] Create `docs/jobs.md` - job development guide
- [ ] Update `README.md` - reflect thin orchestrator role
- [ ] Update `lorchestra/cli.py` docstring (change from "Coordinates extract → canonize → index" to job orchestrator)

**Step 4: Final Validation** (NOT STARTED)
- [ ] Run all tests: `python -m pytest tests/ -v`
- [ ] Run linting: `ruff check lorchestra/`
- [ ] Verify no imports from deleted modules
- [ ] Update execution log in `.aip_artifacts/claude-execution.log`
- [ ] Commit all changes

### Files Changed So Far (uncommitted)

**New files:**
- `lorchestra/jobs.py`
- `tests/test_jobs.py`
- `tests/test_cli_jobs.py`

**Modified files:**
- `lorchestra/cli.py` - added new commands at end (lines 1468-1547)
- `pyproject.toml` - added google-cloud-bigquery dependency

**Tagged:**
- `pre-slim-lorchestra` - safety snapshot before deletion

### Important Context

1. **Safety tag created**: `pre-slim-lorchestra` allows recovery of old code if needed
2. **Old commands still present**: The CLI has BOTH old and new commands right now
3. **Next major task**: Delete the old code (stages/, tools/, old CLI commands)
4. **Current working directory**: `/workspace/lorchestra`
5. **Branch**: `main` (1 commit ahead of origin after event_client commit)

### Continue Instructions

When you resume, execute this command:

```bash
# Check current state
git status
ls -la lorchestra/
python -m pytest tests/ -v  # Should show 26 tests passing (9 + 8 + 9 event_client)

# Then proceed with Step 2: Deletion
# 1. Delete directories
rm -rf lorchestra/stages/
rm -rf lorchestra/tools/

# 2. Delete or simplify pipeline.py
# 3. Remove old CLI commands (careful editing of cli.py)
# 4. Create documentation
# 5. Final validation and commit
```

### Key Files to Edit Next

1. **lorchestra/cli.py** - Remove lines 35-1466 (all old commands), keep only new job commands
2. **lorchestra/pipeline.py** - Delete or heavily simplify
3. **docs/jobs.md** - Create from spec template
4. **README.md** - Update description

### Testing Strategy

After deletions, expect some tests to break. The existing tests that will likely break:
- Any tests importing from `lorchestra.stages.*`
- Any tests importing from `lorchestra.tools.*`
- Any tests for old CLI commands

Safe to delete those test files too since we're removing that functionality.

---

**Resume with:** "Continue with Step 2: Remove vault and domain code. Delete lorchestra/stages/ and lorchestra/tools/ directories, then remove old CLI commands."
