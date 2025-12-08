---
version: "0.1"
tier: C
title: Independent Package Config
owner: benthepsychologist
goal: Refactor lorchestra to use independent configuration discovery and loading
labels: [refactor, core]
orchestrator_contract: "standard"
repo:
  working_branch: "feat/independent-package-config"
---

# Independent Package Config

## Objective

> Refactor `lorchestra` to stop living inside its own repo and behave like a proper installed tool by implementing a configuration model, loader, and init command.

### Background

Currently, `lorchestra` relies on repo-local dotfiles (`.env`, `.canonizer`, etc.) and hardcoded paths like `/workspace/lorchestra`. This limits its portability and usability as an installed package. We need to implement a standard configuration discovery mechanism (`~/.config/lorchestra/config.yaml`) to decouple the tool from the repository structure.

### Acceptance Criteria

- [ ] `lorchestra init` command creates `~/.config/lorchestra/config.yaml` and optional `.env`
- [ ] `LorchestraConfig` model accurately represents all required paths and settings
- [ ] Job runner and processors respect config values instead of hardcoded paths
- [ ] `LORCHESTRA_HOME` environment variable overrides default config location
- [ ] Existing tests pass with mocked configuration

### Constraints

- Must maintain backward compatibility or provide a clear migration path
- Configuration must be loadable without a full CLI instantiation (for library usage)
- Secrets should remain in `.env` (referenced by config) to avoid checking them into version control

## Plan

### Step 1: Config Model & Loader [G0: Plan Approval]

**Prompt:**

Implement the core configuration logic for `lorchestra`.
1. Create a `LorchestraConfig` dataclass/model in `lorchestra/config.py` with fields:
   - `project`, `dataset_raw`, `dataset_canonical`, `dataset_derived`
   - `sqlite_path`, `local_views_root`
   - `canonizer_registry_root`, `formation_registry_root` (optional)
   - `env_file` (optional)
2. Implement `get_lorchestra_home()` resolving `LORCHESTRA_HOME` or `~/.config/lorchestra`.
3. Implement `load_config()` to read `config.yaml` and optionally load `.env`.
4. Export these from `lorchestra/__init__.py`.
5. Add unit tests in `tests/test_config.py`.

**Commands:**

```bash
# Run new tests
pytest tests/test_config.py -v
```

**Outputs:**
- `lorchestra/config.py`
- `lorchestra/__init__.py` (updated)
- `tests/test_config.py`

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Architecture Review
- [ ] Config model includes all necessary fields
- [ ] Home path resolution logic is robust (env var > default)
- [ ] Error handling for missing config is clear

##### Code Quality
- [ ] Type hints are used throughout
- [ ] Tests cover happy path and missing file scenarios

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

### Step 2: Wire Config into CLI [G1: Design Review]

**Prompt:**

Integrate the cleaned configuration loading into the CLI entry point.
1. Modify `lorchestra/cli.py` to load config in the `main` callback.
2. Store the config object in the click/typer context (`ctx.obj`).
3. Update specific commands (like `run`) to retrieve the config from context.
4. Update `lorchestra/job_runner.py` to accept a `config` argument in `run_job`.
5. Pass the config instance down to `run_job`.

**Commands:**
```bash
# Verify CLI help still works
lorchestra --help
```

**Outputs:**
- `lorchestra/cli.py`
- `lorchestra/job_runner.py`

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Design Quality
- [ ] CLI context correctly propagates config
- [ ] `run_job` signature update is clean
- [ ] No circular dependencies introduced

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

### Step 3: Implement Init Command [G2: Code Readiness]

**Prompt:**

Add a `lorchestra init` command to bootstrap the configuration.
1. In `lorchestra/cli.py`, add an `init` command.
2. It should check for `LORCHESTRA_HOME` existence and create it.
3. Write a default `config.yaml` with sensible defaults (pointing to `~/lifeos/...`).
4. Optionally create a `.env` template if one doesn't exist.
5. Print helpful next steps (e.g. "Run `can init`...").
6. Add a test for the init command in `tests/test_cli.py`.

**Commands:**
```bash
# Test the init command with a custom home
LORCHESTRA_HOME=/tmp/ltest lorchestra init
cat /tmp/ltest/config.yaml
rm -rf /tmp/ltest
```

**Outputs:**
- `lorchestra/cli.py`
- `tests/test_cli.py`

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Functionality
- [ ] Init command creates valid YAML
- [ ] Idempotency checked (doesn't overwrite without flag)
- [ ] Permissions/directory creation handled

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

### Step 4: Migrate Hardcoded Paths [G3: Pre-Release]

**Prompt:**

Replace all hardcoded path assumptions with configuration values.
1. Scan `lorchestra/processors/*.py`, `lorchestra/storage/*.py`, and `lorchestra/sql_runner.py`.
2. Replace usage of `os.environ` or hardcoded strings with `config.<field>`.
3. Update `BigQueryStorageClient` and other client factories to accept config parameters designated in Step 1.
4. Ensure `sqlite_path` and `local_views_root` are resolved from config.

**Commands:**
```bash
# Run full test suite to ensure no regressions
pytest tests/
```

**Outputs:**
- `lorchestra/processors/projection.py`
- `lorchestra/storage/bigquery_client.py` (if applicable/present)
- `lorchestra/sql_runner.py`
- Other affected files found during scan

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Code Quality
- [ ] All hardcoded paths removed
- [ ] Fallbacks removed (fail fast if config missing)
- [ ] Tests pass with mocked config

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

### Step 5: Verification & Documentation [G4: Final Approval]

**Prompt:**

Verify the full flow and update documentation.
1. Perform a manual verification by initializing a fresh config and running a dry-run job.
2. Update `README.md` to explain the new `init` flow and configuration file structure.
3. Document how to set up `LORCHESTRA_HOME`.

**Commands:**
```bash
spec gate-report
```

**Outputs:**
- `README.md`
- `walkthrough.md` (proof of work)

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Documentation
- [ ] README updated with new setup instructions
- [ ] Migration notes added for existing users

##### Verification
- [ ] Manual walkthrough completed
- [ ] End-to-end flow validated

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

## Orchestrator

**State Machine:** Standard

**Tools:** bash, pytest, uv, git

## Repository

**Branch:** `feat/independent-package-config`
**Merge Strategy:** squash