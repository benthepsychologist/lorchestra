---
version: "0.1"
tier: C
title: Rename package from lorch to lorchestra
owner: benthepsychologist
goal: Systematically rename all references from "lorch" to "lorchestra" across entire codebase
labels: [refactor, rename, breaking-change]
project_slug: lorch
spec_version: 1.0.0
created: 2025-11-14T00:00:00+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "main"
---

# Rename Package: lorch → lorchestra

## Objective

> Systematically rename the package from "lorch" to "lorchestra"

Rename all references across the entire codebase including:
- Package directory and module structure (15 Python files)
- All Python imports (`from lorch` → `from lorchestra`)
- CLI command and entry points (`lorch` → `lorchestra`)
- Package metadata in pyproject.toml
- All documentation files (11 files, 350+ references)
- Configuration files (3 files)
- Build artifacts

This is a **breaking change** that preserves all functionality while updating naming.

## Acceptance Criteria

- [ ] Package directory renamed: `lorch/` → `lorchestra/`
- [ ] All Python imports updated: `from lorch` → `from lorchestra`
- [ ] CLI command renamed: `lorch` → `lorchestra` in pyproject.toml
- [ ] Package metadata updated in pyproject.toml (name, console_scripts, include)
- [ ] All 15 Python files updated with new imports
- [ ] All 11 documentation files updated (README, CONTRIBUTING, docs/*)
- [ ] All 3 configuration files updated
- [ ] Build artifacts cleaned and package rebuilt
- [ ] Package installs successfully as `lorchestra`
- [ ] `lorchestra --help` command works
- [ ] `lorchestra validate` runs without import errors
- [ ] No references to old "lorch" name remain (except in git history/historical docs)
- [ ] Changes committed with clear breaking change message
- [ ] Changes pushed to remote

## Context

### Background

The package is currently named "lorch" (Local Orchestrator) but we want to rename it to "lorchestra" for better branding and clarity.

### Impact Analysis

Based on comprehensive codebase analysis:

**Python Files (15 files):**
- `lorchestra/__init__.py` - docstring
- `lorchestra/cli.py` - 80+ references (imports, prog_name, all command docstrings)
- `lorchestra/config.py` - docstring
- `lorchestra/pipeline.py` - 6 references (imports + docstring)
- `lorchestra/utils.py` - docstring
- `lorchestra/stages/*.py` - 4 files, imports + docstrings
- `lorchestra/tools/*.py` - 5 files, imports + docstrings

**Documentation Files (11 files, 350+ total references):**
- `README.md` - 186 references
- `CONTRIBUTING.md` - 29 references
- `NEXT_STEPS.md` - 10 references
- `docs/architecture.md` - 15 references
- `docs/configuration.md` - 2 references
- `docs/cloud-migration.md` - 9 references
- `docs/transforms.md` - 2 references
- `docs/whats_next.md` - 4 references
- `docs/vault.md` - 12 references
- `docs/time_based_extraction.md` - 27 references
- `docs/gmail-config-fixes.md` - 7 references

**Configuration Files (3 files):**
- `config/pipeline.yaml` - comment
- `.specwright.yaml` - paths
- `.specwright/aips/lorch-adapter-refactor.yaml` - project_slug, paths

**Build Artifacts (will be regenerated):**
- `build/lib/lorch/` - all files
- `lorch.egg-info/` - package metadata
- `dist/` - distribution files

### Constraints

- Maintain all functionality - this is purely a rename
- No logic changes
- Preserve git history
- Update .specwright files but keep historical AIPs as-is (for audit trail)
- Root directory rename is optional (can stay `/home/user/lorch` if needed)

## Plan

### Step 1: Rename Package Directory [G0: Design Approval]

**Prompt:**

Rename the main Python package directory:

```bash
cd /home/user/lorch
mv lorch lorchestra
```

Verify the directory structure:
```bash
ls -la lorchestra/
ls -la lorchestra/stages/
ls -la lorchestra/tools/
```

**Outputs:**

- `lorchestra/` directory created
- All subdirectories preserved: `stages/`, `tools/`
- All files intact

---

### Step 2: Update Package Metadata [G0: Design Approval]

**Prompt:**

Update `pyproject.toml` to use new package name:

**Changes needed:**
- Line 2: `name = "lorch"` → `name = "lorchestra"`
- Line 4: Update description to mention "lorchestra" instead of "lorch"
- Line 24: `lorch = "lorch.cli:main"` → `lorchestra = "lorchestra.cli:main"`
- Line 32: `include = ["lorch*"]` → `include = ["lorchestra*"]`

**Commands:**
```bash
cat pyproject.toml | grep -E "name =|lorchestra ="
```

**Outputs:**

- `pyproject.toml` (updated)

---

### Step 3: Update Core Python Module Imports [G1: Code Readiness]

**Prompt:**

Update the main package modules with new import paths:

**Files to update:**

1. **lorchestra/__init__.py:**
   - Line 2: Docstring `"""lorch` → `"""lorchestra`

2. **lorchestra/cli.py:**
   - Line 2: Docstring `"""CLI interface for lorch` → `"""CLI interface for lorchestra`
   - Line 11: `from lorch import __version__` → `from lorchestra import __version__`
   - Line 12: `from lorch.config` → `from lorchestra.config`
   - Line 13: `from lorch.pipeline` → `from lorchestra.pipeline`
   - Line 14: `from lorch.utils` → `from lorchestra.utils`
   - Line 25: `prog_name="lorch"` → `prog_name="lorchestra"`
   - Line 28: Docstring `"""lorch - Local` → `"""lorchestra - Local`
   - All docstrings: Replace command examples `lorch` → `lorchestra` (80+ occurrences)

3. **lorchestra/config.py:**
   - Line 2: Docstring `"""Configuration management for lorch` → `"""Configuration management for lorchestra`

4. **lorchestra/pipeline.py:**
   - Line 2: Docstring `"""Pipeline orchestrator for lorch` → `"""Pipeline orchestrator for lorchestra`
   - Line 14: `from lorch.config` → `from lorchestra.config`
   - Line 15: `from lorch.stages.base` → `from lorchestra.stages.base`
   - Line 16: `from lorch.stages.canonize` → `from lorchestra.stages.canonize`
   - Line 17: `from lorch.stages.extract` → `from lorchestra.stages.extract`
   - Line 18: `from lorch.stages.index` → `from lorchestra.stages.index`
   - Line 19: `from lorch.utils` → `from lorchestra.utils`

5. **lorchestra/utils.py:**
   - Docstring updates

**Commands:**
```bash
ruff check lorchestra/
```

**Outputs:**

- 5 core module files updated
- Clean ruff check

---

### Step 4: Update Stage Modules [G1: Code Readiness]

**Prompt:**

Update all stage modules with new imports:

**Files to update:**

1. **lorchestra/stages/__init__.py:**
   - Line 2: Docstring `"""Pipeline stages for lorch` → `"""Pipeline stages for lorchestra`

2. **lorchestra/stages/base.py:**
   - Line 14: `from lorch.config` → `from lorchestra.config`
   - Line 200: `.lorch_write_test` → `.lorchestra_write_test`

3. **lorchestra/stages/extract.py:**
   - Line 11: `from lorch.config` → `from lorchestra.config`
   - Line 12: `from lorch.stages.base` → `from lorchestra.stages.base`
   - Line 13: `from lorch.tools.meltano` → `from lorchestra.tools.meltano`
   - Line 14: `from lorch.utils` → `from lorchestra.utils`

4. **lorchestra/stages/canonize.py:**
   - Line 9: `from lorch.config` → `from lorchestra.config`
   - Line 10: `from lorch.stages.base` → `from lorchestra.stages.base`
   - Line 11: `from lorch.tools.canonizer` → `from lorchestra.tools.canonizer`

5. **lorchestra/stages/index.py:**
   - Line 10: `from lorch.config` → `from lorchestra.config`
   - Line 11: `from lorch.stages.base` → `from lorchestra.stages.base`
   - Line 12: `from lorch.tools.vector_projector` → `from lorchestra.tools.vector_projector`

**Commands:**
```bash
ruff check lorchestra/stages/
```

**Outputs:**

- 5 stage module files updated
- Clean ruff check

---

### Step 5: Update Tool Adapter Modules [G1: Code Readiness]

**Prompt:**

Update all tool adapter modules with new imports:

**Files to update:**

1. **lorchestra/tools/__init__.py:**
   - Line 1: Docstring `"""Tool adapters for orchestrating external tools in lorch` → `"""in lorchestra`
   - Line 3: `from lorch.tools.base` → `from lorchestra.tools.base`
   - Line 4: `from lorch.tools.meltano` → `from lorchestra.tools.meltano`

2. **lorchestra/tools/base.py:**
   - Docstring updates

3. **lorchestra/tools/meltano.py:**
   - Line 9: `from lorch.tools.base` → `from lorchestra.tools.base`

4. **lorchestra/tools/canonizer.py:**
   - Line 11: `from lorch.tools.base` → `from lorchestra.tools.base`

5. **lorchestra/tools/vector_projector.py:**
   - Line 9: `from lorch.tools.base` → `from lorchestra.tools.base`
   - Line 10: `from lorch.utils` → `from lorchestra.utils`

**Commands:**
```bash
ruff check lorchestra/tools/
```

**Outputs:**

- 5 tool module files updated
- Clean ruff check

---

### Step 6: Update README.md [G1: Code Readiness]

**Prompt:**

Update `README.md` with 186 references to replace:

**Pattern replacements:**
- Title: `# lorch - Local Orchestrator` → `# lorchestra - Local Orchestrator`
- All command examples: `lorch` → `lorchestra`
- All descriptions: "lorch orchestrates" → "lorchestra orchestrates"
- Path references: `cd /home/user/lorch` → `cd /home/user/lorchestra` (optional - can keep old path)
- Directory structure: `lorch/` → `lorchestra/`
- Test commands: `pytest --cov=lorch` → `pytest --cov=lorchestra`
- Code formatting: `black lorch/` → `black lorchestra/`
- Linting: `ruff check lorch/` → `ruff check lorchestra/`

**Note:** Keep installation path flexible - can be either `/home/user/lorch` or `/home/user/lorchestra`

**Outputs:**

- `README.md` (updated with 186+ changes)

---

### Step 7: Update CONTRIBUTING.md [G1: Code Readiness]

**Prompt:**

Update `CONTRIBUTING.md` with 29 references:

**Changes:**
- Line 1: `# Contributing to lorch` → `# Contributing to lorchestra`
- Line 3: Thank you message
- Line 34, 44: Path and command examples
- Lines 54-55: Directory structure references
- Lines 101-107: Code style command examples
- Line 220: Import example: `from lorch.config` → `from lorchestra.config`
- Lines 253-503: All command examples
- Line 516: Closing message

**Outputs:**

- `CONTRIBUTING.md` (updated)

---

### Step 8: Update NEXT_STEPS.md [G1: Code Readiness]

**Prompt:**

Update `NEXT_STEPS.md` with 10 references:

**Changes:**
- Line 1: `# Next Steps for lorch Development` → `lorchestra Development`
- Line 3: Description of orchestrator
- Command examples throughout
- Path references: `lorch/stages/index.py` → `lorchestra/stages/index.py`
- Line 606: `Maintained By: lorch development team` → `lorchestra development team`

**Outputs:**

- `NEXT_STEPS.md` (updated)

---

### Step 9: Update Documentation Directory [G1: Code Readiness]

**Prompt:**

Update all files in `docs/` directory:

**Files and reference counts:**

1. **docs/architecture.md** (15 refs):
   - Title, descriptions, ASCII diagrams
   - Component mapping table
   - Path references

2. **docs/configuration.md** (2 refs):
   - Command examples

3. **docs/cloud-migration.md** (9 refs):
   - Pipeline translation descriptions
   - Code/path references
   - Commands

4. **docs/transforms.md** (2 refs):
   - Pipeline description
   - Command example

5. **docs/whats_next.md** (4 refs):
   - Title
   - Descriptions
   - Path references
   - Commands

6. **docs/vault.md** (12 refs):
   - Descriptions
   - Commands
   - Code file path references in links

7. **docs/time_based_extraction.md** (27 refs):
   - Title: `# Time-Based Extraction with lorch` → `lorchestra`
   - All command examples: `lorch extract` → `lorchestra extract`
   - File path references

8. **docs/gmail-config-fixes.md** (7 refs):
   - Command examples and descriptions

**Outputs:**

- All 8 documentation files updated

---

### Step 10: Update Configuration Files [G1: Code Readiness]

**Prompt:**

Update configuration files:

**Files:**

1. **config/pipeline.yaml:**
   - Line 1: `# lorch Pipeline Configuration` → `# lorchestra Pipeline Configuration`

2. **.specwright.yaml:**
   - Line 11-12: Update paths if they reference "lorch" (keep historical references intact)

3. **.specwright/aips/lorch-adapter-refactor.yaml:**
   - Line 242: `project_slug: lorch` → `project_slug: lorchestra`
   - Path references (update if needed, or keep for historical accuracy)
   - Git URL: Note the repository name (will be updated after GitHub rename)

**Note:** Historical spec files can keep old references for audit trail purposes.

**Outputs:**

- Configuration files updated appropriately

---

### Step 11: Clean Build Artifacts [G2: Pre-Release]

**Prompt:**

Remove all build artifacts and old package metadata:

```bash
cd /home/user/lorch
rm -rf build/
rm -rf dist/
rm -rf lorch.egg-info/
rm -rf lorchestra.egg-info/
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
```

Verify cleanup:
```bash
ls -la | grep -E "build|dist|egg-info"
```

**Validation:**
- ✓ No build/ directory
- ✓ No dist/ directory
- ✓ No egg-info directories
- ✓ Python cache cleaned

**Outputs:**

- Clean build environment

---

### Step 12: Reinstall Package [G2: Pre-Release]

**Prompt:**

Uninstall old package and install new one:

```bash
cd /home/user/lorch
pip uninstall lorch -y 2>/dev/null || echo "lorch not installed"
pip install -e .
```

Verify installation:
```bash
pip list | grep lorchestra
which lorchestra
lorchestra --version
```

**Validation:**
- ✓ Old "lorch" package uninstalled
- ✓ New "lorchestra" package installed
- ✓ `lorchestra` command available in PATH
- ✓ Version displays correctly

**Outputs:**

- `lorchestra` package installed successfully

---

### Step 13: Test Basic Functionality [G2: Pre-Release]

**Prompt:**

Test that core commands work without import errors:

```bash
# Test version
lorchestra --version

# Test help
lorchestra --help

# Test validation
lorchestra validate

# Test list commands
lorchestra list extractors
lorchestra list transforms

# Test config commands
lorchestra config show meltano

# Test tools commands
lorchestra tools list
lorchestra tools validate meltano
```

**Validation:**
- ✓ All commands execute without ImportError
- ✓ No references to "lorch" in command output
- ✓ Help text displays "lorchestra" correctly
- ✓ Configuration loads properly
- ✓ All adapters accessible

**Outputs:**

- Test results confirmed
- Screenshots/logs of successful commands (optional)

---

### Step 14: Verify No Lingering References [G2: Pre-Release]

**Prompt:**

Search for any remaining "lorch" references that should be updated:

```bash
cd /home/user/lorch
# Search Python files
grep -r "from lorch" --include="*.py" lorchestra/ || echo "✓ No old imports found"
grep -r "import lorch" --include="*.py" lorchestra/ || echo "✓ No old imports found"

# Search documentation (excluding git history and this spec)
grep -r "lorch " --include="*.md" --exclude-dir=.git --exclude="*adapter-refactor*" . | head -20

# Check key files
grep "prog_name" lorchestra/cli.py
grep "name =" pyproject.toml | head -1
```

**Expected:**
- No `from lorch` imports in Python files
- Only historical references in old specs (acceptable)
- New package name in all active configuration

**Outputs:**

- Verification report
- List of any remaining references (if any need addressing)

---

### Step 15: Commit Changes [G3: Post-Implementation]

**Prompt:**

Commit all changes with clear breaking change message:

```bash
cd /home/user/lorch
git add -A
git status
```

Create commit:
```bash
git commit -m "$(cat <<'EOF'
Rename package from lorch to lorchestra

BREAKING CHANGE: Package name changed from 'lorch' to 'lorchestra'

This is a complete rename of the package for better branding and clarity.
All functionality is preserved - only naming has changed.

**Changes:**
- Renamed package directory: lorch/ → lorchestra/
- Updated all Python imports: from lorch → from lorchestra
- Updated CLI command: lorch → lorchestra
- Updated package metadata in pyproject.toml
- Updated all documentation files (11 files, 350+ references)
- Updated configuration files
- Cleaned and rebuilt package

**Migration for users:**
```bash
pip uninstall lorch
pip install lorchestra  # or: pip install -e .
```

**New usage:**
```bash
lorchestra --help
lorchestra extract <tap>
lorchestra run --stage canonize
```

All previous lorch commands now use lorchestra:
- lorch → lorchestra
- from lorch.* → from lorchestra.*

🤖 Generated with Claude Code
Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

**Validation:**
- ✓ All changed files staged
- ✓ Commit message includes BREAKING CHANGE tag
- ✓ Migration instructions provided

**Outputs:**

- Git commit created
- Commit hash displayed

---

### Step 16: Push Changes [G3: Post-Implementation]

**Prompt:**

Push changes to remote:

```bash
git push origin main
```

**Optional:** If you want to rename the GitHub repository:
1. Go to https://github.com/benthepsychologist/lorch/settings
2. Repository name → Change to "lorchestra"
3. Confirm the rename
4. Update local remote:
   ```bash
   git remote set-url origin git@github.com:benthepsychologist/lorchestra.git
   git remote -v
   ```

**Outputs:**

- Changes pushed to main branch
- GitHub repository renamed (optional)
- Remote URL updated (if renamed)

---

## Models & Tools

**Tools:** bash, pip, git, ruff, grep

**Models:** (use defaults)

## Repository

**Branch:** main

**Merge Strategy:** Direct push (working on main)

## Notes

### Breaking Change Notice

This is a **breaking change** for anyone using the package:
- CLI command changes from `lorch` to `lorchestra`
- All imports need updating: `from lorch` → `from lorchestra`
- Package name in requirements.txt/pyproject.toml must be updated

### Version Bump Recommendation

Consider bumping version to signal breaking change:
- Current: 0.1.0
- Recommended: 0.2.0 (minor bump) or 1.0.0 (major release)

Update in `lorchestra/__init__.py` after rename is complete.

### Root Directory

The root directory can optionally be renamed from `/home/user/lorch` to `/home/user/lorchestra`. This is not required for the package to work but may be cleaner. If renamed, update:
- IDE workspace settings
- Terminal bookmarks
- Any scripts referencing the path

### Historical Documentation

Old spec files in `.specwright/aips/` and `.specwright/specs/` can keep "lorch" references for historical accuracy and audit trail purposes. Only update active/current references.
