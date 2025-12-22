---
version: "0.1"
tier: B
title: "Align Sheets Projection Around a Semver Storacle Plan"
owner: benthepsychologist
goal: "Align Sheets projection around a semver IO plan: projectionist emits, storacle executes, lorchestra orchestrates"
labels: [architecture, refactor]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-22T10:17:31.507864+00:00
updated: 2025-12-22T10:17:31.507864+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/storacle-and-sheets-v3"
---

# Align Sheets Projection Around a Semver Storacle Plan

## Objective

> Update our Sheets projection architecture so:
>
> - **projectionist** emits a **standard, semver’d Storacle Plan** (canonical: single JSON object)
> - **storacle** executes that plan (strategy semantics + dispatch + vendor I/O)
> - **lorchestra** orchestrates runs (job lifecycle, events, retries, dry-run)
>
> This spec supersedes the narrower “move the factory” framing. The factory move still happens, but as part of enforcing the new contract boundary.

## Acceptance Criteria

- [ ] A standard, semver’d **Storacle Plan** contract exists:
    - [ ] Canonical format: single JSON object (RFC 8259)
    - [ ] Plan version: `storacle.plan/1.0.0`
    - [ ] No JSONL introduced as part of this spec
- [ ] **Projectionist emits** this contract shape:
    - [ ] For Sheets, the public entrypoint is `build_plan(ctx, config) -> dict` and returns a Storacle Plan
    - [ ] No `apply()` methods exist in projectionist for this work
    - [ ] Projectionist does **not** import `storacle` or `gorch`
    - [ ] Projectionist does **not** execute vendor I/O (no Sheets API calls)
- [ ] **Storacle expects/executes** this contract shape:
    - [ ] A single entrypoint exists (e.g., `storacle.rpc.execute_plan(plan, *, dry_run=False)`) that dispatches ops by JSON-RPC `method` string
    - [ ] Strategy semantics live in storacle execution (`replace` vs `append`, including header handling)
- [ ] Lorchestra orchestrates end-to-end:
    - [ ] Processor builds context, gets plan from projectionist, and sends that plan to storacle via one RPC-style call: `storacle.rpc.execute_plan(plan, dry_run=...)`
    - [ ] Lorchestra does not load any Sheets service factories or adapters for this path
    - [ ] Events emitted: `sheets_projection.started`, `sheets_projection.completed`, `sheets_projection.failed`
    - [ ] Dry-run does not execute vendor I/O
- [ ] Storacle response contract:
    - [ ] `storacle.rpc.execute_plan` returns a list of JSON-RPC 2.0 responses (`{"jsonrpc":"2.0","id":...,"result":...}` or `{"error":...}`)
    - [ ] Lorchestra uses responses only for logging/event status and does not inspect method or result details
- [ ] gorch sheets is client-only:
    - [ ] Only `SheetsClient` remains (remove any gorch-level write service/factory intended for lorchestra)
- [ ] Layer boundary checks:
    - [ ] No direct `import gorch` / `from gorch` in lorchestra source (imports through storacle at runtime are allowed)
    - [ ] projectionist contains no Google API usage and no auth wiring
- [ ] CI green (ruff + pytest across relevant packages)
- [ ] `lorchestra run proj_sheets_proj_clients --dry-run` passes

## Context

### Background

Part 2 of sheets-projector-job created a gorch-level write service + factory to enforce "lorchestra must not import gorch". This worked, but it blurred responsibilities:

- **gorch** should be low-level API wrappers only
- **storacle** should implement write-time semantics and integration wiring
- **lorchestra** should orchestrate and load factories by string, never import vendor clients

More importantly, projectionist was trending toward a “leaky orchestrator” that both *planned* and *executed* I/O.

The updated architecture is:

- projectionist emits a **standard semver Storacle Plan** (contract shape)
- storacle executes the plan (dispatch + semantics)
- lorchestra orchestrates execution (events, retries, dry-run)

### Architecture Doctrine (enforced)

| Layer | Responsibility |
|-------|------|
| **gorch** | Low-level API wrappers (`SheetsClient`) |
| **storacle** | Executes IO plans; write semantics + wiring (factories, replace/append behavior) |
| **projectionist** | Produces IO plans (query + shaping + strategy) |
| **lorchestra** | Job lifecycle; calls plan producer; executes via storacle; emits events |

### Contract Doctrine (standard + semver)

We do **not** describe this IO contract as “owned” by any one library.

- The contract is a **standard semver’d shape** (e.g., `storacle.plan/1.0.0`) used across layers.
- **projectionist emits** a value conforming to the contract.
- **storacle expects** that value and executes it.
- **lorchestra** treats the contract as an artifact to log/audit/retry.

### Constraints

- projectionist emits plans; it does not run vendor I/O
- projectionist must not import `storacle` or `gorch`
- projectionist must not call any `build_*_service` factories
- storacle executes plans; it does not decide *what* to query
- lorchestra orchestrates; it does not import vendor clients
- lorchestra must not inspect `ops[*].method` (treat plan as opaque)
- storacle `SheetsWriteService` stays auth-agnostic (constructor takes client)
- Auth wiring happens only in top-level functions inside storacle (e.g., factories used by `rpc.execute_plan`); lorchestra never calls these directly
- Import `SheetsClient` from `gorch.sheets.client` directly, not `from gorch`
- gorch never imports upward (no storacle/projectionist imports)
- No `SheetsWriteService.from_authctl()` will be introduced

## Plan

### Step 0: Update the spec contract text [S0]

**Role:** prompting_agent

**Prompt:**

Lock in the IO plan shape and versioning described below so implementation cannot drift.

#### IO Plan Shape (recommended)

In-memory representation may be a dict/dataclass/pydantic model, but must serialize to JSON.

```json
{
    "plan_version": "storacle.plan/1.0.0",
    "plan_id": "sha256:abc123...",
    "meta": {
        "source": "projectionist",
        "job_id": "proj_sheets_proj_clients",
        "created_at": "2025-12-22T13:00:00Z"
    },
    "jsonrpc": "2.0",
    "ops": [
        {
            "jsonrpc": "2.0",
            "id": "sheets-1",
            "method": "sheets.write_table",
            "params": {
                "spreadsheet_id": "abc",
                "sheet_name": "clients",
                "values": [["id", "name"], [1, "Alice"]],
                "strategy": "replace"
            }
        }
    ]
}
```

**Notes:**

- `plan_version` is semver’d; changes follow normal semver rules.
- Ops use JSON-RPC 2.0 request semantics (`method` + `params` + `id`).
- `method` is the dispatch key.
- `plan_id` is required for auditability and future idempotence.
- projectionist is responsible for emitting `values` correctly; storacle is responsible for applying `strategy` semantics.

No JSONL is introduced as part of this spec.

### Step 1: Add IO executor to storacle [G1: storacle]

**Role:** agentic

**Prompt:**

Implement an IO executor in storacle:

- A single entrypoint such as `storacle.rpc.execute_plan(plan: dict, *, dry_run: bool = False) -> list[dict]`
- Returns a list of JSON-RPC 2.0 response objects (one per op in `plan.ops`)
- Dispatch by JSON-RPC `method` string
- Implement `sheets.write_table` (uses internal SheetsWriteService)
- Auth wiring happens inside storacle (e.g., factory internally gets account from plan.meta or config)
- Ensure `dry_run=True` validates but does not call vendor APIs

This executor should work with the standard semver contract described in Step 0.

**Allowed Paths:**

- /workspace/storacle/src/storacle/**
- /workspace/storacle/tests/**

**Verification Commands:**

```bash
cd /workspace/storacle && ruff check src/
cd /workspace/storacle && pytest tests/ -v -k "io or sheets"
```

### Step 2: Implement Sheets write semantics in storacle [G2: storacle]

**Role:** agentic

**Prompt:**

Update `/workspace/storacle/src/storacle/clients/sheets.py`:

1. Ensure vendor client import is direct:
```python
from gorch.sheets.client import SheetsClient  # Direct import, not from gorch
```

2. `SheetsWriteService` implements write-time semantics:

- `strategy="replace"` → clear then write (idempotent)
- `strategy="append"` → append only data rows (skip header)

3. Auth wiring (internal to storacle):

- How storacle gets an authenticated SheetsWriteService is an implementation detail (factory, DI, config)
- One approach: a factory like `build_sheets_write_service(account)` that calls `SheetsClient.from_authctl(account)`
- This is internal to storacle; lorchestra does not call it

Add tests in `/workspace/storacle/tests/`:

- `write_table(..., strategy="append")` skips header
- `write_table(..., strategy="replace")` is idempotent

**Allowed Paths:**

- /workspace/storacle/src/storacle/clients/sheets.py
- /workspace/storacle/tests/**

**Verification Commands:**

```bash
cd /workspace/storacle && ruff check src/
cd /workspace/storacle && pytest tests/ -v -k sheets
```

### Step 3: Update projectionist to emit IO plan [G3: projectionist]

**Role:** agentic

**Prompt:**

Refactor projectionist so that Sheets projection is a *plan producer*:

- Public entrypoint: `build_plan(ctx, config) -> dict` returns a `storacle.plan/1.0.0` plan (JSON object)
- The plan contains ops with `jsonrpc: "2.0"`, `method: "sheets.write_table"`, and `params` including `values` + `strategy`
- Projectionist does not execute I/O; no Sheets API calls
- Do not add or keep `apply()` methods in projectionist
- Projectionist must not import `storacle` or `gorch`

**Test requirements:**

- Assert `build_plan(...)` returns a dict matching the plan schema (plan_version, jsonrpc, ops)
- Assert ops[0].method == "sheets.write_table" and params contain expected values/strategy
- No factory imports, no IO mocking

**Allowed Paths:**

- /workspace/projectionist/**

**Verification Commands:**

```bash
cd /workspace/projectionist && ruff check .
cd /workspace/projectionist && pytest -v -k sheets
```

### Step 4: Update lorchestra to orchestrate via RPC-only [G4: lorchestra]

**Role:** agentic

**Prompt:**

Update the Sheets projection processor to follow the new RPC-only flow:

1. **Processor flow:**

```python
# Pseudocode
plan = projectionist.build_plan(ctx, config)
emit_event("sheets_projection.started", plan_id=plan["plan_id"])

try:
    responses = storacle.rpc.execute_plan(plan, dry_run=ctx.dry_run)
    emit_event("sheets_projection.completed", plan_id=plan["plan_id"], responses=responses)
except Exception as e:
    emit_event("sheets_projection.failed", plan_id=plan["plan_id"], error=str(e))
```

2. **Remove factory loading for Sheets path:**

- Remove `sheets_service_factory` and `sheets_service_factory_args` from `/workspace/lorchestra/lorchestra/jobs/definitions/projection/sync/proj_sheets_proj_clients.json`
- Processor does not call `load_service_factory` for this job

3. **Hard constraints:**

- Lorchestra must not branch on `method == "sheets.write_table"` (treat plan as opaque)
- Lorchestra must not call `SheetsWriteService` or any Sheets adapter directly
- Lorchestra may import `storacle.rpc.execute_plan` only

**Test requirements:**

Update `/workspace/lorchestra/tests/test_sheets_projection_processor.py`:

- Patch `storacle.rpc.execute_plan` (not factory loaders)
- Assert it gets called once with the plan dict returned by projectionist and `dry_run` flag
- Assert events emitted based on mocked RPC responses:
  - `sheets_projection.started` with plan_id
  - `sheets_projection.completed` with plan_id + responses (success case)
  - `sheets_projection.failed` with plan_id + error (failure case)

Note: `load_service_factory` and allowlist tests remain for other jobs that still use factory loading; they are not removed, just not used for the Sheets projection path

**Allowed Paths:**

- /workspace/lorchestra/lorchestra/processors/projectionist.py (or wherever the Sheets processor lives)
- /workspace/lorchestra/lorchestra/jobs/definitions/projection/sync/proj_sheets_proj_clients.json
- /workspace/lorchestra/tests/test_sheets_projection_processor.py

**Note:** No changes required in `job_runner.py` or its tests for this spec; `load_service_factory` and allowlist remain for other jobs that still use factory loading.

**Verification Commands:**

```bash
cd /workspace/lorchestra && ruff check lorchestra/
cd /workspace/lorchestra && pytest tests/test_job_runner.py tests/test_sheets_projection_processor.py -v
```

### Step 5: Clean up gorch [G5: gorch]

**Role:** agentic

**Prompt:**

Delete from gorch (now unused):
- `/workspace/gorch/src/gorch/sheets/services.py`
- `/workspace/gorch/src/gorch/sheets/factories.py`
- `/workspace/gorch/tests/test_sheets_services.py`
- `/workspace/gorch/tests/test_sheets_factories.py`

Update `/workspace/gorch/src/gorch/sheets/__init__.py`:
```python
"""Google Sheets client module."""

from gorch.sheets.client import SheetsClient

__all__ = ["SheetsClient"]
```
(Remove `GorchSheetsWriteService` export)

Reinstall gorch in lorchestra's venv to pick up deleted modules:
```bash
cd /workspace/lorchestra && uv pip install -e /workspace/gorch
```

**Verification Commands:**

```bash
cd /workspace/gorch && ruff check src/
cd /workspace/gorch && pytest tests/test_sheets_client.py -v
# Broaden grep to catch JSON and markdown references too
grep -rn "gorch\.sheets\.services\|gorch\.sheets\.factories" /workspace --include="*.py" --include="*.json" --include="*.md" | grep -v __pycache__ && exit 1 || echo "Clean"
```

### Step 6: Integration test [G6: Integration]

**Role:** coding_agent

**Prompt:**

Run full verification:

```bash
# Ruff on all packages
cd /workspace/gorch && ruff check src/
cd /workspace/storacle && ruff check src/
cd /workspace/lorchestra && ruff check lorchestra/

# All tests
cd /workspace/gorch && pytest tests/ -v
cd /workspace/storacle && pytest tests/ -v
cd /workspace/lorchestra && pytest tests/ -v

# No direct gorch imports in lorchestra SOURCE (runtime imports through storacle are allowed)
grep -rn "from gorch\|import gorch" /workspace/lorchestra/lorchestra/ --include="*.py" | grep -v __pycache__ && exit 1 || echo "Clean: no direct gorch imports"

# Verify old factory paths are gone everywhere
grep -rn "gorch\.sheets\.factories\|gorch\.sheets\.services" /workspace --include="*.py" --include="*.json" --include="*.md" | grep -v __pycache__ && exit 1 || echo "Clean: old paths removed"

# Dry-run
cd /workspace/lorchestra && lorchestra run proj_sheets_proj_clients --dry-run
```

## Design Notes (Non-goals)

- **Scope:** This spec only wires the **Sheets projection** to the new Storacle Plan pattern. Other sinks (BQ, WAL, etc.) and other projections will adopt the same plan shape in future specs.
- This spec does not require WAL/events as IO ops yet; it only ensures the IO plan + executor architecture can support them later.
- This spec does not require JSONL persistence; JSONL is an optional serialization target for logging/audit.

## Models & Tools

**Tools:** bash, pytest, ruff, uv

## Repository

**Branch:** `feat/storacle-and-sheets-v3`

**Merge Strategy:** squash