---
id: e011-02-lorchestra-pm-jobs
title: "Lorchestra PM Orchestration — Plan, Apply, Exec Jobs"
tier: B
owner: benthepsychologist
goal: "lorchestra/jobs/definitions/pm/pm.plan.yaml with single step: op=call, callable=compile_intent, intent='@payload.intent'"
branch: feat/pm-orchestration
repo:
  name: lorchestra
  url: /workspace/lorchestra
status: refined
created: 2026-02-11T21:24:25Z
updated: 2026-02-11T22:15:00Z
---

# e011-02-lorchestra-pm-jobs: Lorchestra PM Orchestration — Plan, Apply, Exec Jobs

**Epic:** e011-pm-system
**Branch:** `feat/pm-orchestration`
**Tier:** B

## Schema Reference

**Depends on**: e011-01-workman-pm-domain (all PM ops and field registry)

**Schema reference**: See [../resources.md](../resources.md) §3 (PMIntent envelope schema) for the intent structure passed to these jobs. The CallableResult output from `compile_intent()` is defined in resources.md §7.6 (Workman API Reference).

## Objective

Implement PM orchestration jobs in lorchestra to enable plan-diff-apply workflow for PM operations. Creates three job definitions (`pm.plan`, `pm.apply`, `pm.exec`) that invoke workman's `compile_intent` function via the call operation (using dispatch_callable) to provide preview-before-write discipline for all PM mutations.

This establishes the orchestration layer between the life CLI (ergonomic commands) and workman (domain logic), ensuring all PM writes go through lorchestra's job execution pipeline with proper audit trails, result capture, and integration with storacle's WAL.

## Problem

1. **No orchestration layer for PM operations**: PM writes currently bypass lorchestra, going directly from CLI to storacle, missing audit trails and execution context
2. **No plan-diff-apply discipline**: Users cannot preview PM changes before applying them, leading to accidental mutations
3. **Missing integration with workman's compile_intent**: The new PMIntent compilation capability needs orchestration jobs to be consumable
4. **No separation of planning from execution**: Interactive and non-interactive workflows both need the same planning step, but different apply strategies

## Current Capabilities

### kernel.surfaces

```yaml
- command: "lorchestra run"
  usage: "lorchestra run gmail_ingest_acct1 --dry-run"
- command: "lorchestra list" 
  usage: "lorchestra list --type ingest"
- command: "lorchestra query"
  usage: "lorchestra query recent-jobs --limit 20"
- command: "run_job()"
  usage: "run_job('gmail_ingest_acct1', dry_run=True)"
```

### modules

```yaml
- name: public_surface
  provides: ['LorchestraConfig', 'load_config()', 'get_lorchestra_home()']
- name: cli
  provides: ['lorchestra run', 'lorchestra init', 'lorchestra jobs list', 'lorchestra jobs show', 'lorchestra stats canonical', 'lorchestra stats raw', 'lorchestra stats jobs', 'lorchestra query', 'lorchestra sql']
- name: config
  provides: ['LorchestraConfig dataclass', 'load_config()', 'get_lorchestra_home()', 'get_config_path()']
- name: job_runner
  provides: ['run_job(job_id, **options)', 'BigQueryStorageClient', 'load_job_spec(job_id)']
- name: compiler
  provides: ['Job spec compilation']
- name: executor
  provides: ['Job execution engine']
- name: plan_builder
  provides: ['Plan building for job steps']
- name: registry
  provides: ['Service registry']
- name: run_store
  provides: ['Run storage and history']
- name: errors
  provides: ['LorchestraError hierarchy']
- name: callable
  provides: ['CallableDispatch', 'CallableResult']
- name: handlers
  provides: ['BaseHandler', 'ComputeHandler', 'OrchestrationHandler', 'StoracleClientHandler', 'HandlerRegistry']
```

### layout

```yaml
- path: lorchestra/__init__.py
  role: "Public re-exports: LorchestraConfig, load_config, get_lorchestra_home"
- path: lorchestra/cli.py
  role: "Click CLI: run, list, stats, query, sql subcommands"
- path: lorchestra/config.py
  role: "LorchestraConfig dataclass, load_config(), LORCHESTRA_HOME"
- path: lorchestra/job_runner.py
  role: "run_job() dispatcher, BigQueryStorageClient"
- path: lorchestra/compiler.py
  role: "Job spec compilation logic"
- path: lorchestra/executor.py
  role: "Job execution engine"
- path: lorchestra/plan_builder.py
  role: "Plan building logic for job steps"
- path: lorchestra/registry.py
  role: "Service registry"
- path: lorchestra/run_store.py
  role: "Run storage and history"
- path: lorchestra/errors.py
  role: "Error definitions"
- path: lorchestra/callable/
  role: "Callable dispatch and result types for external function invocation"
- path: lorchestra/handlers/
  role: "Job step handlers: callable_handler, compute, orchestration, storacle_client"
- path: lorchestra/jobs/definitions/
  role: "JSON/YAML job specs organized by processor type"
```

## Proposed build_delta

```yaml
target: "projects/lorchestra/lorchestra.build.yaml"
summary: "Add PM job definitions (plan, apply, exec). No new code — uses existing call op with dispatch_callable."

adds:
  layout:
    - path: "lorchestra/jobs/definitions/pm/pm.plan.yaml"
      role: "PM plan job: calls compile_intent, returns preview without applying"
    - path: "lorchestra/jobs/definitions/pm/pm.apply.yaml"  
      role: "PM apply job: submits pre-compiled plans via storacle"
    - path: "lorchestra/jobs/definitions/pm/pm.exec.yaml"
      role: "PM exec job: combines plan + apply for non-interactive flow"
  kernel_surfaces: []
  modules: []
modifies:
  modules:
    - name: job_definitions
      change: "Add pm/pm.plan.yaml, pm/pm.apply.yaml, pm/pm.exec.yaml to provides list"
removes: {}
```

## Acceptance Criteria

- [ ] lorchestra/jobs/definitions/pm/pm.plan.yaml with single step: op=call, callable=compile_intent, intent='@payload.intent'
- [ ] pm.plan returns CallableResult with plans, diff, plan_hash as step outcome items
- [ ] lorchestra/jobs/definitions/pm/pm.apply.yaml with steps: extract plans from @payload.plans, submit via storacle.submit
- [ ] lorchestra/jobs/definitions/pm/pm.exec.yaml with two steps: (1) plan using compile_intent, (2) apply using storacle.submit
- [ ] All three jobs follow YAML v2.0 format matching existing workman_create_project.yaml pattern
- [ ] End-to-end: lorchestra.execute({job_id: 'pm.plan', payload: {intent: ...}}) returns plans + diff without writing
- [ ] End-to-end: lorchestra.execute({job_id: 'pm.exec', payload: {intent: ...}}) writes to WAL

## Constraints

- Job definitions use YAML v2.0 format (job_id, version: '2.0', steps[] with step_id/op/params)
- Follow existing pattern from workman_create_project.yaml: call → plan.build → storacle.submit
- pm.plan calls workman.intent.compile_intent via call op, returns plans + diff without submitting
- pm.apply takes compiled plans and submits them via storacle.submit
- pm.exec combines plan + apply in a single two-step job for non-interactive flow
- No new handler types needed — existing call op with dispatch_callable is sufficient
- The plan step MUST be separate from the apply step to allow human review between them
- No changes to existing job definitions or handlers

---

## Architectural Notes

### Callable Dispatch Pattern

Lorchestra's `call` operation is a native op (like `plan.build` and `storacle.submit`) handled directly by the executor. It does NOT use the HandlerRegistry.

**How call op works:**
1. Executor receives step with `op: call` and params including `callable: <name>`
2. Executor strips `callable` from params, forwards everything else as a dict
3. Executor calls `dispatch_callable(name, params)` from `lorchestra.callable.dispatch`
4. `dispatch_callable` looks up callable in registry (workman, canonizer, finalform, etc.)
5. Invokes `callable.execute(params)` with forwarded params dict
6. Returns `CallableResult` with items/items_ref/stats (and any additional fields)

**Params Forwarding Pattern:**

All step fields (except `callable` itself) are forwarded to the callable:

```yaml
# Job step definition:
- step_id: plan.compile
  op: call
  params:
    callable: workman         # Stripped by executor, used for dispatch
    op: pm.compile_intent     # Forwarded to workman
    intent: "@payload.intent" # Forwarded to workman
    ctx: {...}                # Forwarded to workman
```

**For workman callable:**

Workman expects params dict with:
- `op`: Operation name (e.g., "pm.compile_intent")
- `intent`: PMIntent structure (required for compile_intent op)
- `ctx`: Context dict with correlation_id, producer (optional, constructed if missing)

Example invocation:
```python
workman.execute({
    "op": "pm.compile_intent",
    "intent": {"intent_id": "pmi_...", "ops": [...]},
    "ctx": {"correlation_id": "...", "producer": "..."}
})
```

**CallableResult Return:**

compile_intent returns CallableResult with:
- `items`: Array of plans (one per operation in intent)
- `diff`: Human-readable change descriptions (array of strings)
- `plan_hash`: SHA256 hash for integrity verification

Downstream steps reference outputs via `@run.step_id.*`:
- `@run.plan.compile.items` → plans array
- `@run.plan.compile.diff` → diff strings
- `@run.plan.compile.plan_hash` → hash value

**Contrast with Handler Pattern:**

This is different from handlers (compute.*, job.* ops) which are registered and invoked differently.
Call ops use the native dispatch_callable mechanism, not the HandlerRegistry.

---

## Error Handling Strategy

### Failure Modes

**compile_intent failures**:
- Invalid PMIntent structure → CompileError
- Unknown operation in ops array → CompileError
- Payload validation failure → ValidationError
- Invalid @ref reference → CompileError

**Plan building failures**:
- FK assertion failure (referenced entity not found) → PlanBuildError
- Idempotency key conflict → ConflictError (not implemented in e011)

**Storacle write failures**:
- WAL append failure → StoracleError
- BigQuery transient error → RetryableError

### Error Propagation

```
compile_intent error
  └─> Step fails with status="failed", error=message
      └─> RunRecord.outcomes includes failed step
          └─> lorchestra.execute() returns success=False
              └─> Caller receives error details in result
```

**No rollback**: WAL is append-only. Failed steps do not undo previous writes.

**No conflict detection** (e011 scope): Same entity modified twice in intent → last-write-wins at WAL level. Conflict detection deferred to future spec.

### Step-Level Error Control

Lorchestra steps support `continue_on_error`:

```yaml
steps:
  - step_id: plan.compile
    op: call
    params: {...}
    continue_on_error: false  # Default: abort on failure

  - step_id: plans.submit
    op: storacle.submit
    params: {...}
    continue_on_error: false
```

**e011 default**: All PM job steps use `continue_on_error: false` (fail-fast).

### Retry Logic

**Not in lorchestra scope** (e011):
- Retries handled by backend services (storacle, egret, inferometer)
- Lorchestra does not retry failed steps
- Caller can retry entire job

**Future consideration** (e011-05+): Retry policy at job level.

### Error Messages

**User-facing errors** (from life CLI):
```
CompileError: "Invalid operation 'pm.task.create' in intent - did you mean 'pm.work_item.create'?"
ValidationError: "Field 'priority' in pm.work_item.create: expected enum value, got 'EXTREME'"
PlanBuildError: "Referenced project 'proj_01XYZ' does not exist (assertion failed)"
StoracleError: "Failed to write to WAL: BigQuery unavailable (transient)"
```

**Developer-facing errors** (from lorchestra.execute):
```python
{
    "success": False,
    "run_id": "run_01ABC",
    "outcomes": [
        {
            "step_id": "plan.compile",
            "status": "failed",
            "error": "CompileError: Invalid @ref:5 in op 1 - only 3 ops available"
        }
    ]
}
```

---

## PM Plan Job Definition

### Objective
Create the core planning job that calls workman's compile_intent to generate plans and diffs without applying changes. This enables the preview-before-write workflow.

### Files to Touch
- `lorchestra/jobs/definitions/pm/pm.plan.yaml` (create) — Single-step job calling compile_intent

### Implementation Notes
Follow the YAML v2.0 job definition pattern from existing specs like aip-1.yaml:

```yaml
job_id: pm.plan
version: "2.0"
description: "Plan PM intent without applying changes"

steps:
  - step_id: plan.compile
    op: call
    params:
      callable: workman
      op: pm.compile_intent
      intent: "@payload.intent"
```

The call op will dispatch to workman.intent.compile_intent via dispatch_callable(), which returns a CallableResult containing:
- `plans`: List of StoraclePlan objects for each operation
- `diff`: Human-readable change descriptions
- `plan_hash`: SHA256 hash for integrity verification

### Verification
- `lorchestra jobs list` → shows pm.plan job
- `lorchestra run pm.plan --payload '{"intent": {...}}'` → returns plans without writing to WAL
- Job execution produces CallableResult with schema_version, items, stats, plus plans/diff/plan_hash fields

## PM Apply and Exec Jobs

### Objective
Complete the workflow with apply (submit pre-compiled plans) and exec (plan+apply combined) jobs. Apply handles the write phase, exec provides single-command convenience for non-interactive use.

### Files to Touch
- `lorchestra/jobs/definitions/pm/pm.apply.yaml` (create) — Multi-step job extracting and submitting plans
- `lorchestra/jobs/definitions/pm/pm.exec.yaml` (create) — Two-step job combining plan and apply

### Implementation Notes

**pm.apply.yaml structure:**
```yaml
job_id: pm.apply
version: "2.0"
description: "Apply pre-compiled PM plans to WAL"

steps:
  - step_id: plans.submit
    op: storacle.submit
    params:
      plan: "@payload.plans"
```

**pm.exec.yaml structure:**
```yaml
job_id: pm.exec
version: "2.0"
description: "Plan and apply PM intent in single execution"

steps:
  - step_id: plan.compile
    op: call
    params:
      callable: workman
      op: pm.compile_intent
      intent: "@payload.intent"

  - step_id: plans.submit
    op: storacle.submit
    params:
      plan: "@run.plan.compile.items"
```

### Verification
- `lorchestra jobs list` → shows all three PM jobs (plan, apply, exec)
- `lorchestra run pm.plan --payload '{"intent": {...}}'` → returns result.items (plans array), result.diff, result.plan_hash
- `lorchestra run pm.apply --payload '{"plans": [...]}'` → writes to WAL without plan_hash validation
- `lorchestra run pm.exec --payload '{"intent": {...}}'` → end-to-end plan+apply
- WAL contains expected domain events after apply/exec operations
- `ruff check lorchestra/` → clean (no new Python code)

## lorchestra.build.yaml Diff (Post-e011)

The lorchestra.build.yaml file will be updated to include the new PM job definitions:

```yaml
# UPDATES to layout (job_definitions module):
layout:
  # Existing job definition paths...
  - path: "jobs/definitions/pm/"
    module: job_definitions
    role: "PM orchestration jobs (plan, apply, exec) wrapping workman compile_intent"

# UPDATES to modules:
modules:
  - name: job_definitions
    kind: module
    provides:
      # Existing jobs...
      - "workman_create_project.yaml"
      # ... other existing jobs ...
      # NEW in e011
      - "pm/pm.plan.yaml"
      - "pm/pm.apply.yaml"
      - "pm/pm.exec.yaml"

# UPDATES to boundaries:
boundaries:
  - name: job_execution
    type: inbound
    contract: "execute(job_id, payload) -> result with items/diff/plan_hash/success"
    consumers:
      - "life (via lorchestra CLI)"
      - "tests"
      - "direct invocations"
    # NEW in e011: PM jobs callable contract
    # pm.plan: PMIntent → (plans, diff, plan_hash)
    # pm.apply: plans array → (success/failure status)
    # pm.exec: PMIntent → (plans, diff, plan_hash, success/failure)
```

**Job Files to Create**:
- `lorchestra/jobs/definitions/pm/pm.plan.yaml`
- `lorchestra/jobs/definitions/pm/pm.apply.yaml`
- `lorchestra/jobs/definitions/pm/pm.exec.yaml`

**No Python code changes** — all three jobs use YAML v2.0 with existing op handlers (call, storacle.submit).