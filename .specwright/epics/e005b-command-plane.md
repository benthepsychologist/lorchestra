---
version: "0.1"
tier: A
title: "e005b — Command-Plane Refactoring"
owner: benthepsychologist
goal: "Transform lorchestra from a processor-based job runner into a pure orchestration layer with clean handler dispatch"
labels: [architecture, refactor, epic]
project_slug: lorchestra
created: 2025-01-10
updated: 2026-02-02
---

# e005b — Command-Plane Refactoring

## Objective

Refactor lorchestra into a pure orchestration layer where:

- **JobDef** declares what to do (YAML, versioned, no code)
- **Executor** compiles, dispatches, and records runs
- **Handlers** route ops to backends (callable, compute, orchestration)
- **Callables** do the work (injest, canonizer, finalform, projectionist, workman, etc.)
- **Storacle** handles all data-plane I/O behind a JSON-RPC boundary

The epic eliminates direct processor imports, collapses the handler
abstraction for native ops, migrates all job definitions to v2.0 YAML,
and adds pipeline orchestration for sequential multi-job execution.

## Architecture After e005b

```
CLI
 |
 v
execute(envelope)           <-- public API
 |
 v
Executor
 |-- native ops (call, plan.build, storacle.submit)  --> handled directly
 |-- compute ops (compute.*)                          --> ComputeHandler
 |-- orchestration ops (job.*)                        --> OrchestrationHandler
 |
 v
CallableRegistry            <-- dispatch_callable(name, params)
 |-- injest, canonizer, finalform, projectionist, workman
 |-- bq_reader, file_renderer, view_creator
 |-- molt_projector, measurement_projector, observation_projector
```

## Spec Index

### Phase 1: Schema & Executor Foundation

| Spec | Title | Gate | Status |
|------|-------|------|--------|
| e005b-01 | Job schema model (JobDef, JobInstance, RunRecord, StepManifest) | — | Done |
| e005b-02 | Executor implementation + JobRegistry + RunStore | — | Done |

### Phase 2: Boundaries & E2E Validation

| Spec | Title | Gate | Status |
|------|-------|------|--------|
| e005b-03a | Storacle client protocol + JSON-RPC boundary | gate03a | Done |
| e005b-03b | Handler registry (callable, compute, orchestration) | gate03b | Done |
| e005b-03c | E2E tests + JSON-RPC envelope boundary for storacle | gate03c | Done |
| e005b-03d | Smoke namespace CLI + event_client routing | gate03d | Done |

### Phase 3: CLI Rewire & Job Migration

| Spec | Title | Gate | Status |
|------|-------|------|--------|
| e005b-04a | Rewire CLI to execute(envelope) public API | gate04a | Done |
| e005b-04b | Migrate ingest jobs to call.injest YAML defs | gate04b | Done |
| e005b-04c | Migrate canonize jobs to call.canonizer YAML defs | gate04c | Done |
| e005b-04d | Migrate final-form jobs to call.finalform YAML defs | gate04d | Done |
| e005b-04e | Migrate projectionist jobs, retire direct storacle import | gate04e | Done |
| e005b-04f | Validate workman callable chain + idempotency keys | gate04f | Done |

### Phase 4: Handler Collapse & New Callables

| Spec | Title | Gate | Status |
|------|-------|------|--------|
| e005b-05 | Collapse handler abstraction for native ops | gate05a-f | Done |

Collapsed call.* ops into generic `call` op with `params.callable`.
Added native ops `plan.build` and `storacle.submit` handled directly
by Executor. Deleted CallableHandler. Added 6 new callables
(bq_reader, file_renderer, view_creator, molt_projector,
measurement_projector, observation_projector). Migrated all job
definitions to v2.0 three-step YAML pattern. Added projection job
definitions for file, molt, table, view, and sync targets.

### Phase 5: Pipeline Orchestration

| Spec | Title | Gate | Status |
|------|-------|------|--------|
| e005b-06 | Pipeline loop for sequential multi-job execution | pipeline_v2 | Done |

Pipeline runner (`lorchestra/pipeline.py`) with `run_pipeline()`,
`load_pipeline()`, recursive sub-pipeline support, `stop_on_failure`,
and `PipelineResult`. Six pipeline YAML definitions (ingest, canonize,
formation, project, views, daily_all). CLI integration via
`lorchestra exec pipeline <pipeline_id>`.

### Phase 6: Performance (planned)

| Spec | Title | Gate | Status |
|------|-------|------|--------|
| e005b-07a | TBD — performance / batching | — | Planned |
| e005b-07b | TBD — performance / parallelism | — | Planned |

## Key Commits

| Commit | Spec | Message |
|--------|------|---------|
| `6b5dcae` | e005b-01 | feat(schemas): implement JobDef -> JobInstance -> RunRecord model |
| `f90352e` | e005b-02 | feat(executor): implement e005-02 executor |
| `688942c` | e005b-03c | e2e tests + JSON-RPC envelope boundary for storacle |
| `eb792d3` | e005b-03d | smoke namespace CLI + event_client routing |
| `ceaf8a1` | e005b-04a | rewire CLI to execute(envelope) public API |
| `d16e4ef` | e005b-04b | migrate ingest jobs to call.injest YAML defs |
| `726fc8a` | e005b-04c | migrate canonize jobs to call.canonizer YAML defs |
| `7d08590` | e005b-04d | migrate final-form jobs to call.finalform YAML defs |
| `214b659` | e005b-04e | migrate projectionist jobs, retire direct storacle import |
| `b1f2f05` | e005b-04f | validate workman callable chain (checkpoint) |
| `bcc0e22` | e005b-05 + e005b-06 | collapse handler abstraction for native ops |

## Test Coverage

| Gate | Test File(s) | Tests |
|------|-------------|-------|
| gate03c | `tests/e2e/test_gate03c_*.py` (5 files) | ~70 |
| gate03d | `tests/e2e/test_gate03d_smoke_datasets.py` | 22 |
| gate04f | `tests/e2e/test_gate04f_workman_callable_chain.py` | 4 |
| gate05a | `tests/e2e/test_gate05a_storacle_methods.py` | ~20 |
| gate05b | `tests/e2e/test_gate05b_ddl_callables.py` | ~25 |
| gate05c | `tests/e2e/test_gate05c_bq_reader_sqlite.py` | ~20 |
| gate05d | `tests/e2e/test_gate05d_file_renderer.py` | ~20 |
| gate05e | `tests/e2e/test_gate05e_measurement_projector.py` | ~25 |
| gate05f | `tests/e2e/test_gate05f_observation_projector.py` | ~30 |
| pipeline_v2 | `tests/e2e/test_pipeline_v2.py` | 23 |
| equivalence | `tests/e2e/test_equivalence.py` | ~50 |
| schemas | `tests/schemas/test_ops.py`, `tests/test_schemas.py` | ~80 |
| executor | `tests/test_executor.py` | ~50 |
| handlers | `tests/test_handlers.py` | ~15 |
| dispatch | `tests/callable/test_dispatch.py` | ~15 |

**Total: 814 passing, 27 skipped**

## Op Taxonomy (after e005b-05)

| Op | Backend | Handler |
|----|---------|---------|
| `call` | callable | Executor (native) |
| `plan.build` | native | Executor (native) |
| `storacle.submit` | native | Executor (native) |
| `compute.llm` | inferometer | ComputeHandler |
| `job.run` | orchestration | OrchestrationHandler |

## Deprecations

- `backends` parameter on `Executor.__init__()` — use `handlers` with `HandlerRegistry` instead (emits `DeprecationWarning`)
- `CallableHandler` — deleted in e005b-05; native ops handled by Executor directly
- Specific `call.*` op variants (call.injest, call.canonizer, etc.) — replaced by generic `call` op with `params.callable`

## Files Added/Deleted

**Deleted:**
- `lorchestra/handlers/callable_handler.py`

**Added (key files):**
- `lorchestra/pipeline.py`
- `lorchestra/plan_builder.py`
- `lorchestra/callable/bq_reader.py`
- `lorchestra/callable/file_renderer.py`
- `lorchestra/callable/view_creator.py`
- `lorchestra/callable/molt_projector.py`
- `lorchestra/callable/measurement_projector.py`
- `lorchestra/callable/observation_projector.py`
- 6 pipeline definitions in `jobs/definitions/pipeline/`
- 30+ new projection job definitions (file, molt, table, view, sync)
