# Task: e005-01-schemas

**Epic:** e005-command-plane
**Branch:** feat/job-schemas

## Goal

Transform lorchestra into a pure orchestration layer with JobDef → JobInstance → RunRecord → StepManifest model, enforcing strict boundaries: lorchestra orchestrates, storacle handles data-plane, compute handles external IO.

## Expectations

- lorchestra/schemas/job_def.py defines JobDef dataclass
- lorchestra/schemas/job_instance.py defines JobInstance dataclass
- lorchestra/schemas/run_record.py defines RunRecord dataclass
- lorchestra/schemas/step_manifest.py (OpManifest) defines StepManifest dataclass
- lorchestra/schemas/attempt.py defines AttemptRecord and StepOutcome dataclasses
- JobDef has: job_id, version, steps (step_id, op, params with @ctx.* and @payload.* refs, optional @run.* refs, optional phase_id, optional timeout_s, optional continue_on_error, optional if, optional idempotency)
- step.if is compile-time only: must be decidable using @ctx.* and @payload.* (no @run.*); otherwise CompileError
- idempotency is typed (write ops only): {scope: run|semantic|explicit, semantic_key_ref?: str, include_payload_hash?: bool}; semantic requires semantic_key_ref; include_payload_hash default false
- Schema invariants: phase_id has no execution semantics in v0 (no ordering/dispatch/retry policy). idempotency MUST be absent/null for non-write ops; for write.* ops, idempotency defaults to {scope: run} if omitted
- JobInstance has: job_id, job_version, job_def_sha256, compiled_at, steps (step_id, op, params, phase_id?, timeout_s?, continue_on_error?, compiled_skip) — fixed step list, no job.run remaining, no loops
- JobStepInstance.compiled_skip: bool (default false); structural compile sets it when step.if evaluates to false
- RunRecord has: run_id (ULID), job_id, job_def_sha256, envelope, started_at
- StepManifest has: run_id, step_id, backend, op, resolved_params, prompt_hash (nullable), idempotency_key
- StepManifest persisted form is replay-safe and dispatchable: backend derived from op prefix (query/write/assert => data_plane, compute => compute)
- AttemptRecord has: run_id, attempt_n, started_at, completed_at, status, step_outcomes[]
- StepOutcome has: step_id, status (pending|running|completed|failed|skipped), started_at, completed_at, manifest_ref, output_ref, error
- JSON Schema files in lorchestra/schemas/*.schema.json for validation
- Op enum defines taxonomy: query.*, write.*, assert.*, compute.*, job.run
