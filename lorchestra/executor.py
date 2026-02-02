"""
Executor - Step dispatch and execution engine.

The Executor implements:
- Native op handling (call, plan.build, storacle.submit) directly
- Handler dispatch for compute.* and job.* ops via HandlerRegistry
- Reference resolution (@run.* refs from previous step outputs)
- Idempotency key computation
- Retry logic with continue_on_error semantics
- Attempt tracking

Execution flow:
1. Create RunRecord when execution starts
2. Create AttemptRecord for each execution attempt
3. For each step:
   a. Resolve @run.* references from previous outputs
   b. Compute idempotency key
   c. Create StepManifest
   d. Dispatch: native ops handled inline, others via HandlerRegistry
   e. Record StepOutcome (output available via @run.step_id.*)
4. Update AttemptRecord with final status

Native ops (e005b-05):
- call: dispatch to callable by name, surface CallableResult as step output
- plan.build: build StoraclePlan from items + method
- storacle.submit: submit plan to storacle boundary

Handler-dispatched ops:
- ComputeHandler: compute.llm (via inferometer service)
- OrchestrationHandler: job.* (via lorchestra itself)
"""

import hashlib
import json
import re
import warnings
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

from lorchestra.schemas import (
    JobDef,
    JobInstance,
    JobStepInstance,
    RunRecord,
    StepManifest,
    AttemptRecord,
    StepOutcome,
    StepStatus,
    IdempotencyConfig,
)

from .registry import JobRegistry
from .compiler import compile_job
from .run_store import RunStore, InMemoryRunStore, FileRunStore, DEFAULT_RUN_PATH

if TYPE_CHECKING:
    from lorchestra.handlers import HandlerRegistry


# Reference pattern for @run.* references
RUN_REF_PATTERN = re.compile(r"@run\.([a-zA-Z_][a-zA-Z0-9_.]*)")


def _utcnow() -> datetime:
    """Return current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)


def _resolve_run_refs(
    value: Any,
    step_outputs: dict[str, Any],
) -> Any:
    """
    Resolve @run.* references in a value using previous step outputs.

    @run.step_id.path.to.value resolves to step_outputs["step_id"]["path"]["to"]["value"]

    Args:
        value: The value containing potential @run.* references
        step_outputs: Dictionary mapping step_id to step output

    Returns:
        The resolved value

    Raises:
        ValueError: If a reference cannot be resolved
    """
    if isinstance(value, str):
        if value.startswith("@run."):
            # Full reference resolution
            match = RUN_REF_PATTERN.match(value)
            if match:
                path = match.group(1)
                parts = path.split(".")
                step_id = parts[0]

                if step_id not in step_outputs:
                    raise ValueError(f"@run reference to unknown step: {step_id}")

                result = step_outputs[step_id]
                for part in parts[1:]:
                    if isinstance(result, dict) and part in result:
                        result = result[part]
                    else:
                        raise ValueError(
                            f"@run reference path not found: {value} (missing '{part}')"
                        )
                return result
        return value
    elif isinstance(value, dict):
        return {k: _resolve_run_refs(v, step_outputs) for k, v in value.items()}
    elif isinstance(value, (list, tuple)):
        return [_resolve_run_refs(v, step_outputs) for v in value]
    else:
        return value


def _compute_idempotency_key(
    run_id: str,
    step_id: str,
    step: JobStepInstance,
    resolved_params: dict[str, Any],
    idempotency: Optional[IdempotencyConfig],
) -> str:
    """
    Compute the idempotency key for a step execution.

    Key format depends on idempotency scope:
    - run: "{run_id}:{step_id}"
    - semantic: "{job_id}:{semantic_key_value}"
    - explicit: Uses explicit key from params

    If include_payload_hash is True, adds hash of resolved params.

    Args:
        run_id: The run ULID
        step_id: The step identifier
        step: The step instance
        resolved_params: Fully resolved parameters
        idempotency: Idempotency configuration (None for non-write ops)

    Returns:
        The computed idempotency key
    """
    # Default to run scope if not specified
    if idempotency is None:
        idempotency = IdempotencyConfig(scope="run")

    if idempotency.scope == "run":
        base_key = f"{run_id}:{step_id}"
    elif idempotency.scope == "semantic":
        # Extract semantic key from resolved params
        semantic_ref = idempotency.semantic_key_ref
        if semantic_ref:
            if semantic_ref.startswith("@"):
                # Reference to a param value (e.g., "@payload.entity_id")
                param_path = semantic_ref.split(".")
                value = resolved_params
                for part in param_path:
                    if part.startswith("@"):
                        continue
                    if isinstance(value, dict) and part in value:
                        value = value[part]
                    else:
                        raise ValueError(f"Semantic key not found: {semantic_ref}")
            else:
                # Direct param key (e.g., "entity_id")
                if semantic_ref in resolved_params:
                    value = resolved_params[semantic_ref]
                else:
                    raise ValueError(f"Semantic key not found in params: {semantic_ref}")
            base_key = f"semantic:{value}"
        else:
            raise ValueError("semantic_key_ref is required for semantic scope")
    elif idempotency.scope == "explicit":
        # Explicit key must be provided in params
        if "idempotency_key" not in resolved_params:
            raise ValueError("explicit scope requires 'idempotency_key' in params")
        base_key = f"explicit:{resolved_params['idempotency_key']}"
    else:
        raise ValueError(f"Unknown idempotency scope: {idempotency.scope}")

    # Optionally add payload hash
    if idempotency.include_payload_hash:
        params_json = json.dumps(resolved_params, sort_keys=True, separators=(",", ":"))
        params_hash = hashlib.sha256(params_json.encode()).hexdigest()[:16]
        return f"{base_key}:{params_hash}"

    return base_key


class Backend(ABC):
    """
    Abstract base class for execution backends.

    Backends handle the actual execution of operations:
    - data_plane: Handled by storacle (query.*, write.*, assert.*)
    - compute: External compute service (compute.*)
    - orchestration: Handled by lorchestra itself (job.*)
    """

    @abstractmethod
    def execute(self, manifest: StepManifest) -> dict[str, Any]:
        """
        Execute a step manifest.

        Args:
            manifest: The StepManifest to execute

        Returns:
            The execution result as a dictionary

        Raises:
            Exception: If execution fails
        """
        pass


class NoOpBackend(Backend):
    """
    No-op backend for testing and dry-run mode.

    Returns empty results without executing anything.
    """

    def execute(self, manifest: StepManifest) -> dict[str, Any]:
        return {
            "status": "noop",
            "op": manifest.op.value,
            "params": manifest.resolved_params,
        }


class ExecutionError(Exception):
    """Raised when step execution fails."""

    def __init__(self, step_id: str, message: str, cause: Optional[Exception] = None):
        self.step_id = step_id
        self.cause = cause
        super().__init__(f"Step '{step_id}' failed: {message}")


class ExecutionResult:
    """Result of executing a job."""

    def __init__(
        self,
        run_record: RunRecord,
        attempt: AttemptRecord,
        success: bool,
        error: Optional[ExecutionError] = None,
    ):
        self.run_record = run_record
        self.attempt = attempt
        self.success = success
        self.error = error

    @property
    def run_id(self) -> str:
        return self.run_record.run_id

    @property
    def failed_steps(self) -> list[StepOutcome]:
        return list(self.attempt.get_failed_steps())


class Executor:
    """
    Execution engine for JobInstances.

    The Executor handles:
    - Step dispatch to handlers via HandlerRegistry
    - Reference resolution
    - Idempotency
    - Retry logic
    - Attempt tracking

    Usage (recommended - with HandlerRegistry):
        from lorchestra.handlers import HandlerRegistry

        registry = HandlerRegistry.create_default()
        # Or manually configure:
        # registry = HandlerRegistry()
        # registry.register("inferometer", ComputeHandler(compute_client))

        executor = Executor(
            store=InMemoryRunStore(),
            handlers=registry,
        )
        result = executor.execute(instance, envelope={"key": "value"})

    Native ops (call, plan.build, storacle.submit) are handled directly
    by the Executor and do not go through the HandlerRegistry.
    """

    def __init__(
        self,
        store: RunStore,
        handlers: Optional["HandlerRegistry"] = None,
        backends: Optional[dict[str, Backend]] = None,
        max_attempts: int = 1,
    ):
        """
        Initialize the executor.

        Args:
            store: RunStore for persisting execution artifacts
            handlers: HandlerRegistry for step dispatch (recommended)
            backends: (Deprecated) Dictionary mapping backend names to Backend implementations.
                     Use `handlers` parameter instead.
            max_attempts: Maximum number of retry attempts (default: 1, no retries)
        """
        self._store = store
        self._max_attempts = max_attempts

        # Handle handlers vs backends (with backwards compatibility)
        if handlers is not None:
            self._handlers = handlers
            self._backends = None
            if backends is not None:
                warnings.warn(
                    "Both 'handlers' and 'backends' provided. 'backends' will be ignored. "
                    "The 'backends' parameter is deprecated; use 'handlers' instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
        elif backends is not None:
            # Legacy mode: wrap backends in a compatibility layer
            warnings.warn(
                "The 'backends' parameter is deprecated. Use 'handlers' with HandlerRegistry instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self._backends = backends
            self._handlers = None
        else:
            # Default: create noop handler registry
            from lorchestra.handlers import HandlerRegistry
            self._handlers = HandlerRegistry.create_noop()
            self._backends = None

    def execute(
        self,
        instance: JobInstance,
        envelope: Optional[dict[str, Any]] = None,
    ) -> ExecutionResult:
        """
        Execute a JobInstance.

        Args:
            instance: The compiled JobInstance to execute
            envelope: Runtime envelope (available as @run.envelope.*)

        Returns:
            ExecutionResult containing the run record, attempt, and status
        """
        envelope = envelope or {}

        # Create run record
        run_record = self._store.create_run(instance, envelope)

        # Execute with retries
        last_error: Optional[ExecutionError] = None
        for attempt_n in range(1, self._max_attempts + 1):
            try:
                attempt = self._execute_attempt(instance, run_record, envelope, attempt_n)
                self._store.store_attempt(attempt)

                if attempt.status == StepStatus.COMPLETED:
                    return ExecutionResult(run_record, attempt, success=True)
                elif attempt.status == StepStatus.FAILED:
                    # Check if we should retry
                    failed_steps = attempt.get_failed_steps()
                    if attempt_n < self._max_attempts:
                        continue
                    # Final attempt failed
                    return ExecutionResult(
                        run_record, attempt, success=False,
                        error=ExecutionError(
                            failed_steps[0].step_id if failed_steps else "unknown",
                            "Step execution failed",
                        ),
                    )
            except ExecutionError as e:
                last_error = e
                if attempt_n < self._max_attempts:
                    continue
                # Create failed attempt record
                attempt = AttemptRecord(
                    run_id=run_record.run_id,
                    attempt_n=attempt_n,
                    started_at=_utcnow(),
                    completed_at=_utcnow(),
                    status=StepStatus.FAILED,
                    step_outcomes=(),
                )
                self._store.store_attempt(attempt)
                return ExecutionResult(run_record, attempt, success=False, error=e)

        # Should not reach here, but handle gracefully
        attempt = AttemptRecord(
            run_id=run_record.run_id,
            attempt_n=self._max_attempts,
            started_at=_utcnow(),
            completed_at=_utcnow(),
            status=StepStatus.FAILED,
            step_outcomes=(),
        )
        return ExecutionResult(run_record, attempt, success=False, error=last_error)

    def _execute_attempt(
        self,
        instance: JobInstance,
        run_record: RunRecord,
        envelope: dict[str, Any],
        attempt_n: int,
    ) -> AttemptRecord:
        """
        Execute a single attempt of a job.

        Args:
            instance: The JobInstance to execute
            run_record: The RunRecord for this execution
            envelope: Runtime envelope
            attempt_n: The attempt number (1-indexed)

        Returns:
            AttemptRecord with step outcomes
        """
        started_at = _utcnow()
        step_outputs: dict[str, Any] = {}
        step_outcomes: list[StepOutcome] = []
        overall_status = StepStatus.COMPLETED
        had_failure = False

        # Make envelope available for @run.envelope.* resolution
        step_outputs["envelope"] = envelope

        for step in instance.steps:
            # Check for compile-time skip
            if step.compiled_skip:
                step_outcomes.append(StepOutcome(
                    step_id=step.step_id,
                    status=StepStatus.SKIPPED,
                ))
                continue

            step_started = _utcnow()
            try:
                # Execute the step
                output, manifest_ref, output_ref = self._execute_step(
                    step, run_record.run_id, step_outputs
                )

                step_completed = _utcnow()
                step_outcomes.append(StepOutcome(
                    step_id=step.step_id,
                    status=StepStatus.COMPLETED,
                    started_at=step_started,
                    completed_at=step_completed,
                    manifest_ref=manifest_ref,
                    output_ref=output_ref,
                ))

                # Store output for subsequent @run.* resolution
                step_outputs[step.step_id] = output

            except Exception as e:
                step_completed = _utcnow()
                error_info = {
                    "type": type(e).__name__,
                    "message": str(e),
                }

                step_outcomes.append(StepOutcome(
                    step_id=step.step_id,
                    status=StepStatus.FAILED,
                    started_at=step_started,
                    completed_at=step_completed,
                    error=error_info,
                ))

                had_failure = True

                # Check continue_on_error
                if not step.continue_on_error:
                    overall_status = StepStatus.FAILED
                    break

        # Determine final status
        if had_failure and overall_status != StepStatus.FAILED:
            # Had failures but all were continue_on_error steps
            # Still mark as completed since we finished all steps
            pass

        completed_at = _utcnow()
        return AttemptRecord(
            run_id=run_record.run_id,
            attempt_n=attempt_n,
            started_at=started_at,
            completed_at=completed_at,
            status=overall_status,
            step_outcomes=tuple(step_outcomes),
        )

    def _execute_step(
        self,
        step: JobStepInstance,
        run_id: str,
        step_outputs: dict[str, Any],
    ) -> tuple[Any, str, str]:
        """
        Execute a single step.

        Args:
            step: The step to execute
            run_id: The run ULID
            step_outputs: Outputs from previous steps

        Returns:
            Tuple of (output, manifest_ref, output_ref)
        """
        # Resolve @run.* references
        resolved_params = _resolve_run_refs(step.params, step_outputs)

        # Compute idempotency key
        # Note: In a real implementation, we'd get the IdempotencyConfig from the step
        # For now, use default run-scoped idempotency
        idempotency = IdempotencyConfig(scope="run")

        idempotency_key = _compute_idempotency_key(
            run_id, step.step_id, step, resolved_params, idempotency
        )

        # Create manifest
        manifest = StepManifest.from_op(
            run_id=run_id,
            step_id=step.step_id,
            op=step.op,
            resolved_params=resolved_params,
            idempotency_key=idempotency_key,
        )

        # Store manifest
        manifest_ref = self._store.store_manifest(manifest)

        # Dispatch to handler or backend
        output = self._dispatch_manifest(manifest, step)

        # Store output
        output_ref = self._store.store_output(run_id, step.step_id, output)

        return output, manifest_ref, output_ref

    def _dispatch_manifest(
        self,
        manifest: StepManifest,
        step: JobStepInstance,
    ) -> dict[str, Any]:
        """
        Dispatch a manifest to the appropriate handler or backend.

        Native ops (call, plan.build, storacle.submit) are handled directly
        by the executor. Other ops (compute.*, job.*) go through HandlerRegistry.

        Args:
            manifest: The StepManifest to execute
            step: The step instance (for error reporting)

        Returns:
            The execution result

        Raises:
            ExecutionError: If no handler/backend is configured for the backend type
        """
        from lorchestra.schemas.ops import Op

        # Native ops: handled directly by executor
        if manifest.op == Op.CALL:
            return self._handle_call(manifest)
        elif manifest.op == Op.PLAN_BUILD:
            return self._handle_plan_build(manifest)
        elif manifest.op == Op.STORACLE_SUBMIT:
            return self._handle_storacle_submit(manifest)

        # Handler-dispatched ops (compute.*, job.*)
        if self._handlers is not None:
            try:
                return self._handlers.dispatch(manifest)
            except KeyError as e:
                raise ExecutionError(
                    step.step_id,
                    f"No handler configured for '{manifest.backend}': {e}",
                ) from e

        # Fall back to legacy backends dict
        if self._backends is not None:
            backend = self._backends.get(step.op.backend)
            if backend is None:
                raise ExecutionError(
                    step.step_id,
                    f"No backend configured for '{step.op.backend}'",
                )
            return backend.execute(manifest)

        # Should not reach here
        raise ExecutionError(
            step.step_id,
            "No handlers or backends configured",
        )

    def _handle_call(self, manifest: StepManifest) -> dict[str, Any]:
        """
        Handle the generic `call` op: dispatch to callable by name.

        The callable name is in params["callable"]. All other params
        are forwarded to the callable. The CallableResult is surfaced
        as the step output so downstream steps can reference @run.step_id.items.

        Args:
            manifest: StepManifest with op=call

        Returns:
            Dict with items, stats, schema_version from CallableResult
        """
        from lorchestra.callable.dispatch import dispatch_callable

        callable_name = manifest.resolved_params["callable"]
        # Forward all params except "callable" to the callable
        params = {k: v for k, v in manifest.resolved_params.items() if k != "callable"}
        result = dispatch_callable(callable_name, params)
        return {
            "items": result.items,
            "stats": result.stats,
            "schema_version": result.schema_version,
        }

    def _handle_plan_build(self, manifest: StepManifest) -> dict[str, Any]:
        """
        Handle the `plan.build` native op: build StoraclePlan from items.

        Reads items from params (typically @run.step_id.items resolved),
        plus method and optional field params, and produces a StoraclePlan.

        Args:
            manifest: StepManifest with op=plan.build

        Returns:
            Dict with plan (serialized StoraclePlan)
        """
        from lorchestra.plan_builder import build_plan_from_items

        items = manifest.resolved_params["items"]
        method = manifest.resolved_params.get("method", "wal.append")
        correlation_id = f"{manifest.run_id}:{manifest.step_id}"

        # Phase 2 field params (optional)
        fields = manifest.resolved_params.get("fields")
        field_map = manifest.resolved_params.get("field_map")
        field_defaults = manifest.resolved_params.get("field_defaults")

        plan = build_plan_from_items(
            items=items,
            correlation_id=correlation_id,
            method=method,
            fields=fields,
            field_map=field_map,
            field_defaults=field_defaults,
        )
        return {"plan": plan.to_dict()}

    def _handle_storacle_submit(self, manifest: StepManifest) -> dict[str, Any]:
        """
        Handle the `storacle.submit` native op: submit plan to storacle.

        Reads the plan dict from params (typically @run.persist.plan resolved)
        and submits it to storacle via the client boundary.

        Args:
            manifest: StepManifest with op=storacle.submit

        Returns:
            Dict with storacle response
        """
        from lorchestra.storacle.client import submit_plan, RpcMeta
        from lorchestra.plan_builder import StoraclePlan, StoracleOp

        plan_dict = manifest.resolved_params["plan"]
        plan = StoraclePlan(
            kind=plan_dict.get("kind", "storacle.plan"),
            version=plan_dict.get("version", "0.1"),
            correlation_id=plan_dict.get("correlation_id", ""),
            ops=[
                StoracleOp(**op_data)
                for op_data in plan_dict.get("ops", [])
            ],
        )
        meta = RpcMeta(
            run_id=manifest.run_id,
            step_id=manifest.step_id,
            correlation_id=plan.correlation_id,
        )
        return submit_plan(plan, meta)


def execute_job(
    job_def: JobDef,
    ctx: Optional[dict[str, Any]] = None,
    payload: Optional[dict[str, Any]] = None,
    envelope: Optional[dict[str, Any]] = None,
    store: Optional[RunStore] = None,
    handlers: Optional["HandlerRegistry"] = None,
    backends: Optional[dict[str, Backend]] = None,
) -> ExecutionResult:
    """
    Compile and execute a job from a JobDef.

    This is the internal implementation. For the public API, use execute(envelope).

    Args:
        job_def: The job definition to execute
        ctx: Context for compilation (@ctx.* resolution)
        payload: Payload for compilation (@payload.* resolution)
        envelope: Runtime envelope (@run.envelope.* resolution)
        store: Optional RunStore (defaults to InMemoryRunStore)
        handlers: Optional HandlerRegistry for step dispatch (recommended)
        backends: (Deprecated) Optional backend configurations. Use handlers instead.

    Returns:
        ExecutionResult with run details and status
    """
    # Compile
    instance = compile_job(job_def, ctx, payload)

    # Execute
    store = store or InMemoryRunStore()
    executor = Executor(store=store, handlers=handlers, backends=backends)
    return executor.execute(instance, envelope=envelope)


def _load_job_def(envelope: dict[str, Any]) -> tuple[JobDef, dict[str, Any], dict[str, Any]]:
    """Load a JobDef from an envelope, returning (job_def, ctx, payload).

    Shared by compile() and execute() so both use the same registry-loading path.
    """
    job_id = envelope["job_id"]
    ctx = envelope.get("ctx", {})
    payload = envelope.get("payload", {})

    registry = envelope.get("registry")
    if registry is None:
        from pathlib import Path
        definitions_dir = envelope.get("definitions_dir", Path.cwd() / "jobs" / "definitions")
        registry = JobRegistry(definitions_dir)

    version = envelope.get("version")
    job_def = registry.load(job_id, version=version)

    return job_def, ctx, payload


def compile(envelope: dict[str, Any]) -> "JobInstance":
    """
    Compile a job from an envelope (without executing).

    Envelope schema:
        job_id: str - The job identifier to load and compile
        ctx: dict - Context for @ctx.* resolution (optional)
        payload: dict - Payload for @payload.* resolution (optional)
        definitions_dir: Path - Directory containing job definitions (optional)
        registry: JobRegistry - Registry to load job from (optional)
        version: str - Job version to load (optional)

    Args:
        envelope: Envelope containing job_id and optional parameters

    Returns:
        JobInstance ready for execution

    Raises:
        KeyError: If job_id is not in envelope
        JobNotFoundError: If job_id is not found in registry
    """
    job_def, ctx, payload = _load_job_def(envelope)
    return compile_job(job_def, ctx, payload)


def execute(envelope: dict[str, Any]) -> ExecutionResult:
    """
    Execute a job from an envelope.

    This is the primary public API for lorchestra execution.

    Envelope schema:
        job_id: str - The job identifier to load and execute
        ctx: dict - Context for @ctx.* resolution (optional)
        payload: dict - Payload for @payload.* resolution (optional)
        smoke_namespace: str - Smoke test namespace for routing BQ writes (optional)
        definitions_dir: Path - Directory containing job definitions (optional)
        registry: JobRegistry - Registry to load job from (optional)
        store: RunStore - Store for run artifacts (optional, defaults to FileRunStore)
        handlers: HandlerRegistry - Handler registry for step dispatch (optional, recommended)
        backends: dict[str, Backend] - (Deprecated) Backend implementations (optional)

    Args:
        envelope: Execution envelope containing job_id and optional parameters

    Returns:
        ExecutionResult with run details and status

    Raises:
        KeyError: If job_id is not in envelope
        JobNotFoundError: If job_id is not found in registry
    """
    import os

    # Load job def via shared path
    job_def, ctx, payload = _load_job_def(envelope)

    # Configure smoke namespace if provided
    smoke_namespace = envelope.get("smoke_namespace")
    if smoke_namespace:
        from lorchestra.stack_clients.event_client import set_run_mode
        set_run_mode(smoke_namespace=smoke_namespace)
        os.environ["STORACLE_SMOKE_NAMESPACE"] = smoke_namespace

    # Get or create store
    store = envelope.get("store")
    if store is None:
        store = FileRunStore(DEFAULT_RUN_PATH)

    # Get handlers (recommended) or backends (deprecated)
    handlers = envelope.get("handlers")
    backends = envelope.get("backends")

    # Execute using internal function
    return execute_job(
        job_def=job_def,
        ctx=ctx,
        payload=payload,
        envelope=envelope,
        store=store,
        handlers=handlers,
        backends=backends,
    )
