"""
Executor - Step dispatch and execution engine.

The Executor implements:
- Step dispatch to appropriate handlers (data_plane, compute, orchestration)
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
   d. Dispatch to handler via HandlerRegistry
   e. Record StepOutcome
4. Update AttemptRecord with final status

Handler Architecture (e005-03):
- HandlerRegistry dispatches StepManifests to appropriate handlers
- DataPlaneHandler: query.*, write.*, assert.* (via StoracleClient)
- ComputeHandler: compute.* (via ComputeClient)
- OrchestrationHandler: job.* (via lorchestra itself)
"""

import hashlib
import json
import re
import warnings
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Callable, Optional, TYPE_CHECKING

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
from .compiler import Compiler, compile_job
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
        from lorchestra.handlers import HandlerRegistry, DataPlaneHandler

        registry = HandlerRegistry()
        registry.register("data_plane", DataPlaneHandler(storacle_client))

        executor = Executor(
            store=InMemoryRunStore(),
            handlers=registry,
        )
        result = executor.execute(instance, envelope={"key": "value"})

    Usage (legacy - with backends dict, deprecated):
        executor = Executor(
            store=InMemoryRunStore(),
            backends={
                "data_plane": SomeBackend(),
            },
        )
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

        Args:
            manifest: The StepManifest to execute
            step: The step instance (for error reporting)

        Returns:
            The execution result

        Raises:
            ExecutionError: If no handler/backend is configured for the backend type
        """
        # Use handlers (new pattern) if available
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


def execute(envelope: dict[str, Any]) -> ExecutionResult:
    """
    Execute a job from an envelope.

    This is the primary public API for lorchestra execution.

    Envelope schema:
        job_id: str - The job identifier to load and execute
        ctx: dict - Context for @ctx.* resolution (optional)
        payload: dict - Payload for @payload.* resolution (optional)
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
    # Extract required fields
    job_id = envelope["job_id"]

    # Extract optional fields
    ctx = envelope.get("ctx", {})
    payload = envelope.get("payload", {})

    # Get or create registry
    registry = envelope.get("registry")
    if registry is None:
        # Default to looking in current directory's jobs/definitions
        from pathlib import Path
        definitions_dir = envelope.get("definitions_dir", Path.cwd() / "jobs" / "definitions")
        registry = JobRegistry(definitions_dir)

    # Load job definition
    version = envelope.get("version")
    job_def = registry.load(job_id, version=version)

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
