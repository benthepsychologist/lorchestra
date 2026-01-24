"""
Compiler - Transform JobDef + context into JobInstance.

The compiler resolves:
- @ctx.* references from the compilation context
- @payload.* references from the job payload
- if_ conditions (compile-time only, no @run.* refs allowed)

The resulting JobInstance has:
- Fixed step list (no dynamic expansion)
- Resolved compile-time references
- compiled_skip flags for steps whose if_ evaluated to false
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Optional

from lorchestra.schemas import (
    JobDef,
    JobInstance,
    JobStepInstance,
    StepDef,
    IdempotencyConfig,
    CompileError,
)

from .registry import JobRegistry


# Reference pattern: @namespace.path.to.value
# Namespace is one of: ctx, payload, run
REF_PATTERN = re.compile(r"@(ctx|payload|run)\.([a-zA-Z_][a-zA-Z0-9_.]*)")


def _utcnow() -> datetime:
    """Return current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)


def _resolve_reference(ref: str, ctx: dict[str, Any], payload: dict[str, Any]) -> Any:
    """
    Resolve a single reference string.

    Args:
        ref: Reference string like "@ctx.project_id" or "@payload.entity_id"
        ctx: Context dictionary
        payload: Payload dictionary

    Returns:
        The resolved value

    Raises:
        CompileError: If the reference cannot be resolved
    """
    match = REF_PATTERN.match(ref)
    if not match:
        raise CompileError(f"Invalid reference format: {ref}")

    namespace = match.group(1)
    path = match.group(2)

    # Select source dictionary
    if namespace == "ctx":
        source = ctx
    elif namespace == "payload":
        source = payload
    elif namespace == "run":
        # @run.* refs are resolved at runtime, not compile time
        raise CompileError(f"@run.* references cannot be resolved at compile time: {ref}")
    else:
        raise CompileError(f"Unknown namespace: {namespace}")

    # Navigate path
    value = source
    for part in path.split("."):
        if isinstance(value, dict):
            if part not in value:
                raise CompileError(f"Reference path not found: {ref} (missing '{part}')")
            value = value[part]
        else:
            raise CompileError(f"Cannot navigate into non-dict at '{part}' in {ref}")

    return value


def _contains_run_refs(value: Any) -> bool:
    """Check if a value contains @run.* references."""
    if isinstance(value, str):
        return "@run." in value
    elif isinstance(value, dict):
        return any(_contains_run_refs(v) for v in value.values())
    elif isinstance(value, (list, tuple)):
        return any(_contains_run_refs(v) for v in value)
    return False


def _resolve_value(
    value: Any,
    ctx: dict[str, Any],
    payload: dict[str, Any],
    preserve_run_refs: bool = True,
) -> Any:
    """
    Recursively resolve references in a value.

    Args:
        value: The value to resolve (may be str, dict, list, or primitive)
        ctx: Context dictionary
        payload: Payload dictionary
        preserve_run_refs: If True, @run.* refs are left unresolved

    Returns:
        The resolved value

    Raises:
        CompileError: If a compile-time reference cannot be resolved
    """
    if isinstance(value, str):
        # Check for reference patterns
        if value.startswith("@"):
            match = REF_PATTERN.match(value)
            if match:
                namespace = match.group(1)
                if namespace == "run" and preserve_run_refs:
                    # Leave @run.* refs for runtime resolution
                    return value
                return _resolve_reference(value, ctx, payload)
        # Handle embedded references in strings (e.g., "prefix_@ctx.id_suffix")
        # For now, only support full-string references
        return value
    elif isinstance(value, dict):
        return {k: _resolve_value(v, ctx, payload, preserve_run_refs) for k, v in value.items()}
    elif isinstance(value, (list, tuple)):
        return [_resolve_value(v, ctx, payload, preserve_run_refs) for v in value]
    else:
        # Primitives pass through unchanged
        return value


def _evaluate_condition(
    condition: str,
    ctx: dict[str, Any],
    payload: dict[str, Any],
) -> bool:
    """
    Evaluate a compile-time condition.

    Conditions are simple expressions that resolve to a boolean:
    - "@ctx.enabled" -> resolves to the value of ctx["enabled"]
    - "@ctx.mode == 'prod'" -> comparison
    - "@payload.skip" -> resolves to payload["skip"]

    Args:
        condition: The condition string
        ctx: Context dictionary
        payload: Payload dictionary

    Returns:
        Boolean result of the condition

    Raises:
        CompileError: If the condition is invalid or contains @run.* refs
    """
    if "@run." in condition:
        raise CompileError(
            f"if condition must be compile-time decidable. "
            f"@run.* references are not allowed: {condition}"
        )

    # Simple reference case: "@ctx.enabled" or "@payload.skip"
    if condition.startswith("@"):
        match = REF_PATTERN.match(condition)
        if match and condition == match.group(0):
            # Full match - this is a simple reference
            value = _resolve_reference(condition, ctx, payload)
            return bool(value)

    # Expression case: "@ctx.mode == 'prod'"
    # Support simple equality/inequality comparisons
    for op, op_func in [
        ("==", lambda a, b: a == b),
        ("!=", lambda a, b: a != b),
        (">=", lambda a, b: a >= b),
        ("<=", lambda a, b: a <= b),
        (">", lambda a, b: a > b),
        ("<", lambda a, b: a < b),
    ]:
        if op in condition:
            parts = condition.split(op, 1)
            if len(parts) == 2:
                left = parts[0].strip()
                right = parts[1].strip()

                # Resolve left side if it's a reference
                if left.startswith("@"):
                    left_val = _resolve_reference(left, ctx, payload)
                else:
                    left_val = _parse_literal(left)

                # Resolve right side if it's a reference
                if right.startswith("@"):
                    right_val = _resolve_reference(right, ctx, payload)
                else:
                    right_val = _parse_literal(right)

                return op_func(left_val, right_val)

    raise CompileError(f"Cannot evaluate condition: {condition}")


def _parse_literal(s: str) -> Any:
    """Parse a literal value from a string."""
    s = s.strip()

    # String literals
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        return s[1:-1]

    # Boolean literals
    if s.lower() == "true":
        return True
    if s.lower() == "false":
        return False

    # None/null
    if s.lower() in ("none", "null"):
        return None

    # Numeric literals
    try:
        if "." in s:
            return float(s)
        return int(s)
    except ValueError:
        pass

    # Return as-is
    return s


class Compiler:
    """
    Compiler for transforming JobDef + context into JobInstance.

    Usage:
        registry = JobRegistry(definitions_dir)
        compiler = Compiler(registry)
        instance = compiler.compile("my_job", ctx={"project": "prod"}, payload={"id": 123})
    """

    def __init__(self, registry: JobRegistry):
        """
        Initialize the compiler.

        Args:
            registry: JobRegistry for loading job definitions
        """
        self._registry = registry

    def compile(
        self,
        job_id: str,
        ctx: Optional[dict[str, Any]] = None,
        payload: Optional[dict[str, Any]] = None,
    ) -> JobInstance:
        """
        Compile a job definition into an executable instance.

        Args:
            job_id: The job identifier to compile
            ctx: Context dictionary for @ctx.* resolution
            payload: Payload dictionary for @payload.* resolution

        Returns:
            A JobInstance ready for execution

        Raises:
            JobNotFoundError: If the job definition doesn't exist
            CompileError: If compilation fails (invalid refs, conditions, etc.)
        """
        ctx = ctx or {}
        payload = payload or {}

        # Load the job definition
        job_def = self._registry.load(job_id)

        return self._compile_job_def(job_def, ctx, payload)

    def compile_def(
        self,
        job_def: JobDef,
        ctx: Optional[dict[str, Any]] = None,
        payload: Optional[dict[str, Any]] = None,
    ) -> JobInstance:
        """
        Compile a JobDef directly (without registry lookup).

        Args:
            job_def: The job definition to compile
            ctx: Context dictionary for @ctx.* resolution
            payload: Payload dictionary for @payload.* resolution

        Returns:
            A JobInstance ready for execution
        """
        ctx = ctx or {}
        payload = payload or {}
        return self._compile_job_def(job_def, ctx, payload)

    def _compile_job_def(
        self,
        job_def: JobDef,
        ctx: dict[str, Any],
        payload: dict[str, Any],
    ) -> JobInstance:
        """
        Internal implementation of job compilation.

        Args:
            job_def: The job definition
            ctx: Context dictionary
            payload: Payload dictionary

        Returns:
            Compiled JobInstance
        """
        compiled_steps: list[JobStepInstance] = []

        for step_def in job_def.steps:
            compiled_step = self._compile_step(step_def, ctx, payload)
            compiled_steps.append(compiled_step)

        return JobInstance(
            job_id=job_def.job_id,
            job_version=job_def.version,
            job_def_sha256=JobRegistry.compute_hash(job_def),
            compiled_at=_utcnow(),
            steps=tuple(compiled_steps),
        )

    def _compile_step(
        self,
        step_def: StepDef,
        ctx: dict[str, Any],
        payload: dict[str, Any],
    ) -> JobStepInstance:
        """
        Compile a single step definition.

        Args:
            step_def: The step definition
            ctx: Context dictionary
            payload: Payload dictionary

        Returns:
            Compiled JobStepInstance
        """
        # Evaluate if_ condition (compile-time only)
        compiled_skip = False
        if step_def.if_ is not None:
            try:
                condition_result = _evaluate_condition(step_def.if_, ctx, payload)
                compiled_skip = not condition_result
            except CompileError:
                raise
            except Exception as e:
                raise CompileError(
                    f"Step '{step_def.step_id}': Failed to evaluate if condition: {e}"
                )

        # Resolve compile-time references in params
        # @run.* refs are preserved for runtime resolution
        resolved_params = _resolve_value(step_def.params, ctx, payload, preserve_run_refs=True)

        return JobStepInstance(
            step_id=step_def.step_id,
            op=step_def.op,
            params=resolved_params,
            phase_id=step_def.phase_id,
            timeout_s=step_def.timeout_s,
            continue_on_error=step_def.continue_on_error,
            compiled_skip=compiled_skip,
        )


def compile_job(
    job_def: JobDef,
    ctx: Optional[dict[str, Any]] = None,
    payload: Optional[dict[str, Any]] = None,
) -> JobInstance:
    """
    Convenience function to compile a JobDef without a registry.

    Args:
        job_def: The job definition to compile
        ctx: Context dictionary for @ctx.* resolution
        payload: Payload dictionary for @payload.* resolution

    Returns:
        A JobInstance ready for execution
    """
    ctx = ctx or {}
    payload = payload or {}

    compiled_steps: list[JobStepInstance] = []

    for step_def in job_def.steps:
        # Evaluate if_ condition
        compiled_skip = False
        if step_def.if_ is not None:
            condition_result = _evaluate_condition(step_def.if_, ctx, payload)
            compiled_skip = not condition_result

        # Resolve params
        resolved_params = _resolve_value(step_def.params, ctx, payload, preserve_run_refs=True)

        compiled_steps.append(JobStepInstance(
            step_id=step_def.step_id,
            op=step_def.op,
            params=resolved_params,
            phase_id=step_def.phase_id,
            timeout_s=step_def.timeout_s,
            continue_on_error=step_def.continue_on_error,
            compiled_skip=compiled_skip,
        ))

    return JobInstance(
        job_id=job_def.job_id,
        job_version=job_def.version,
        job_def_sha256=JobRegistry.compute_hash(job_def),
        compiled_at=_utcnow(),
        steps=tuple(compiled_steps),
    )
