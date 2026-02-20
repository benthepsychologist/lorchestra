"""Pipeline runner — sequential execution of jobs via execute().

This module provides pipeline orchestration *above* the JobDef/Executor stack.
A pipeline is a list of stages, each containing job_ids to execute sequentially.
Each job is executed via lorchestra.executor.execute() — the same path as
`lorchestra exec run <job_id>`.

This replaces CompositeProcessor.run() which called run_job() for each child.

Pipeline YAML schema (static):
    pipeline_id: pipeline.formation
    description: Run measurement events and observations jobs
    stages:
      - name: measurement_events
        jobs: [proj_me_intake_01, proj_me_intake_02, proj_me_followup]
      - name: observations
        jobs: [proj_obs_intake_01, proj_obs_intake_02, proj_obs_followup]
    stop_on_failure: false

Pipeline YAML schema (batch with loop):
    pipeline_id: pipeline.extract_batch
    stages:
      - name: query_phase
        jobs:
          - peek
      - name: extraction
        loop:
          over: '@run.peek.items'
          payload:
            session_id: '@item.session_id'
            model: '@payload.model'
          jobs:
            - llm_extract_evidence
    stop_on_failure: false
"""

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFINITIONS_DIR = Path(__file__).parent / "jobs" / "definitions"


@dataclass
class PipelineResult:
    """Result of executing a pipeline.

    Matches the shape of the old CompositeResult:
    - pipeline_id: pipeline identifier
    - success: true if no failures
    - total: total number of child jobs
    - succeeded: count of successful jobs
    - failed: count of failed jobs
    - failures: list of {job_id, error} for failed jobs
    - duration_ms: total execution time
    - stopped_early: true if stop_on_failure triggered
    """
    pipeline_id: str
    success: bool = True
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    failures: list[dict[str, str]] = field(default_factory=list)
    duration_ms: int = 0
    stopped_early: bool = False

    def to_dict(self) -> dict[str, Any]:
        result = {
            "pipeline_id": self.pipeline_id,
            "success": self.success,
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "failures": self.failures,
            "duration_ms": self.duration_ms,
        }
        if self.stopped_early:
            result["stopped_early"] = True
        return result


def load_pipeline(pipeline_id: str, definitions_dir: Path | None = None) -> dict:
    """Load a pipeline spec from definitions/pipeline/.

    Tries YAML first, falls back to JSON (for old composite definitions).

    Args:
        pipeline_id: Pipeline identifier (e.g., "pipeline.formation")
        definitions_dir: Override definitions directory (default: standard location)

    Returns:
        Pipeline spec dict with pipeline_id, stages, stop_on_failure

    Raises:
        FileNotFoundError: If pipeline definition not found
    """
    base_dir = (definitions_dir or DEFINITIONS_DIR) / "pipeline"

    # Try YAML first
    yaml_path = base_dir / f"{pipeline_id}.yaml"
    if yaml_path.exists():
        import yaml
        with open(yaml_path) as f:
            return yaml.safe_load(f)

    # Fall back to JSON
    json_path = base_dir / f"{pipeline_id}.json"
    if json_path.exists():
        with open(json_path) as f:
            return json.load(f)

    raise FileNotFoundError(
        f"Pipeline definition not found: {pipeline_id} "
        f"(searched {base_dir} for .yaml and .json)"
    )


def _is_pipeline(job_id: str, definitions_dir: Path | None = None) -> bool:
    """Check if a job_id refers to a pipeline definition (not a JobDef)."""
    base_dir = (definitions_dir or DEFINITIONS_DIR) / "pipeline"
    return (base_dir / f"{job_id}.yaml").exists() or (base_dir / f"{job_id}.json").exists()


# ---------------------------------------------------------------------------
# Reference resolution for @run.*, @item.*, @payload.*
# ---------------------------------------------------------------------------


def _resolve_context_ref(ref: str, context: dict) -> Any:
    """Resolve a @run.job_id.field reference from pipeline context.

    Example: '@run.peek.items' -> context['@run']['peek']['items']

    Returns an empty list if the reference path doesn't exist,
    since loop references typically expect an iterable.
    """
    if not isinstance(ref, str) or not ref.startswith("@run."):
        return ref
    parts = ref[5:].split(".")  # Strip '@run.'
    result = context.get("@run", {})
    for part in parts:
        if isinstance(result, dict):
            result = result.get(part)
        else:
            return []
    return result if result is not None else []


def _resolve_payload_template(template: dict, item: dict, context: dict) -> dict:
    """Resolve @item.*, @payload.*, @run.* references in a loop payload template.

    Args:
        template: Payload template from loop config (e.g., {session_id: '@item.session_id'})
        item: Current iteration item from the loop
        context: Pipeline context with @run and @payload
    """
    resolved = {}
    for key, value in template.items():
        if isinstance(value, str):
            if value.startswith("@item."):
                resolved[key] = item.get(value[6:])
            elif value.startswith("@payload."):
                resolved[key] = context.get("@payload", {}).get(value[9:])
            elif value.startswith("@run."):
                resolved[key] = _resolve_context_ref(value, context)
            else:
                resolved[key] = value
        else:
            resolved[key] = value
    return resolved


def _capture_job_output(job_id: str, exec_result: Any, context: dict) -> None:
    """Capture the last step's output dict into @run context.

    Only captures if the last step produced a dict output.
    Write-only jobs (e.g., storacle.submit) won't have a dict output — that's fine,
    @run simply won't have an entry for that job.
    """
    if not exec_result:
        return
    step_outputs = getattr(exec_result, "step_outputs", None)
    if not step_outputs or not isinstance(step_outputs, dict):
        return
    # Get step IDs excluding 'envelope' (internal to executor)
    step_ids = [k for k in step_outputs if k != "envelope"]
    if not step_ids:
        return
    last_output = step_outputs[step_ids[-1]]
    if isinstance(last_output, dict):
        context["@run"][job_id] = last_output


# ---------------------------------------------------------------------------
# Job execution
# ---------------------------------------------------------------------------


def _run_child(
    job_id: str,
    smoke_namespace: str | None,
    definitions_dir: Path | None,
    payload: dict | None = None,
) -> tuple[bool, str | None, Any]:
    """Run a single child — either a pipeline (recursively) or a job via execute().

    Args:
        job_id: Job or pipeline identifier
        smoke_namespace: Optional smoke test namespace
        definitions_dir: Optional definitions directory override
        payload: Optional payload dict for @payload.* resolution in the job

    Returns:
        (success, error_message_or_none, execution_result_or_none)
    """
    if _is_pipeline(job_id, definitions_dir):
        child_spec = load_pipeline(job_id, definitions_dir)
        child_result = run_pipeline(child_spec, smoke_namespace, definitions_dir)
        if child_result.success:
            return True, None, None
        else:
            error_msg = f"sub-pipeline failed ({child_result.failed}/{child_result.total} jobs)"
            return False, error_msg, None
    else:
        from lorchestra.executor import execute

        envelope: dict[str, Any] = {"job_id": job_id}
        if smoke_namespace:
            envelope["smoke_namespace"] = smoke_namespace
            # Default limit for smoke tests to avoid full data loads
            envelope["limit"] = 10
        if definitions_dir:
            envelope["definitions_dir"] = str(definitions_dir)
        if payload:
            envelope["payload"] = payload

        exec_result = execute(envelope)
        if exec_result.success:
            return True, None, exec_result
        else:
            error_msg = str(exec_result.error) if exec_result.error else "execution failed"
            return False, error_msg, exec_result


# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------


def run_pipeline(
    pipeline_spec: dict,
    smoke_namespace: str | None = None,
    definitions_dir: Path | None = None,
    progress_callback: Callable[..., Any] | None = None,
    payload: dict | None = None,
) -> PipelineResult:
    """Execute a pipeline — sequential run of jobs via execute().

    Iterates through stages and jobs, calling execute() for each.
    If a child job_id is itself a pipeline, it runs recursively.
    Stages with a 'loop' directive iterate over a prior job's output,
    binding @item.* and @payload.* references in the loop payload.

    Args:
        pipeline_spec: Pipeline spec dict with stages and stop_on_failure
        smoke_namespace: Optional smoke test namespace for BQ routing
        definitions_dir: Override definitions directory for child job loading
        progress_callback: Optional callback(event, **kwargs) for progress updates.
            Events: 'stage_start', 'job_start', 'job_ok', 'job_fail'
        payload: Optional payload dict for @payload.* resolution in loop stages

    Returns:
        PipelineResult with execution summary
    """
    pipeline_id = pipeline_spec.get("pipeline_id", "unknown")
    stages = pipeline_spec.get("stages", [])
    stop_on_failure = pipeline_spec.get("stop_on_failure", False)

    # Static stages contribute to upfront total; loop stages are counted dynamically
    total_jobs = sum(len(stage.get("jobs", [])) for stage in stages if "loop" not in stage)

    # Pipeline context: captures job outputs and invocation payload
    context: dict[str, Any] = {
        "@run": {},
        "@payload": payload or pipeline_spec.get("payload", {}),
    }

    logger.info(f"Starting pipeline: {pipeline_id}")
    logger.info(f"  stages: {len(stages)}, static jobs: {total_jobs}")
    logger.info(f"  stop_on_failure: {stop_on_failure}")

    start_time = time.time()
    result = PipelineResult(pipeline_id=pipeline_id, total=total_jobs)

    def _emit(event: str, **kwargs):
        if progress_callback:
            progress_callback(event, **kwargs)

    for stage in stages:
        stage_name = stage.get("name", "unnamed")
        stage_had_failure = False

        if "loop" in stage:
            stage_had_failure = _run_loop_stage(
                stage, context, smoke_namespace, definitions_dir,
                result, _emit,
            )
        else:
            stage_had_failure = _run_static_stage(
                stage, context, smoke_namespace, definitions_dir,
                result, _emit,
            )

        if stage_had_failure and stop_on_failure:
            logger.error(f"  Stage {stage_name} failed, stopping pipeline")
            result.stopped_early = True
            break

    result.success = result.failed == 0
    result.duration_ms = int((time.time() - start_time) * 1000)

    logger.info(
        f"Pipeline {pipeline_id}: "
        f"success={result.success}, succeeded={result.succeeded}, "
        f"failed={result.failed}, duration={result.duration_ms}ms"
    )

    return result


def _run_static_stage(
    stage: dict,
    context: dict,
    smoke_namespace: str | None,
    definitions_dir: Path | None,
    result: PipelineResult,
    emit: Callable,
) -> bool:
    """Run a static stage (list of job_ids). Returns True if stage had a failure."""
    stage_name = stage.get("name", "unnamed")
    jobs = stage.get("jobs", [])
    stage_had_failure = False

    logger.info(f"  Stage: {stage_name} ({len(jobs)} jobs)")
    emit("stage_start", stage_name=stage_name, job_count=len(jobs))

    for job_id in jobs:
        job_start = time.time()

        try:
            logger.info(f"    Running: {job_id}")
            emit("job_start", job_id=job_id)
            success, error_msg, exec_result = _run_child(
                job_id, smoke_namespace, definitions_dir,
            )
            job_duration = int((time.time() - job_start) * 1000)

            if success:
                result.succeeded += 1
                if exec_result:
                    _capture_job_output(job_id, exec_result, context)
                logger.info(f"    ok {job_id} ({job_duration}ms)")
                emit("job_ok", job_id=job_id, duration_ms=job_duration)
            else:
                result.failed += 1
                result.failures.append({"job_id": job_id, "error": error_msg})
                stage_had_failure = True
                logger.error(f"    FAIL {job_id}: {error_msg}")
                emit("job_fail", job_id=job_id, duration_ms=job_duration, error=error_msg)

        except Exception as e:
            job_duration = int((time.time() - job_start) * 1000)
            result.failed += 1
            result.failures.append({"job_id": job_id, "error": str(e)})
            stage_had_failure = True
            logger.error(f"    FAIL {job_id}: {e}")
            emit("job_fail", job_id=job_id, duration_ms=job_duration, error=str(e))

    return stage_had_failure


def _run_loop_stage(
    stage: dict,
    context: dict,
    smoke_namespace: str | None,
    definitions_dir: Path | None,
    result: PipelineResult,
    emit: Callable,
) -> bool:
    """Run a loop stage — iterate over a prior job's output and run jobs per item.

    Loop config schema:
        loop:
          over: '@run.peek.items'    # Reference to prior job output
          payload:                    # Template with @item.*, @payload.*, @run.* refs
            session_id: '@item.session_id'
            model: '@payload.model'
          jobs:                       # Jobs to run for each item
            - llm_extract_evidence

    Returns True if stage had a failure.
    """
    stage_name = stage.get("name", "unnamed")
    loop_config = stage["loop"]
    stage_had_failure = False

    # Resolve the items to iterate over
    items = _resolve_context_ref(loop_config["over"], context)
    if not isinstance(items, list):
        logger.warning(f"  Stage: {stage_name} loop over resolved to non-list, skipping")
        items = []

    loop_jobs = loop_config.get("jobs", [])
    payload_template = loop_config.get("payload", {})

    logger.info(f"  Stage: {stage_name} (loop: {len(items)} items x {len(loop_jobs)} jobs)")
    emit("stage_start", stage_name=stage_name, job_count=len(items) * len(loop_jobs))

    for item in items:
        for job_id in loop_jobs:
            result.total += 1
            job_start = time.time()

            try:
                # Resolve payload template for this iteration
                resolved_payload = _resolve_payload_template(
                    payload_template, item, context,
                )

                logger.info(f"    Running: {job_id}")
                emit("job_start", job_id=job_id)
                success, error_msg, exec_result = _run_child(
                    job_id, smoke_namespace, definitions_dir,
                    payload=resolved_payload,
                )
                job_duration = int((time.time() - job_start) * 1000)

                if success:
                    result.succeeded += 1
                    if exec_result:
                        _capture_job_output(job_id, exec_result, context)
                    logger.info(f"    ok {job_id} ({job_duration}ms)")
                    emit("job_ok", job_id=job_id, duration_ms=job_duration)
                else:
                    result.failed += 1
                    result.failures.append({"job_id": job_id, "error": error_msg})
                    stage_had_failure = True
                    logger.error(f"    FAIL {job_id}: {error_msg}")
                    emit("job_fail", job_id=job_id, duration_ms=job_duration, error=error_msg)

            except Exception as e:
                job_duration = int((time.time() - job_start) * 1000)
                result.failed += 1
                result.failures.append({"job_id": job_id, "error": str(e)})
                stage_had_failure = True
                logger.error(f"    FAIL {job_id}: {e}")
                emit("job_fail", job_id=job_id, duration_ms=job_duration, error=str(e))

    return stage_had_failure
