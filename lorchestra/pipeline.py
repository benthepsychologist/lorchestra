"""Pipeline runner — sequential execution of jobs via execute().

This module provides pipeline orchestration *above* the JobDef/Executor stack.
A pipeline is a list of stages, each containing job_ids to execute sequentially.
Each job is executed via lorchestra.executor.execute() — the same path as
`lorchestra exec run <job_id>`.

This replaces CompositeProcessor.run() which called run_job() for each child.

Pipeline YAML schema:
    pipeline_id: pipeline.formation
    description: Run measurement events and observations jobs
    stages:
      - name: measurement_events
        jobs: [proj_me_intake_01, proj_me_intake_02, proj_me_followup]
      - name: observations
        jobs: [proj_obs_intake_01, proj_obs_intake_02, proj_obs_followup]
    stop_on_failure: false
"""

import json
import logging
import time
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


def _run_child(
    job_id: str,
    smoke_namespace: str | None,
    definitions_dir: Path | None,
) -> tuple[bool, str | None]:
    """Run a single child — either a pipeline (recursively) or a job via execute().

    Returns:
        (success, error_message_or_none)
    """
    if _is_pipeline(job_id, definitions_dir):
        child_spec = load_pipeline(job_id, definitions_dir)
        child_result = run_pipeline(child_spec, smoke_namespace, definitions_dir)
        if child_result.success:
            return True, None
        else:
            error_msg = f"sub-pipeline failed ({child_result.failed}/{child_result.total} jobs)"
            return False, error_msg
    else:
        from lorchestra.executor import execute

        envelope: dict[str, Any] = {"job_id": job_id}
        if smoke_namespace:
            envelope["smoke_namespace"] = smoke_namespace
        if definitions_dir:
            envelope["definitions_dir"] = definitions_dir

        exec_result = execute(envelope)
        if exec_result.success:
            return True, None
        else:
            error_msg = str(exec_result.error) if exec_result.error else "execution failed"
            return False, error_msg


def run_pipeline(
    pipeline_spec: dict,
    smoke_namespace: str | None = None,
    definitions_dir: Path | None = None,
) -> PipelineResult:
    """Execute a pipeline — sequential run of jobs via execute().

    Iterates through stages and jobs, calling execute() for each.
    If a child job_id is itself a pipeline, it runs recursively.
    This is the v2 equivalent of CompositeProcessor.run().

    Args:
        pipeline_spec: Pipeline spec dict with stages and stop_on_failure
        smoke_namespace: Optional smoke test namespace for BQ routing
        definitions_dir: Override definitions directory for child job loading

    Returns:
        PipelineResult with execution summary
    """
    pipeline_id = pipeline_spec.get("pipeline_id", "unknown")
    stages = pipeline_spec.get("stages", [])
    stop_on_failure = pipeline_spec.get("stop_on_failure", False)

    total_jobs = sum(len(stage.get("jobs", [])) for stage in stages)

    logger.info(f"Starting pipeline: {pipeline_id}")
    logger.info(f"  stages: {len(stages)}, total jobs: {total_jobs}")
    logger.info(f"  stop_on_failure: {stop_on_failure}")

    start_time = time.time()
    result = PipelineResult(pipeline_id=pipeline_id, total=total_jobs)

    for stage in stages:
        stage_name = stage.get("name", "unnamed")
        jobs = stage.get("jobs", [])
        stage_had_failure = False

        logger.info(f"  Stage: {stage_name} ({len(jobs)} jobs)")

        for job_id in jobs:
            job_start = time.time()

            try:
                logger.info(f"    Running: {job_id}")
                success, error_msg = _run_child(job_id, smoke_namespace, definitions_dir)
                job_duration = int((time.time() - job_start) * 1000)

                if success:
                    result.succeeded += 1
                    logger.info(f"    ok {job_id} ({job_duration}ms)")
                else:
                    result.failed += 1
                    result.failures.append({"job_id": job_id, "error": error_msg})
                    stage_had_failure = True
                    logger.error(f"    FAIL {job_id}: {error_msg}")

            except Exception as e:
                job_duration = int((time.time() - job_start) * 1000)
                result.failed += 1
                result.failures.append({"job_id": job_id, "error": str(e)})
                stage_had_failure = True
                logger.error(f"    FAIL {job_id}: {e}")

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
