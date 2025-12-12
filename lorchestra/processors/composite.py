"""CompositeProcessor - runs multiple child jobs as a single unit.

This processor enables defining composite jobs that group multiple
atomic jobs into single orchestration units. It supports:

- Sequential stages (stage 2 runs after stage 1 completes)
- All child job IDs are declared in job config (YAML/JSON), never in Python
- Structured result reporting per spec

Phase Composite Result Schema:
{
    "job_id": "pipeline.ingest",
    "success": false,
    "total": 23,
    "succeeded": 21,
    "failed": 2,
    "failures": [
        {"job_id": "ingest_gmail_acct3", "error": "Connection timeout"},
        {"job_id": "validate_stripe_refunds", "error": "Invalid data format"}
    ],
    "duration_ms": 45321
}

Sequential Composite (pipeline.daily_all) Result Schema:
{
    "job_id": "pipeline.daily_all",
    "success": false,
    "phases_completed": ["pipeline.ingest", "pipeline.canonize"],
    "failed_phase": "pipeline.formation",
    "phase_results": {
        "pipeline.ingest": { ... },
        "pipeline.canonize": { ... },
        "pipeline.formation": { ... }
    },
    "duration_ms": 123456
}
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from lorchestra.processors.base import (
    EventClient,
    JobContext,
    StorageClient,
    registry,
)

logger = logging.getLogger(__name__)


@dataclass
class CompositeResult:
    """Result of executing a composite job.

    Matches the spec schema:
    - job_id: composite job identifier
    - success: true if failed == 0, false otherwise
    - total: total number of child jobs
    - succeeded: count of successful jobs
    - failed: count of failed jobs
    - failures: list of {job_id, error} for failed jobs
    - duration_ms: total execution time

    For sequential composites (stop_on_failure=True), also includes:
    - phases_completed: list of completed phase job_ids
    - failed_phase: which phase failed (if any)
    - phase_results: map of phase job_id -> result
    """
    job_id: str
    success: bool = True
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    failures: list[dict[str, str]] = field(default_factory=list)
    duration_ms: int = 0
    # For sequential composites (pipeline.daily_all)
    phases_completed: list[str] | None = None
    failed_phase: str | None = None
    phase_results: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict matching spec schema."""
        result = {
            "job_id": self.job_id,
            "success": self.success,
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "failures": self.failures,
            "duration_ms": self.duration_ms,
        }
        # Include sequential composite fields only if present
        if self.phases_completed is not None:
            result["phases_completed"] = self.phases_completed
        if self.failed_phase is not None:
            result["failed_phase"] = self.failed_phase
        if self.phase_results is not None:
            result["phase_results"] = self.phase_results
        return result


class CompositeProcessor:
    """Processor for composite jobs.

    Executes multiple child jobs as a single unit. Child job lists
    are read from job config - NEVER hardcoded in this processor.

    Job config format:
    {
        "job_id": "pipeline.ingest",
        "job_type": "composite",
        "stages": [
            {
                "name": "ingestion",
                "jobs": ["ingest_gmail_acct1", "ingest_gmail_acct2", ...]
            },
            {
                "name": "validation",
                "jobs": ["validate_gmail", "validate_exchange", ...]
            }
        ],
        "stop_on_failure": false  // true for pipeline.daily_all
    }
    """

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> CompositeResult:
        """Execute a composite job.

        Args:
            job_spec: Parsed job specification with stages config.
                      Child job IDs come from job_spec, not hardcoded.
            context: Execution context with BQ client and run_id
            storage_client: Storage operations interface
            event_client: Event emission interface

        Returns:
            CompositeResult matching spec schema
        """
        from lorchestra.job_runner import run_job

        job_id = job_spec["job_id"]
        stages = job_spec.get("stages", [])
        stop_on_failure = job_spec.get("stop_on_failure", False)

        # Count total jobs across all stages
        total_jobs = sum(len(stage.get("jobs", [])) for stage in stages)

        logger.info(f"Starting composite job: {job_id}")
        logger.info(f"  stages: {len(stages)}, total jobs: {total_jobs}")
        logger.info(f"  stop_on_failure: {stop_on_failure}")
        if context.dry_run:
            logger.info("  DRY RUN - will list jobs only")

        # Emit started event
        event_client.log_event(
            event_type="composite.started",
            source_system="lorchestra",
            correlation_id=context.run_id,
            status="success",
            payload={
                "job_id": job_id,
                "stage_count": len(stages),
                "total_jobs": total_jobs,
                "dry_run": context.dry_run,
            },
        )

        start_time = time.time()

        result = CompositeResult(
            job_id=job_id,
            total=total_jobs,
        )

        # For sequential composites, track phase results
        if stop_on_failure:
            result.phases_completed = []
            result.phase_results = {}

        try:
            for stage_idx, stage in enumerate(stages):
                stage_name = stage.get("name", f"stage_{stage_idx + 1}")
                jobs = stage.get("jobs", [])

                logger.info(f"  Stage {stage_idx + 1}/{len(stages)}: {stage_name} ({len(jobs)} jobs)")

                stage_failures = []

                for child_job_id in jobs:
                    child_start = time.time()

                    if context.dry_run:
                        # In dry run, just list what would run
                        logger.info(f"    [DRY-RUN] Would run: {child_job_id}")
                        result.succeeded += 1
                    else:
                        try:
                            logger.info(f"    Running: {child_job_id}")
                            run_job(
                                child_job_id,
                                config=context.config,
                                dry_run=False,
                                test_table=context.test_table,
                                bq_client=context.bq_client,
                            )

                            child_duration = int((time.time() - child_start) * 1000)
                            result.succeeded += 1
                            logger.info(f"    ✓ {child_job_id} ({child_duration}ms)")

                        except Exception as e:
                            child_duration = int((time.time() - child_start) * 1000)
                            error_msg = str(e)
                            result.failed += 1
                            result.failures.append({
                                "job_id": child_job_id,
                                "error": error_msg,
                            })
                            stage_failures.append(child_job_id)
                            logger.error(f"    ✗ {child_job_id}: {error_msg}")

                # For phase composites: continue running all jobs even if some fail
                # For sequential composites: check if we should stop after this stage
                if stop_on_failure:
                    # This is a sequential composite (pipeline.daily_all)
                    # Each "stage" is actually a phase (another composite)
                    if stage_failures:
                        result.failed_phase = stage_name
                        logger.error(f"  Phase {stage_name} failed, stopping composite")
                        break
                    else:
                        result.phases_completed.append(stage_name)

            # success = (failed == 0) for phase composites
            # For sequential composites, success = no failed_phase
            if stop_on_failure:
                result.success = (result.failed_phase is None)
            else:
                result.success = (result.failed == 0)

            result.duration_ms = int((time.time() - start_time) * 1000)

            # Emit completion event
            event_client.log_event(
                event_type="composite.completed" if result.success else "composite.failed",
                source_system="lorchestra",
                correlation_id=context.run_id,
                status="success" if result.success else "failed",
                payload=result.to_dict(),
            )

            logger.info(
                f"Composite {job_id}: "
                f"success={result.success}, succeeded={result.succeeded}, "
                f"failed={result.failed}, duration={result.duration_ms}ms"
            )

            return result

        except Exception as e:
            result.duration_ms = int((time.time() - start_time) * 1000)
            result.success = False
            logger.error(f"Composite job failed: {e}", exc_info=True)

            event_client.log_event(
                event_type="composite.failed",
                source_system="lorchestra",
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "job_id": job_id,
                    "error_type": type(e).__name__,
                    "duration_ms": result.duration_ms,
                },
            )

            raise


# Register with global registry
registry.register("composite", CompositeProcessor())
