"""
Orchestration Handler for job.run operations.

Handles orchestration operations:
- job.run: Execute a sub-job within the current execution context

This handler is unique in that it calls back into lorchestra itself
to execute nested jobs, enabling job composition.
"""

from typing import Any, TYPE_CHECKING

from lorchestra.handlers.base import Handler
from lorchestra.schemas import StepManifest, Op

if TYPE_CHECKING:
    from lorchestra.handlers.registry import HandlerRegistry
    from lorchestra.run_store import RunStore


class OrchestrationHandler(Handler):
    """
    Handler for orchestration operations (job.*).

    Currently supports:
    - job.run: Execute a sub-job

    This handler enables job composition by calling back into
    the lorchestra execution engine.
    """

    def __init__(
        self,
        registry: "HandlerRegistry | None" = None,
        store: "RunStore | None" = None,
    ):
        """
        Initialize the orchestration handler.

        Args:
            registry: HandlerRegistry for sub-job execution (optional)
            store: RunStore for sub-job artifacts (optional)

        Note: If registry/store are not provided, sub-jobs will be
        executed in noop mode.
        """
        self._registry = registry
        self._store = store

    def execute(self, manifest: StepManifest) -> dict[str, Any]:
        """
        Execute an orchestration operation.

        Args:
            manifest: The StepManifest containing operation details

        Returns:
            The operation result as a dictionary

        Raises:
            ValueError: If the operation is not an orchestration operation
        """
        op = manifest.op
        params = manifest.resolved_params

        if op == Op.JOB_RUN:
            return self._job_run(params, manifest)

        raise ValueError(f"Unsupported orchestration op: {op.value}")

    def _job_run(
        self, params: dict[str, Any], manifest: StepManifest
    ) -> dict[str, Any]:
        """
        Execute job.run operation.

        Params:
            job_id: str - Job identifier to execute
            ctx: dict - Context for @ctx.* resolution (optional)
            payload: dict - Payload for @payload.* resolution (optional)
            version: str - Job version to execute (optional)

        Returns:
            Dict with sub-job execution result
        """
        job_id = params["job_id"]
        ctx = params.get("ctx", {})
        payload = params.get("payload", {})
        version = params.get("version")

        # If no registry/store configured, return noop result
        if self._registry is None or self._store is None:
            return {
                "status": "noop",
                "job_id": job_id,
                "message": "Sub-job execution not configured (no registry/store)",
            }

        # Import here to avoid circular imports
        from lorchestra.registry import JobRegistry
        from lorchestra.compiler import compile_job
        from lorchestra.executor import Executor

        # Load and compile sub-job
        # Note: In real usage, the JobRegistry would be passed in or configured
        # For now, we return a placeholder result
        try:
            # Try to load job definition
            # The registry path would typically be configured at handler creation
            definitions_dir = params.get("definitions_dir")
            if definitions_dir:
                job_registry = JobRegistry(definitions_dir)
                job_def = job_registry.load(job_id, version=version)

                # Compile the job
                instance = compile_job(job_def, ctx, payload)

                # Execute with same registry/store
                executor = Executor(
                    store=self._store,
                    handlers=self._registry,
                )

                # Create envelope for sub-job
                envelope = {
                    "parent_run_id": manifest.run_id,
                    "parent_step_id": manifest.step_id,
                    **params.get("envelope", {}),
                }

                result = executor.execute(instance, envelope=envelope)

                return {
                    "status": "completed" if result.success else "failed",
                    "job_id": job_id,
                    "run_id": result.run_id,
                    "success": result.success,
                    "error": str(result.error) if result.error else None,
                }
            else:
                return {
                    "status": "skipped",
                    "job_id": job_id,
                    "message": "No definitions_dir provided for sub-job loading",
                }

        except Exception as e:
            return {
                "status": "failed",
                "job_id": job_id,
                "error": str(e),
            }
