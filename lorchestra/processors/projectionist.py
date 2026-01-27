"""Projectionist processor: orchestrator glue for projectionist projections.

This is a thin glue layer that:
1. Builds ProjectionContext from job spec
2. Calls projectionist.build_plan() to get a storacle.plan/1.0.0 plan dict
3. Calls storacle.rpc.execute_plan() to execute the plan
4. Logs events

v3 ARCHITECTURE:
- Projectionist emits plans (dict with ops) - no I/O
- Storacle executes plans via RPC - handles auth and vendor calls
- Lorchestra orchestrates (dry_run, events) - treats plan AND responses as opaque

Hard constraints (from spec):
- Lorchestra must NOT branch on ops[*].method (treat plan as opaque)
- Lorchestra must NOT inspect response result schema (treat responses as opaque)
- Lorchestra must NOT call SheetsWriteService or any Sheets adapter directly
- Lorchestra imports storacle.rpc for execute_plan only
"""

import logging
from typing import Any, Callable, Iterator

from lorchestra.processors import registry
from lorchestra.processors.base import EventClient, JobContext, StorageClient

logger = logging.getLogger(__name__)

# Lazy imports for external dependencies - may not be installed
# These are imported lazily at runtime via _import_* functions below


def _import_projectionist():
    """Lazily import projectionist dependencies."""
    from projectionist.context import ProjectionContext
    from projectionist.projections.sheets import build_plan as sheets_build_plan
    return ProjectionContext, sheets_build_plan


def _import_storacle_rpc():
    """Lazily import storacle.rpc.execute_plan."""
    from storacle.rpc import execute_plan
    return execute_plan


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

# Type alias for build_plan functions
# Signature: (ctx: ProjectionContext, config: dict, *, bq: BqQueryService) -> dict
BuildPlanFn = Callable[..., dict[str, Any]]


# =============================================================================
# PROJECTION BUILD_PLAN REGISTRY
# =============================================================================

# Maps projection_name → build_plan function
# Each function must have signature: (ctx, config, *, bq) -> plan dict
# Note: Registry is populated lazily when projectionist module is imported
PROJECTION_BUILD_PLAN_REGISTRY: dict[str, BuildPlanFn] = {}


def _get_build_plan_registry() -> dict[str, BuildPlanFn]:
    """Get the build plan registry, populating it lazily."""
    if not PROJECTION_BUILD_PLAN_REGISTRY:
        try:
            _, sheets_build_plan = _import_projectionist()
            PROJECTION_BUILD_PLAN_REGISTRY["sheets"] = sheets_build_plan
        except ImportError:
            pass  # projectionist not installed
    return PROJECTION_BUILD_PLAN_REGISTRY


# =============================================================================
# SERVICE ADAPTERS
# =============================================================================


class BqQueryServiceAdapter:
    """Adapter wrapping StorageClient as projectionist BqQueryService protocol.

    Uses exactly one StorageClient method: query_to_dataframe().
    """

    def __init__(self, storage_client: StorageClient):
        self._storage = storage_client

    def query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        """Execute SQL and yield rows as dicts.

        Note: params are not currently supported by query_to_dataframe().
        """
        rows = self._storage.query_to_dataframe(sql)
        yield from rows


# =============================================================================
# PROCESSOR
# =============================================================================


def _resolve_placeholders(value: Any, config: Any) -> Any:
    """Resolve ${PLACEHOLDER} in string values using lorchestra config.

    Supports:
    - ${PROJECT}
    - ${DATASET_CANONICAL}
    - ${DATASET_RAW}
    - ${DATASET_DERIVED}

    Recursively processes dicts and lists.
    """
    if isinstance(value, str):
        value = value.replace("${PROJECT}", config.project)
        value = value.replace("${DATASET_CANONICAL}", config.dataset_canonical)
        value = value.replace("${DATASET_RAW}", config.dataset_raw)
        value = value.replace("${DATASET_DERIVED}", config.dataset_derived)
        return value
    elif isinstance(value, dict):
        return {k: _resolve_placeholders(v, config) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_placeholders(v, config) for v in value]
    else:
        return value


def _has_any_error(responses: list[dict]) -> bool:
    """Check if any response contains an error (opaque check)."""
    return any("error" in r for r in responses)


class ProjectionistProcessor:
    """Run projectionist projections using storacle RPC.

    Job JSON schema (v3 - RPC-based):
    {
        "job_id": "proj_sheets_clients",
        "job_type": "projectionist",
        "projection_name": "sheets",
        "projection_config": {
            "query": "SELECT * FROM `${PROJECT}.${DATASET_CANONICAL}.view_name`",
            "spreadsheet_id": "...",
            "sheet_name": "clients",
            "strategy": "replace",
            "account": "gdrive"
        }
    }

    Flow:
    1. Look up build_plan function from registry by projection_name
    2. Build ProjectionContext and call build_plan() to get plan dict
    3. Call storacle.rpc.execute_plan(plan, dry_run=ctx.dry_run)
    4. Log events - pass responses through opaquely
    """

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute a projectionist job via storacle RPC."""
        job_id = job_spec.get("job_id", "unknown")
        projection_name = job_spec.get("projection_name")
        projection_config_raw = job_spec["projection_config"]
        source_system = job_spec.get("source_system", "lorchestra")

        # Look up build_plan function from registry
        build_plan_registry = _get_build_plan_registry()
        build_plan_fn = build_plan_registry.get(projection_name)
        if build_plan_fn is None:
            raise ValueError(
                f"Unknown projection: {projection_name}. "
                f"Registered: {list(build_plan_registry.keys())}"
            )

        # Resolve placeholders in config using lorchestra config values
        projection_config = _resolve_placeholders(projection_config_raw, context.config)

        # Add job_id to config for plan metadata
        projection_config["job_id"] = job_id

        # Build BQ adapter for projectionist to read data
        bq_service = BqQueryServiceAdapter(storage_client)

        # Build context (lazy import)
        ProjectionContext, _ = _import_projectionist()
        ctx = ProjectionContext(
            dry_run=context.dry_run,
            run_id=context.run_id,
            config=projection_config,
        )

        # Step 1: Build plan using projectionist
        # The build_plan function is looked up from registry, not hardcoded
        plan = build_plan_fn(ctx, projection_config, bq=bq_service)

        plan_id = plan.get("plan_id", "unknown")

        # Log started (after plan so we have plan_id)
        event_client.log_event(
            event_type="projection.started",
            source_system=source_system,
            correlation_id=context.run_id,
            status="success",
            payload={
                "job_id": job_id,
                "projection_name": projection_name,
                "plan_id": plan_id,
                "dry_run": context.dry_run,
            },
        )

        try:
            # Step 2: Execute plan via storacle RPC (lazy import)
            # Lorchestra treats both plan and responses as OPAQUE
            execute_plan = _import_storacle_rpc()
            responses = execute_plan(plan, dry_run=context.dry_run)

            # Check for any errors (opaque check - just presence of 'error' key)
            if _has_any_error(responses):
                # At least one op failed - raise with count only (not inspecting details)
                error_count = sum(1 for r in responses if "error" in r)
                raise RuntimeError(
                    f"RPC execution failed: {error_count}/{len(responses)} ops returned errors"
                )

            # Log completed - pass responses through opaquely
            # We do NOT inspect result schema (no rows_written extraction)
            event_client.log_event(
                event_type="projection.completed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="success",
                payload={
                    "job_id": job_id,
                    "projection_name": projection_name,
                    "plan_id": plan_id,
                    "dry_run": context.dry_run,
                    "ops_count": len(responses),
                    "responses": responses,  # Pass through opaquely for logging
                },
            )

            logger.info(
                f"Projection complete: {projection_name} "
                f"plan_id={plan_id[:12]}... "
                f"ops={len(responses)} "
                f"dry_run={context.dry_run}"
            )

        except Exception as e:
            # Log failed
            event_client.log_event(
                event_type="projection.failed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "job_id": job_id,
                    "projection_name": projection_name,
                    "plan_id": plan_id,
                },
            )
            raise


# Register the processor
registry.register("projectionist", ProjectionistProcessor())
