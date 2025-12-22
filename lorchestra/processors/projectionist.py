"""Projectionist processor: orchestrator glue for projectionist projections.

This is a thin glue layer that:
1. Builds ProjectionContext from job spec
2. Instantiates projection by name
3. Constructs services (BQ adapter + sheets via factory loader)
4. Calls plan() → apply()
5. Logs events

NO SQL building, NO column logic, NO sheet formatting, NO retries, NO auth.
All semantics live in projectionist projections.

Service construction note:
The sheets service is loaded via load_service_factory() from job_runner.
This enforces clean dependency boundaries - lorchestra never imports gorch.
Factory path and args come from job spec, not hardcoded values.
"""

import logging
from typing import Any, Iterator

from projectionist import ProjectionResult
from projectionist.context import ProjectionContext
from projectionist.projections.sheets import SheetsProjection

from lorchestra.job_runner import load_service_factory
from lorchestra.processors import registry
from lorchestra.processors.base import EventClient, JobContext, StorageClient

logger = logging.getLogger(__name__)


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


class ProjectionServicesImpl:
    """Concrete implementation of ProjectionServices for lorchestra.

    Provides bq (query) and sheets (write) capabilities to projections.
    Sheets service is duck-typed (write_table protocol) - no storacle import.
    """

    def __init__(
        self,
        bq_service: BqQueryServiceAdapter,
        sheets_service: Any,  # Duck-typed: must have write_table() method
    ):
        self._bq = bq_service
        self._sheets = sheets_service

    @property
    def bq(self) -> BqQueryServiceAdapter:
        return self._bq

    @property
    def sheets(self) -> Any:
        return self._sheets


# =============================================================================
# PROJECTION REGISTRY
# =============================================================================

# Maps projection_name → projection class
# Add new projections here as they're created
PROJECTION_REGISTRY: dict[str, type] = {
    "sheets": SheetsProjection,
}


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


class ProjectionistProcessor:
    """Run projectionist projections based on job JSON.

    Job JSON schema:
    {
        "job_id": "proj_sheets_clients",
        "job_type": "projectionist",
        "projection_name": "sheets",
        "sheets_service_factory": "gorch.sheets.factories:build_sheets_write_service",
        "sheets_service_factory_args": {"account": "gdrive"},
        "projection_config": {
            "query": "SELECT * FROM `${PROJECT}.${DATASET_CANONICAL}.view_name`",
            "spreadsheet_id": "...",
            "sheet_name": "clients",
            "strategy": "replace"
        }
    }

    Service factories are loaded via load_service_factory() with allowlist.
    Placeholders ${PROJECT}, ${DATASET_CANONICAL}, etc. are resolved at runtime.
    This processor is intentionally dumb - all logic lives in projectionist.
    """

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute a projectionist job."""
        job_id = job_spec.get("job_id", "unknown")
        projection_name = job_spec["projection_name"]
        projection_config_raw = job_spec["projection_config"]
        source_system = job_spec.get("source_system", "lorchestra")

        # Resolve placeholders in config using lorchestra config values
        projection_config = _resolve_placeholders(projection_config_raw, context.config)

        # Look up projection class
        projection_cls = PROJECTION_REGISTRY.get(projection_name)
        if projection_cls is None:
            raise ValueError(
                f"Unknown projection: {projection_name}. "
                f"Registered: {list(PROJECTION_REGISTRY.keys())}"
            )

        # Build services - sheets service loaded via factory from job spec
        bq_service = BqQueryServiceAdapter(storage_client)

        # Load sheets service factory from job spec (enforces allowlist)
        sheets_factory_path = job_spec["sheets_service_factory"]
        sheets_factory_args = job_spec.get("sheets_service_factory_args", {})
        sheets_factory = load_service_factory(sheets_factory_path)
        sheets_service = sheets_factory(**sheets_factory_args)

        services = ProjectionServicesImpl(bq_service, sheets_service)

        # Build context
        ctx = ProjectionContext(
            dry_run=context.dry_run,
            run_id=context.run_id,
            config=projection_config,
        )

        # Instantiate projection
        projection = projection_cls()

        # Step 1: Plan (always runs, even in dry_run)
        plan = projection.plan(services, ctx)

        # Log started (after plan so we have plan_id)
        event_client.log_event(
            event_type="sheets_projection.started",
            source_system=source_system,
            correlation_id=context.run_id,
            status="success",
            payload={
                "job_id": job_id,
                "projection_name": projection_name,
                "plan_id": plan.plan_id,
                "spreadsheet_id": projection_config.get("spreadsheet_id"),
                "sheet_name": projection_config.get("sheet_name"),
                "dry_run": context.dry_run,
            },
        )

        try:
            # Step 2: Apply
            result: ProjectionResult = projection.apply(plan, services, ctx)

            # Log completed
            event_client.log_event(
                event_type="sheets_projection.completed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="success",
                payload={
                    "job_id": job_id,
                    "projection_name": result.projection_name,
                    "plan_id": result.plan_id,
                    "rows_affected": result.rows_affected,
                    "duration_seconds": result.duration_seconds,
                    "dry_run": result.dry_run,
                },
            )

            logger.info(
                f"Projection complete: {projection_name} "
                f"plan_id={result.plan_id[:12]}... "
                f"rows={result.rows_affected} "
                f"dry_run={result.dry_run}"
            )

        except Exception as e:
            # Log failed
            event_client.log_event(
                event_type="sheets_projection.failed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "job_id": job_id,
                    "projection_name": projection_name,
                    "plan_id": plan.plan_id,
                },
            )
            raise


# Register the processor
registry.register("projectionist", ProjectionistProcessor())
