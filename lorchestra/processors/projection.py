"""Projection processors for lorchestra.

This module provides processors for all projection pipelines:

VIEW PROJECTIONS (canonical → BQ views):

1. CreateProjectionProcessor (job_type: create_projection)
   - Creates/updates BigQuery views from SQL defined in lorchestra/sql/projections.py
   - Views extract and flatten data from canonical_objects table
   - Run once initially, then whenever projection SQL changes

TABLE PROJECTIONS (source → BQ tables via MERGE upsert):

2. MeasurementEventProjection (job_type: project_measurement_events)
   - Projects canonical form_responses → measurement_events table
   - 1 row per form submission (FHIR MeasurementEvent grain)
   - Incremental: only processes new/updated canonical records

3. ObservationProjection (job_type: project_observations)
   - Projects measurement_events → observations table
   - Uses final-form for clinical instrument scoring
   - 1 row per scored construct with components JSON
   - Incremental: only processes measurement_events without observations

LOCAL PROJECTIONS (BQ → local storage):

4. SyncSqliteProcessor (job_type: sync_sqlite)
   - Syncs BQ projection view to local SQLite table
   - Uses full replace strategy (DELETE + INSERT)
   - Run daily to refresh local data

5. FileProjectionProcessor (job_type: file_projection)
   - Queries SQLite and renders results to markdown files
   - Uses path_template for file locations
   - Uses content_template for file content

Pipeline flows:
    canonical_objects → BQ Views (proj_*)           [create_projection]
    canonical_objects → measurement_events table    [project_measurement_events]
    measurement_events → observations table         [project_observations]
    BQ views/tables → SQLite tables                 [sync_sqlite]
    SQLite → Markdown files                         [file_projection]

See docs/projection-pipeline.md for full documentation.
"""

import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from lorchestra.processors import registry
from lorchestra.processors.base import EventClient, JobContext, StorageClient
from lorchestra.sql.projections import get_projection_sql

logger = logging.getLogger(__name__)


class CreateProjectionProcessor:
    """Create/update BQ projection views from hardcoded SQL."""

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute the create_projection job.

        Args:
            job_spec: Job specification with projection.name
            context: Execution context
            storage_client: Storage operations interface
            event_client: Event emission interface
        """
        proj_name = job_spec["projection"]["name"]
        source_system = job_spec.get("source_system", "lorchestra")

        # Get project and dataset from context config
        project = context.config.project
        dataset = context.config.dataset_canonical

        # Log start event
        event_client.log_event(
            event_type="projection.started",
            source_system=source_system,
            correlation_id=context.run_id,
            status="success",
            payload={
                "projection_name": proj_name,
                "project": project,
                "dataset": dataset,
            },
        )

        try:
            # Get SQL and execute
            sql = get_projection_sql(proj_name, project, dataset)

            if context.dry_run:
                # Log what would happen
                event_client.log_event(
                    event_type="projection.dry_run",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={
                        "projection_name": proj_name,
                        "sql_preview": sql[:500],
                    },
                )
            else:
                # Execute the CREATE VIEW SQL
                result = storage_client.execute_sql(sql)

                # Log completion event
                event_client.log_event(
                    event_type="projection.completed",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={
                        "projection_name": proj_name,
                        "rows_affected": result.get("rows_affected", 0),
                    },
                )

        except Exception as e:
            # Log failure event
            event_client.log_event(
                event_type="projection.failed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "projection_name": proj_name,
                },
            )
            raise


class SyncSqliteProcessor:
    """Sync BQ projection to local SQLite (full replace)."""

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute the sync_sqlite job.

        Args:
            job_spec: Job specification with source.projection, sink.sqlite_path, sink.table
                      Optional source.dataset to override default (for tables in derived dataset)
            context: Execution context
            storage_client: Storage operations interface
            event_client: Event emission interface
        """
        source_config = job_spec["source"]
        proj_name = source_config["projection"]
        sqlite_path = Path(job_spec["sink"]["sqlite_path"]).expanduser()
        table = job_spec["sink"]["table"]
        source_system = job_spec.get("source_system", "lorchestra")

        # Get project and dataset from context config (with optional override)
        project = context.config.project
        dataset_override = source_config.get("dataset")
        if dataset_override == "derived":
            dataset = context.config.dataset_derived
        elif dataset_override:
            dataset = dataset_override
        else:
            dataset = context.config.dataset_canonical

        # Log start event
        event_client.log_event(
            event_type="sync.started",
            source_system=source_system,
            correlation_id=context.run_id,
            status="success",
            payload={
                "projection_name": proj_name,
                "sqlite_path": str(sqlite_path),
                "table": table,
            },
        )

        try:
            # Build the fully qualified view name
            view_name = f"`{project}.{dataset}.{proj_name}`"

            if context.dry_run:
                # Log what would happen
                event_client.log_event(
                    event_type="sync.dry_run",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={
                        "projection_name": proj_name,
                        "sqlite_path": str(sqlite_path),
                        "table": table,
                    },
                )
                return

            # Capture projection time BEFORE any processing
            projection_time = datetime.now(timezone.utc).isoformat()

            # 1. Query BQ projection
            rows = storage_client.query_to_dataframe(f"SELECT * FROM {view_name}")

            if not rows:
                event_client.log_event(
                    event_type="sync.completed",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={"rows": 0, "table": table},
                )
                return

            # 2. Ensure SQLite directory exists
            sqlite_path.parent.mkdir(parents=True, exist_ok=True)

            # 3. Write to SQLite (DROP + CREATE + INSERT for full replace)
            conn = sqlite3.connect(sqlite_path)
            try:
                # Get column names from first row, add projected_at
                columns = list(rows[0].keys()) + ["projected_at"]

                # Drop table if exists (ensures schema is always correct)
                conn.execute(f'DROP TABLE IF EXISTS "{table}"')

                # Create table with current schema
                col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
                conn.execute(f'CREATE TABLE "{table}" ({col_defs})')

                # Bulk insert rows
                placeholders = ", ".join("?" * len(columns))
                col_names = ", ".join(f'"{c}"' for c in columns)
                insert_sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'

                for row in rows:
                    # Build values for original columns, then append projected_at
                    values = [
                        str(row.get(col)) if row.get(col) is not None else None
                        for col in columns[:-1]  # Exclude projected_at from row lookup
                    ]
                    values.append(projection_time)
                    conn.execute(insert_sql, values)

                conn.commit()
            finally:
                conn.close()

            # Log completion event
            event_client.log_event(
                event_type="sync.completed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="success",
                payload={
                    "rows": len(rows),
                    "table": table,
                    "sqlite_path": str(sqlite_path),
                },
            )

        except Exception as e:
            # Log failure event
            event_client.log_event(
                event_type="sync.failed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "projection_name": proj_name,
                    "table": table,
                },
            )
            raise


class FileProjectionProcessor:
    """Render SQLite data to markdown files."""

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute the file_projection job.

        Args:
            job_spec: Job specification with source.sqlite_path, source.query,
                      sink.base_path, sink.path_template, sink.content_template
            context: Execution context
            storage_client: Storage operations interface (not used directly)
            event_client: Event emission interface
        """
        sqlite_path = Path(job_spec["source"]["sqlite_path"]).expanduser()
        query = job_spec["source"]["query"]
        base_path = Path(job_spec["sink"]["base_path"]).expanduser()
        path_template = job_spec["sink"]["path_template"]
        content_template = job_spec["sink"]["content_template"]
        source_system = job_spec.get("source_system", "lorchestra")

        # Log start event
        event_client.log_event(
            event_type="file_projection.started",
            source_system=source_system,
            correlation_id=context.run_id,
            status="success",
            payload={
                "sqlite_path": str(sqlite_path),
                "base_path": str(base_path),
                "query": query[:100],
            },
        )

        try:
            if context.dry_run:
                # Log what would happen
                event_client.log_event(
                    event_type="file_projection.dry_run",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={
                        "sqlite_path": str(sqlite_path),
                        "base_path": str(base_path),
                    },
                )
                return

            # Query SQLite
            conn = sqlite3.connect(sqlite_path)
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute(query)
                rows = [dict(row) for row in cursor.fetchall()]
            finally:
                conn.close()

            if not rows:
                event_client.log_event(
                    event_type="file_projection.completed",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={"files": 0},
                )
                return

            # Capture projection time for all files in this run
            projection_time = datetime.now(timezone.utc).isoformat()

            # Get optional front_matter config
            front_matter_spec = job_spec["sink"].get("front_matter")

            # Render files
            files_written = 0
            for row in rows:
                # Inject _projected_at into row data for template substitution
                row_with_meta = {**row, "_projected_at": projection_time}

                # Build file path from template
                file_path = base_path / path_template.format(**row_with_meta)
                file_path.parent.mkdir(parents=True, exist_ok=True)

                # Render content body from template
                content_body = content_template.format(**row_with_meta)

                # Build front matter if configured
                if front_matter_spec:
                    # Resolve placeholders in front matter values
                    resolved = {}
                    for key, value in front_matter_spec.items():
                        if isinstance(value, str):
                            try:
                                resolved[key] = value.format(**row_with_meta)
                            except KeyError as exc:
                                raise RuntimeError(
                                    f"Missing key {exc!s} in front_matter for job "
                                    f"'{job_spec.get('job_id', 'unknown')}'. "
                                    f"Available keys: {list(row_with_meta.keys())}"
                                ) from exc
                        else:
                            resolved[key] = value

                    # Use yaml.safe_dump for correct YAML output
                    front_matter_yaml = yaml.safe_dump(
                        resolved, sort_keys=False, allow_unicode=True
                    )
                    content = f"---\n{front_matter_yaml}---\n\n{content_body}"
                else:
                    content = content_body

                file_path.write_text(content)
                files_written += 1

            # Log completion event
            event_client.log_event(
                event_type="file_projection.completed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="success",
                payload={
                    "files": files_written,
                    "base_path": str(base_path),
                },
            )

        except Exception as e:
            # Log failure event
            event_client.log_event(
                event_type="file_projection.failed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "sqlite_path": str(sqlite_path),
                    "base_path": str(base_path),
                },
            )
            raise


# =============================================================================
# TABLE PROJECTION PROCESSORS
# =============================================================================

# Default registry paths for final-form
DEFAULT_MEASURE_REGISTRY = Path("/workspace/finalform/measure-registry")
DEFAULT_BINDING_REGISTRY = Path("/workspace/finalform/form-binding-registry")


class MeasurementEventProjection:
    """Project canonical form_responses → measurement_events table.

    This is "canonization v2" for measurement data:
    1. Reads canonical form_response objects (incremental)
    2. Extracts and categorizes them by binding type
    3. Wraps in FHIR-adjacent MeasurementEvent metadata
    4. Upserts to measurement_events table

    The measurement_event is the raw record that a measurement occurred.
    Observations (scored results) are derived by a separate projection job.
    """

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute a measurement_event projection job."""
        job_id = job_spec["job_id"]
        source = job_spec["source"]
        sink = job_spec.get("sink", {})
        options = job_spec.get("options", {})
        events_config = job_spec.get("events", {})

        # Source config
        source_filter = source.get("filter", {})
        canonical_schema = source_filter.get("canonical_schema")

        # Sink config
        measurement_table = sink.get("table", "measurement_events")
        event_type = sink.get("event_type", "form")
        event_subtype = sink.get("event_subtype")  # binding_id

        # Options
        limit = options.get("limit")

        # Event names
        on_started = events_config.get("on_started", "projection.measurement_events.started")
        on_complete = events_config.get("on_complete", "projection.measurement_events.completed")
        on_fail = events_config.get("on_fail", "projection.measurement_events.failed")

        logger.info(f"Starting measurement_event projection: {job_id}")
        logger.info(f"  event_type: {event_type}, event_subtype: {event_subtype}")
        if context.dry_run:
            logger.info("  DRY RUN - no writes will occur")

        start_time = time.time()

        event_client.log_event(
            event_type=on_started,
            source_system="projection",
            target_object_type="measurement_event",
            correlation_id=context.run_id,
            status="success",
            payload={"job_id": job_id, "event_subtype": event_subtype},
        )

        try:
            # Query canonical objects (incremental - skip already processed)
            filters = {k: v for k, v in source_filter.items() if k != "canonical_schema"}
            records = list(
                storage_client.query_canonical_for_formation(
                    canonical_schema=canonical_schema,
                    filters=filters,
                    measurement_table=measurement_table,
                    limit=limit,
                )
            )

            logger.info(f"Found {len(records)} canonical records to process")

            if not records:
                logger.info("No records to process")
                return

            # Create measurement_events
            measurement_rows = []
            failed_records = []

            for i, record in enumerate(records):
                try:
                    me_row = self._canonical_to_measurement_event(
                        record=record,
                        event_type=event_type,
                        event_subtype=event_subtype,
                        correlation_id=record.get("correlation_id") or context.run_id,
                    )
                    measurement_rows.append(me_row)

                except Exception as e:
                    failed_records.append({"idem_key": record["idem_key"], "error": str(e)})
                    if len(failed_records) <= 3:
                        logger.warning(f"FAILED: {record['idem_key'][:50]}... Error: {e}")

                if (i + 1) % 100 == 0:
                    logger.info(f"Processed {i + 1}/{len(records)}...")

            logger.info(f"Results: {len(measurement_rows)} measurement_events, {len(failed_records)} failed")

            # Upsert measurement_events
            measurements_upserted = 0
            if not context.dry_run and measurement_rows:
                measurements_upserted = storage_client.upsert_measurements(
                    measurements=measurement_rows,
                    table=measurement_table,
                    correlation_id=context.run_id,
                )
                logger.info(f"Upserted {measurements_upserted} measurement events")
            elif context.dry_run:
                logger.info(f"[DRY-RUN] Would upsert {len(measurement_rows)} measurement events")

            duration_seconds = time.time() - start_time

            event_client.log_event(
                event_type=on_complete,
                source_system="projection",
                target_object_type="measurement_event",
                correlation_id=context.run_id,
                status="success",
                payload={
                    "job_id": job_id,
                    "records_processed": len(records),
                    "measurements_created": len(measurement_rows),
                    "measurements_upserted": measurements_upserted,
                    "records_failed": len(failed_records),
                    "duration_seconds": round(duration_seconds, 2),
                    "dry_run": context.dry_run,
                },
            )

            logger.info(f"Projection complete: {len(measurement_rows)} events, duration={duration_seconds:.2f}s")

        except Exception as e:
            duration_seconds = time.time() - start_time
            logger.error(f"Measurement event projection failed: {e}", exc_info=True)

            event_client.log_event(
                event_type=on_fail,
                source_system="projection",
                target_object_type="measurement_event",
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "job_id": job_id,
                    "error_type": type(e).__name__,
                    "duration_seconds": round(duration_seconds, 2),
                },
            )
            raise

    def _canonical_to_measurement_event(
        self,
        record: dict[str, Any],
        event_type: str,
        event_subtype: str | None,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Create a measurement_event row from a canonical form_response."""
        payload = record.get("payload", {})

        # Use canonical idem_key as measurement_event_id (1:1 mapping)
        idem_key = record["idem_key"]

        # Get subject_id from respondent
        respondent = payload.get("respondent", {})
        subject_id = respondent.get("id") or respondent.get("email")

        # Get form metadata
        form_id = payload.get("form_id")
        form_submission_id = payload.get("response_id") or payload.get("submission_id")
        occurred_at = payload.get("submitted_at")

        now = datetime.now(timezone.utc).isoformat()

        return {
            "idem_key": idem_key,
            "measurement_event_id": idem_key,
            "subject_id": subject_id,
            "subject_contact_id": None,
            "event_type": event_type,
            "event_subtype": event_subtype,
            "occurred_at": occurred_at,
            "received_at": now,
            "source_system": "google_forms",
            "source_entity": "form_response",
            "source_id": form_submission_id,
            "canonical_object_id": idem_key,
            "form_id": form_id,
            "binding_id": event_subtype,  # event_subtype is the binding_id
            "binding_version": None,  # Set by scoring job
            "metadata": json.dumps({}),
            "correlation_id": correlation_id,
            "processed_at": now,
            "created_at": now,
        }


class ObservationProjection:
    """Project measurement_events → observations table.

    This processor:
    1. Reads measurement_events that don't have observations yet (incremental)
    2. Looks up canonical payload via canonical_object_id
    3. Runs final-form scoring pipeline
    4. Creates observations (1 per scored measure, with components JSON)

    This is the scoring/formation step that derives clinical scores from raw form data.
    """

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute an observation projection job."""
        # Import final-form here to avoid import errors if not installed
        from finalform.pipeline import Pipeline, PipelineConfig, ProcessingResult  # noqa: F401

        job_id = job_spec["job_id"]
        source = job_spec["source"]
        transform_config = job_spec.get("transform", {})
        sink = job_spec.get("sink", {})
        options = job_spec.get("options", {})
        events_config = job_spec.get("events", {})

        # Source config - filter measurement_events
        source_filter = source.get("filter", {})
        binding_id_filter = source_filter.get("binding_id")

        # Transform config
        binding_id = transform_config.get("binding_id")
        binding_version = transform_config.get("binding_version")

        # Resolve registry paths
        formation_root = context.config.formation_registry_root
        if formation_root:
            root_path = Path(formation_root).expanduser()
            default_measure = root_path / "measure-registry"
            default_binding = root_path / "form-binding-registry"
        else:
            default_measure = DEFAULT_MEASURE_REGISTRY
            default_binding = DEFAULT_BINDING_REGISTRY

        measure_registry_path = Path(
            transform_config.get("measure_registry_path", default_measure)
        ).expanduser()
        binding_registry_path = Path(
            transform_config.get("binding_registry_path", default_binding)
        ).expanduser()

        # Sink config
        measurement_table = sink.get("measurement_table", "measurement_events")
        observation_table = sink.get("table", sink.get("observation_table", "observations"))

        # Options
        limit = options.get("limit")

        # Event names
        on_started = events_config.get("on_started", "projection.observations.started")
        on_complete = events_config.get("on_complete", "projection.observations.completed")
        on_fail = events_config.get("on_fail", "projection.observations.failed")

        logger.info(f"Starting observation projection: {job_id}")
        logger.info(f"  binding: {binding_id}@{binding_version or 'latest'}")
        if context.dry_run:
            logger.info("  DRY RUN - no writes will occur")

        start_time = time.time()

        event_client.log_event(
            event_type=on_started,
            source_system="projection",
            target_object_type="observation",
            correlation_id=context.run_id,
            status="success",
            payload={"job_id": job_id, "binding_id": binding_id},
        )

        try:
            # Create the final-form Pipeline
            config = PipelineConfig(
                measure_registry_path=measure_registry_path,
                binding_registry_path=binding_registry_path,
                binding_id=binding_id,
                binding_version=binding_version,
            )
            pipeline = Pipeline(config)

            loaded_binding_id = pipeline.binding_spec.binding_id
            loaded_binding_version = pipeline.binding_spec.version

            # Query measurement_events that need scoring (incremental)
            records = list(
                storage_client.query_measurement_events_for_scoring(
                    measurement_table=measurement_table,
                    observation_table=observation_table,
                    binding_id=binding_id_filter or binding_id,
                    limit=limit,
                )
            )

            logger.info(f"Found {len(records)} measurement_events to score")

            if not records:
                logger.info("No records to score")
                return

            # Score each measurement_event
            observation_rows = []
            failed_records = []
            total_diagnostics_errors = 0
            total_diagnostics_warnings = 0

            for i, me_record in enumerate(records):
                try:
                    # Look up canonical payload
                    canonical_object_id = me_record["canonical_object_id"]
                    canonical_record = storage_client.get_canonical_by_idem_key(canonical_object_id)

                    if not canonical_record:
                        raise ValueError(f"Canonical object not found: {canonical_object_id}")

                    payload = canonical_record.get("payload", {})

                    # Build form_response for final-form
                    form_response = self._build_form_response(canonical_record, payload)

                    # Run scoring
                    result: ProcessingResult = pipeline.process(form_response)

                    # Track diagnostics
                    if result.diagnostics:
                        if hasattr(result.diagnostics, "errors"):
                            total_diagnostics_errors += len(result.diagnostics.errors)
                        if hasattr(result.diagnostics, "warnings"):
                            total_diagnostics_warnings += len(result.diagnostics.warnings)

                    # Create observations from scored results
                    correlation_id = me_record.get("correlation_id") or context.run_id

                    for event in result.events:
                        obs_row = self._measure_to_observation(
                            event=event,
                            measurement_event_id=me_record["measurement_event_id"],
                            correlation_id=correlation_id,
                        )
                        observation_rows.append(obs_row)

                except Exception as e:
                    failed_records.append({
                        "measurement_event_id": me_record["measurement_event_id"],
                        "error": str(e),
                    })
                    if len(failed_records) <= 3:
                        logger.warning(f"FAILED: {me_record['measurement_event_id'][:50]}... Error: {e}")

                if (i + 1) % 100 == 0:
                    logger.info(f"Scored {i + 1}/{len(records)}...")

            logger.info(f"Results: {len(observation_rows)} observations, {len(failed_records)} failed")

            # Upsert observations
            observations_upserted = 0
            if not context.dry_run and observation_rows:
                observations_upserted = storage_client.upsert_observations(
                    observations=observation_rows,
                    table=observation_table,
                    correlation_id=context.run_id,
                )
                logger.info(f"Upserted {observations_upserted} observations")
            elif context.dry_run:
                logger.info(f"[DRY-RUN] Would upsert {len(observation_rows)} observations")

            duration_seconds = time.time() - start_time

            event_client.log_event(
                event_type=on_complete,
                source_system="projection",
                target_object_type="observation",
                correlation_id=context.run_id,
                status="success",
                payload={
                    "job_id": job_id,
                    "binding_id": loaded_binding_id,
                    "binding_version": loaded_binding_version,
                    "records_scored": len(records),
                    "observations_created": len(observation_rows),
                    "observations_upserted": observations_upserted,
                    "records_failed": len(failed_records),
                    "duration_seconds": round(duration_seconds, 2),
                    "diagnostics": {
                        "errors": total_diagnostics_errors,
                        "warnings": total_diagnostics_warnings,
                    },
                    "dry_run": context.dry_run,
                },
            )

            logger.info(f"Projection complete: {len(observation_rows)} observations, duration={duration_seconds:.2f}s")

        except Exception as e:
            duration_seconds = time.time() - start_time
            logger.error(f"Observation projection failed: {e}", exc_info=True)

            event_client.log_event(
                event_type=on_fail,
                source_system="projection",
                target_object_type="observation",
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "job_id": job_id,
                    "binding_id": binding_id,
                    "error_type": type(e).__name__,
                    "duration_seconds": round(duration_seconds, 2),
                },
            )
            raise

    def _build_form_response(
        self,
        record: dict[str, Any],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Map canonical form_response → final-form input format."""
        answers_data = payload.get("answers", [])
        items = []
        for i, ans in enumerate(answers_data):
            items.append({
                "field_key": ans.get("question_id"),
                "answer": ans.get("answer_value"),
                "position": i,
            })

        connection_name = record.get("connection_name", "")
        form_id = payload.get("form_id") or f"googleforms::{connection_name}"

        respondent = payload.get("respondent", {})
        subject_id = respondent.get("id") or respondent.get("email")

        return {
            "form_id": form_id,
            "form_submission_id": payload.get("response_id") or payload.get("submission_id"),
            "subject_id": subject_id,
            "timestamp": payload.get("submitted_at"),
            "items": items,
        }

    def _measure_to_observation(
        self,
        event: Any,  # MeasurementEvent from final-form
        measurement_event_id: str,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Create an observation row from a scored MeasurementEvent."""
        # Build idempotent key
        idem_key = f"{measurement_event_id}:{event.measure_id}"

        # Build components array
        components = []
        for obs in event.observations:
            components.append({
                "code": obs.code,
                "kind": obs.kind,
                "value": obs.value,
                "value_type": obs.value_type,
                "label": obs.label,
                "raw_answer": obs.raw_answer,
                "position": obs.position,
                "missing": obs.missing,
            })

        # Extract total score and severity
        total_score = self._extract_total_score(event)
        severity_label = self._extract_severity(event)
        severity_code = self._severity_label_to_code(severity_label) if severity_label else None

        now = datetime.now(timezone.utc).isoformat()

        return {
            "idem_key": idem_key,
            "observation_id": event.measurement_event_id,
            "measurement_event_id": measurement_event_id,
            "subject_id": event.subject_id,
            "measure_code": event.measure_id,
            "measure_version": event.measure_version,
            "value_numeric": total_score,
            "value_text": None,
            "unit": "score",
            "severity_code": severity_code,
            "severity_label": severity_label,
            "components": json.dumps(components),
            "metadata": json.dumps({}),
            "correlation_id": correlation_id,
            "processed_at": now,
            "created_at": now,
        }

    def _extract_total_score(self, event: Any) -> float | None:
        """Extract the total score from a MeasurementEvent."""
        for obs in event.observations:
            if obs.kind == "scale" and obs.code.endswith("_total"):
                if obs.value is not None:
                    try:
                        return float(obs.value)
                    except (TypeError, ValueError):
                        pass
        return None

    def _extract_severity(self, event: Any) -> str | None:
        """Extract severity label from a MeasurementEvent."""
        for obs in event.observations:
            if obs.kind == "scale" and obs.code.endswith("_total"):
                if obs.label:
                    return obs.label
        return None

    def _severity_label_to_code(self, label: str) -> str | None:
        """Map severity label to standardized code."""
        if not label:
            return None
        label_lower = label.lower()
        if "none" in label_lower or "minimal" in label_lower:
            return "none"
        elif "mild" in label_lower:
            return "mild"
        elif "moderate" in label_lower:
            if "severe" in label_lower:
                return "moderate_severe"
            return "moderate"
        elif "severe" in label_lower:
            return "severe"
        return None


# =============================================================================
# CROSS-PROJECT SYNC PROCESSOR
# =============================================================================


class CrossProjectSyncProcessor:
    """Sync curated data to another GCP project via CREATE OR REPLACE TABLE.

    Runs a curation query against local-orchestration and writes results
    to a table in another project (e.g., molt-chatbot). Uses fully-qualified
    cross-project table names — BQ handles cross-project references when the
    service account has bigquery.dataEditor on the target dataset.

    Job spec:
    {
        "job_id": "sync_molt_emails",
        "job_type": "sync_bq_cross_project",
        "source": {
            "query_name": "context_emails"   // looked up from molt_projections.py
        },
        "sink": {
            "project": "molt-chatbot",
            "dataset": "molt",
            "table": "context_emails"
        }
    }
    """

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute a cross-project sync job."""
        from lorchestra.sql.molt_projections import get_molt_projection_sql

        job_id = job_spec.get("job_id", "unknown")
        source_config = job_spec["source"]
        sink_config = job_spec["sink"]

        sink_project = sink_config["project"]
        sink_dataset = sink_config["dataset"]
        sink_table = sink_config["table"]
        fq_table = f"`{sink_project}.{sink_dataset}.{sink_table}`"

        # Get source SQL — either inline or from named query
        if "sql" in source_config:
            source_sql = source_config["sql"]
        elif "query_name" in source_config:
            source_sql = get_molt_projection_sql(
                name=source_config["query_name"],
                project=context.config.project,
                dataset=context.config.dataset_canonical,
            )
        else:
            raise ValueError(
                f"Job {job_id}: source must have 'sql' or 'query_name'"
            )

        # Emit started event
        event_client.log_event(
            event_type="projection.cross_project.started",
            source_system="lorchestra",
            correlation_id=context.run_id,
            status="success",
            payload={
                "job_id": job_id,
                "sink": f"{sink_project}.{sink_dataset}.{sink_table}",
            },
        )

        try:
            # Build the CTAS statement
            ctas = f"CREATE OR REPLACE TABLE {fq_table} AS\n{source_sql}"

            if context.dry_run:
                logger.info(
                    f"[DRY-RUN] Would execute cross-project sync: {job_id}"
                )
                logger.info(f"  Target: {fq_table}")
                logger.info(f"  SQL preview: {source_sql[:200]}...")

                event_client.log_event(
                    event_type="projection.cross_project.dry_run",
                    source_system="lorchestra",
                    correlation_id=context.run_id,
                    status="success",
                    payload={
                        "job_id": job_id,
                        "sink": f"{sink_project}.{sink_dataset}.{sink_table}",
                        "sql_preview": source_sql[:500],
                    },
                )
                return

            # Execute the cross-project CTAS
            result = storage_client.execute_sql(ctas)

            event_client.log_event(
                event_type="projection.cross_project.completed",
                source_system="lorchestra",
                correlation_id=context.run_id,
                status="success",
                payload={
                    "job_id": job_id,
                    "sink": f"{sink_project}.{sink_dataset}.{sink_table}",
                    "rows_affected": result.get("rows_affected", 0),
                    "total_rows": result.get("total_rows", 0),
                },
            )

            logger.info(
                f"Cross-project sync complete: {job_id} → {fq_table} "
                f"({result.get('total_rows', 0)} rows)"
            )

        except Exception as e:
            logger.error(f"Cross-project sync failed: {job_id} - {e}", exc_info=True)

            event_client.log_event(
                event_type="projection.cross_project.failed",
                source_system="lorchestra",
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "job_id": job_id,
                    "sink": f"{sink_project}.{sink_dataset}.{sink_table}",
                    "error_type": type(e).__name__,
                },
            )
            raise


# =============================================================================
# REGISTER ALL PROCESSORS
# =============================================================================

# View projections
registry.register("create_projection", CreateProjectionProcessor())

# Table projections
registry.register("project_measurement_events", MeasurementEventProjection())
registry.register("project_observations", ObservationProjection())

# Local projections
registry.register("sync_sqlite", SyncSqliteProcessor())
registry.register("file_projection", FileProjectionProcessor())

# Cross-project projections
registry.register("sync_bq_cross_project", CrossProjectSyncProcessor())
