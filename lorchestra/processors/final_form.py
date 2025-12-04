"""FinalFormProcessor - wraps final-form library for clinical instrument processing.

This processor handles all final_form jobs by:
1. Reading source config from job_spec (binding_id, form_id filters)
2. Querying canonical_objects for records to process (incremental - only unformed/updated)
3. Calling final-form Pipeline to process form responses (final-form does NO IO)
4. Transforming ProcessingResult → storage format
5. Upserting results to measurement_events and observations via storage_client
6. Emitting formation.completed/failed events

The final-form library is a pure transform engine - it returns results,
this processor handles all IO and event emission.
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from final_form.pipeline import Pipeline, PipelineConfig, ProcessingResult
from final_form.core.models import MeasurementEvent, Observation

from lorchestra.processors.base import (
    EventClient,
    JobContext,
    StorageClient,
    registry,
)

logger = logging.getLogger(__name__)

# Default registry paths
DEFAULT_MEASURE_REGISTRY = Path("/workspace/final-form/measure-registry")
DEFAULT_BINDING_REGISTRY = Path("/workspace/final-form/form-binding-registry")


class FinalFormProcessor:
    """Processor for final_form jobs.

    Wraps the final-form library to process canonical form_response objects through
    clinical instruments and produce measurement events and observations.
    Handles all IO and event emission.
    """

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute a final_form job.

        Args:
            job_spec: Parsed job specification with source/sink/transform config
            context: Execution context with BQ client and run_id
            storage_client: Storage operations interface
            event_client: Event emission interface
        """
        # Extract config from job_spec
        job_id = job_spec["job_id"]
        source = job_spec["source"]
        transform_config = job_spec.get("transform", {})
        sink = job_spec.get("sink", {})
        options = job_spec.get("options", {})
        events_config = job_spec.get("events", {})

        # Source config
        source_filter = source.get("filter", {})
        canonical_schema = source_filter.get("canonical_schema")
        form_id = source_filter.get("form_id")

        # Transform config
        binding_id = transform_config.get("binding_id")
        binding_version = transform_config.get("binding_version")
        measure_registry_path = Path(
            transform_config.get("measure_registry_path", DEFAULT_MEASURE_REGISTRY)
        )
        binding_registry_path = Path(
            transform_config.get("binding_registry_path", DEFAULT_BINDING_REGISTRY)
        )

        # Sink config
        measurement_table = sink.get("measurement_table", "measurement_events")
        observation_table = sink.get("observation_table", "observations")

        # Options
        limit = options.get("limit")

        # Event names (default to formation.* pattern)
        on_started = events_config.get("on_started", "formation.started")
        on_complete = events_config.get("on_complete", "formation.completed")
        on_fail = events_config.get("on_fail", "formation.failed")

        logger.info(f"Starting formation job: {job_id}")
        logger.info(f"  binding: {binding_id}@{binding_version or 'latest'}")
        logger.info(f"  form_id: {form_id}")
        if context.dry_run:
            logger.info("  DRY RUN - no writes will occur")

        start_time = time.time()

        # Emit formation.started event
        event_client.log_event(
            event_type=on_started,
            source_system="final_form",
            target_object_type="measurement",
            correlation_id=context.run_id,
            status="success",
            payload={
                "job_id": job_id,
                "binding_id": binding_id,
                "binding_version": binding_version,
            },
        )

        try:
            # Create the final-form Pipeline
            pipeline = self._create_pipeline(
                binding_id=binding_id,
                binding_version=binding_version,
                measure_registry_path=measure_registry_path,
                binding_registry_path=binding_registry_path,
            )

            # Get the loaded binding info for event logging
            loaded_binding_id = pipeline.binding_spec.binding_id
            loaded_binding_version = pipeline.binding_spec.version

            # Query canonical objects for formation (incremental)
            records = list(
                self._query_canonical_for_formation(
                    storage_client=storage_client,
                    canonical_schema=canonical_schema,
                    form_id=form_id,
                    source_filter=source_filter,
                    measurement_table=measurement_table,
                    limit=limit,
                )
            )

            logger.info(f"Found {len(records)} canonical records to process")

            if not records:
                logger.info("No records to process")
                return

            # Process each record
            measurement_rows = []
            observation_rows = []
            failed_records = []
            total_diagnostics_errors = 0
            total_diagnostics_warnings = 0

            for i, record in enumerate(records):
                try:
                    payload = record.get("payload", {})

                    # Build form_response dict for final-form
                    form_response = self._build_form_response(record, payload)

                    # Run through final-form Pipeline
                    result: ProcessingResult = pipeline.process(form_response)

                    # Track diagnostics
                    if result.diagnostics:
                        diag = result.diagnostics
                        if hasattr(diag, "errors"):
                            total_diagnostics_errors += len(diag.errors)
                        if hasattr(diag, "warnings"):
                            total_diagnostics_warnings += len(diag.warnings)

                    # Transform each MeasurementEvent to storage format
                    for event in result.events:
                        m_row = self._measurement_event_to_storage(
                            event=event,
                            canonical_idem_key=record["idem_key"],
                            correlation_id=record.get("correlation_id") or context.run_id,
                        )
                        measurement_rows.append(m_row)

                        # Transform observations for this measurement
                        for obs in event.observations:
                            o_row = self._observation_to_storage(
                                obs=obs,
                                measurement_idem_key=m_row["idem_key"],
                                correlation_id=m_row["correlation_id"],
                            )
                            observation_rows.append(o_row)

                except Exception as e:
                    failed_records.append({
                        "idem_key": record["idem_key"],
                        "error": str(e),
                    })
                    if len(failed_records) <= 3:
                        logger.warning(f"FAILED: {record['idem_key'][:50]}... Error: {e}")

                if (i + 1) % 100 == 0:
                    logger.info(f"Processed {i + 1}/{len(records)}...")

            logger.info(
                f"Results: {len(measurement_rows)} measurements, "
                f"{len(observation_rows)} observations, {len(failed_records)} failed"
            )

            # Upsert results (idempotent by idem_key)
            measurements_upserted = 0
            observations_upserted = 0

            if not context.dry_run:
                if measurement_rows:
                    measurements_upserted = storage_client.upsert_measurements(
                        measurements=measurement_rows,
                        table=measurement_table,
                        correlation_id=context.run_id,
                    )
                    logger.info(f"Upserted {measurements_upserted} measurement events")

                if observation_rows:
                    observations_upserted = storage_client.upsert_observations(
                        observations=observation_rows,
                        table=observation_table,
                        correlation_id=context.run_id,
                    )
                    logger.info(f"Upserted {observations_upserted} observations")
            else:
                logger.info(
                    f"[DRY-RUN] Would upsert {len(measurement_rows)} measurements, "
                    f"{len(observation_rows)} observations"
                )

            duration_seconds = time.time() - start_time

            # Emit formation.completed event
            event_client.log_event(
                event_type=on_complete,
                source_system="final_form",
                target_object_type="measurement",
                correlation_id=context.run_id,
                status="success",
                payload={
                    "job_id": job_id,
                    "binding_id": loaded_binding_id,
                    "binding_version": loaded_binding_version,
                    "records_processed": len(records),
                    "measurements_created": len(measurement_rows),
                    "observations_created": len(observation_rows),
                    "measurements_upserted": measurements_upserted,
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

            logger.info(
                f"Formation complete: {len(measurement_rows)} measurements, "
                f"{len(observation_rows)} observations, duration={duration_seconds:.2f}s"
            )

        except Exception as e:
            duration_seconds = time.time() - start_time
            logger.error(f"Formation failed: {e}", exc_info=True)

            event_client.log_event(
                event_type=on_fail,
                source_system="final_form",
                target_object_type="measurement",
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

    def _create_pipeline(
        self,
        binding_id: str,
        binding_version: str | None,
        measure_registry_path: Path,
        binding_registry_path: Path,
    ) -> Pipeline:
        """Create a final-form Pipeline for the specified binding.

        Args:
            binding_id: The form binding to use
            binding_version: Optional specific version (None = latest)
            measure_registry_path: Path to measure registry
            binding_registry_path: Path to binding registry

        Returns:
            Configured Pipeline instance
        """
        config = PipelineConfig(
            measure_registry_path=measure_registry_path,
            binding_registry_path=binding_registry_path,
            binding_id=binding_id,
            binding_version=binding_version,
        )
        return Pipeline(config)

    def _query_canonical_for_formation(
        self,
        storage_client: StorageClient,
        canonical_schema: str | None,
        form_id: str | None,
        source_filter: dict[str, Any],
        measurement_table: str,
        limit: int | None,
    ):
        """Query canonical objects that need formation (incremental).

        Returns records where:
        - canonical_schema matches (if provided)
        - form_id matches (if provided)
        - Either never formed OR re-canonized since last formation

        Uses LEFT JOIN with measurement_events to skip already-processed records.
        """
        filters = {k: v for k, v in source_filter.items() if k != "canonical_schema"}

        return storage_client.query_canonical_for_formation(
            canonical_schema=canonical_schema,
            filters=filters,
            measurement_table=measurement_table,
            limit=limit,
        )

    def _build_form_response(
        self,
        record: dict[str, Any],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Map canonical form_response → final-form input format.

        Canonical schema uses:
        - response_id (maps to form_submission_id)
        - answers[] with question_id/answer_value (maps to items[] with field_key/answer)
        - respondent.email (maps to subject_id)
        - No form_id (derive from connection_name on record)

        Args:
            record: The canonical_objects record
            payload: The JSON payload from the record

        Returns:
            Dict in the format expected by final-form Pipeline.process()
        """
        # Map answers to items format expected by final-form
        # Canonical: answers[].question_id/answer_value -> items[].field_key/answer
        answers_data = payload.get("answers", [])
        items = []
        for i, ans in enumerate(answers_data):
            items.append({
                "field_key": ans.get("question_id"),
                "answer": ans.get("answer_value"),
                "position": i,
            })

        # Derive form_id from connection_name (e.g., "google-forms-intake-01" -> "googleforms::intake_01")
        connection_name = record.get("connection_name", "")
        form_id = payload.get("form_id") or f"googleforms::{connection_name}"

        # Get subject_id from respondent - could be email or id
        respondent = payload.get("respondent", {})
        subject_id = respondent.get("id") or respondent.get("email")

        return {
            "form_id": form_id,
            "form_submission_id": payload.get("response_id") or payload.get("submission_id"),
            "subject_id": subject_id,
            "timestamp": payload.get("submitted_at"),
            "items": items,
        }

    def _measurement_event_to_storage(
        self,
        event: MeasurementEvent,
        canonical_idem_key: str,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Transform MeasurementEvent to storage row format.

        Args:
            event: The MeasurementEvent from final-form
            canonical_idem_key: The source canonical object's idem_key
            correlation_id: Correlation ID for tracing

        Returns:
            Dict ready for storage upsert
        """
        # Build idempotent key: {canonical_idem_key}:{measure_id}:measurement
        idem_key = f"{canonical_idem_key}:{event.measure_id}:measurement"

        return {
            "idem_key": idem_key,
            "canonical_idem_key": canonical_idem_key,
            "measurement_event_id": event.measurement_event_id,
            "measure_id": event.measure_id,
            "measure_version": event.measure_version,
            "subject_id": event.subject_id,
            "timestamp": event.timestamp,
            "binding_id": event.source.binding_id,
            "binding_version": event.source.binding_version,
            "form_id": event.source.form_id,
            "form_submission_id": event.source.form_submission_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "correlation_id": correlation_id,
        }

    def _observation_to_storage(
        self,
        obs: Observation,
        measurement_idem_key: str,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Transform Observation to storage row format.

        Args:
            obs: The Observation from final-form
            measurement_idem_key: The parent measurement's idem_key
            correlation_id: Correlation ID for tracing

        Returns:
            Dict ready for storage upsert
        """
        # Build idempotent key: {measurement_idem_key}:obs:{code}
        idem_key = f"{measurement_idem_key}:obs:{obs.code}"

        return {
            "idem_key": idem_key,
            "measurement_idem_key": measurement_idem_key,
            "observation_id": obs.observation_id,
            "measure_id": obs.measure_id,
            "code": obs.code,
            "kind": obs.kind,
            "value": obs.value,
            "value_type": obs.value_type,
            "label": obs.label,
            "raw_answer": obs.raw_answer,
            "position": obs.position,
            "missing": obs.missing,
            "correlation_id": correlation_id,
        }


# Register with global registry
registry.register("final_form", FinalFormProcessor())
