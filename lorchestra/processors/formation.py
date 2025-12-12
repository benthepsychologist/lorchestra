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

Data Model (FHIR-aligned):
- measurement_events: 1 row per form submission (the measurement event)
- observations: 1 row per scored construct (PHQ-9, GAD-7) with components JSON array
- components: JSON array within observations containing items and scales

Mapping from final-form:
- final-form MeasurementEvent → our observation (1 per scored measure)
- final-form Observation → our components (items/scales within a measure)
- form submission → our measurement_event (1 per submission)
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from finalform.pipeline import Pipeline, PipelineConfig, ProcessingResult
from finalform.core.models import MeasurementEvent

from lorchestra.processors.base import (
    EventClient,
    JobContext,
    StorageClient,
    registry,
)

logger = logging.getLogger(__name__)

# Default registry paths
DEFAULT_MEASURE_REGISTRY = Path("/workspace/finalform/measure-registry")
DEFAULT_BINDING_REGISTRY = Path("/workspace/finalform/form-binding-registry")


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

        # Resolve registry paths from config or defaults
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

                    # FHIR-aligned data model:
                    # - ONE measurement_event per form submission
                    # - ONE observation per scored measure (final-form MeasurementEvent)
                    # - Components array within each observation

                    correlation_id = record.get("correlation_id") or context.run_id

                    # Create ONE measurement_event for this submission
                    if result.events:
                        me_row = self._submission_to_measurement_event(
                            record=record,
                            payload=payload,
                            form_response=form_response,
                            result=result,
                            correlation_id=correlation_id,
                        )
                        measurement_rows.append(me_row)

                        # Create ONE observation per scored measure (final-form MeasurementEvent)
                        for event in result.events:
                            obs_row = self._measure_to_observation(
                                event=event,
                                measurement_event_idem_key=me_row["idem_key"],
                                measurement_event_id=me_row["measurement_event_id"],
                                correlation_id=correlation_id,
                            )
                            observation_rows.append(obs_row)

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
            # IMPORTANT: Write observations FIRST, then measurement_events.
            # This ensures that if observations fail, measurement_events don't exist,
            # so the incremental query will pick up those records on re-run.
            # (measurement_events existence is what the incremental query checks)
            measurements_upserted = 0
            observations_upserted = 0

            if not context.dry_run:
                # Write observations FIRST (if this fails, measurement_events won't be written)
                if observation_rows:
                    observations_upserted = storage_client.upsert_observations(
                        observations=observation_rows,
                        table=observation_table,
                        correlation_id=context.run_id,
                    )
                    logger.info(f"Upserted {observations_upserted} observations")

                # Write measurement_events LAST (marks records as "formed" for incremental query)
                if measurement_rows:
                    measurements_upserted = storage_client.upsert_measurements(
                        measurements=measurement_rows,
                        table=measurement_table,
                        correlation_id=context.run_id,
                    )
                    logger.info(f"Upserted {measurements_upserted} measurement events")
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

    def _submission_to_measurement_event(
        self,
        record: dict[str, Any],
        payload: dict[str, Any],
        form_response: dict[str, Any],
        result: ProcessingResult,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Create ONE measurement_event row for the form submission.

        In FHIR terms, this is like a DiagnosticReport - the container for
        all observations from a single measurement event (form submission).

        Args:
            record: The canonical_objects record
            payload: The JSON payload from the record
            form_response: The form_response dict we built for final-form
            result: The ProcessingResult from final-form
            correlation_id: Correlation ID for tracing

        Returns:
            Dict ready for storage upsert
        """
        # Use canonical idem_key directly - 1 per submission
        idem_key = record["idem_key"]

        # Get binding info from first event (all events share same submission)
        first_event = result.events[0] if result.events else None

        now = datetime.now(timezone.utc).isoformat()

        return {
            "idem_key": idem_key,
            "measurement_event_id": idem_key,  # Same as idem_key for submissions
            "subject_id": form_response.get("subject_id"),
            "subject_contact_id": None,  # Will be populated by future projection
            "event_type": "form",
            "event_subtype": first_event.source.binding_id if first_event else None,
            "occurred_at": form_response.get("timestamp"),
            "received_at": now,
            "source_system": "google_forms",
            "source_entity": "form_response",
            "source_id": form_response.get("form_submission_id"),
            "canonical_object_id": record["idem_key"],
            "form_id": form_response.get("form_id"),
            "binding_id": first_event.source.binding_id if first_event else None,
            "binding_version": first_event.source.binding_version if first_event else None,
            "metadata": json.dumps({}),
            "correlation_id": correlation_id,
            "processed_at": now,
            "created_at": now,
        }

    def _measure_to_observation(
        self,
        event: MeasurementEvent,
        measurement_event_idem_key: str,
        measurement_event_id: str,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Create ONE observation row per scored measure, with components array.

        In FHIR terms, this is like an Observation with components.
        What final-form calls a MeasurementEvent is actually an Observation
        (the scored result of a measure like PHQ-9).

        Args:
            event: The MeasurementEvent from final-form (which is our observation)
            measurement_event_idem_key: The parent measurement_event's idem_key
            measurement_event_id: The parent measurement_event's ID
            correlation_id: Correlation ID for tracing

        Returns:
            Dict ready for storage upsert
        """
        # Build idempotent key: {measurement_event_idem_key}:{measure_code}
        idem_key = f"{measurement_event_idem_key}:{event.measure_id}"

        # Build components array from event.observations (items and scales)
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

        # Extract total score from _total scale observation
        total_score = self._extract_total_score(event)

        # Extract severity from scale observations
        severity_label = self._extract_severity(event)
        severity_code = self._severity_label_to_code(severity_label) if severity_label else None

        now = datetime.now(timezone.utc).isoformat()

        return {
            "idem_key": idem_key,
            "observation_id": event.measurement_event_id,  # Reuse final-form's UUID
            "measurement_event_id": measurement_event_id,
            "subject_id": event.subject_id,
            "measure_code": event.measure_id,
            "measure_version": event.measure_version,
            "value_numeric": total_score,
            "value_text": None,  # For future qualitative results
            "unit": "score",
            "severity_code": severity_code,
            "severity_label": severity_label,
            "components": json.dumps(components),
            "metadata": json.dumps({}),
            "correlation_id": correlation_id,
            "processed_at": now,
            "created_at": now,
        }

    def _extract_total_score(self, event: MeasurementEvent) -> float | None:
        """Extract the total score from a MeasurementEvent.

        Looks for observations with code ending in '_total' and kind='scale'.
        """
        for obs in event.observations:
            if obs.kind == "scale" and obs.code.endswith("_total"):
                if obs.value is not None:
                    try:
                        return float(obs.value)
                    except (TypeError, ValueError):
                        pass
        return None

    def _extract_severity(self, event: MeasurementEvent) -> str | None:
        """Extract severity label from a MeasurementEvent.

        Looks for observations with code ending in '_total' that have a label.
        """
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


# Register with global registry
registry.register("final_form", FinalFormProcessor())
