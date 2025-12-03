"""FinalFormProcessor - wraps final-form library for clinical instrument processing.

This processor handles all final_form jobs by:
1. Reading source config from job_spec (canonical_schema, instrument filters)
2. Querying canonical_objects for records to process
3. Calling final-form library to score instruments (final-form does NO IO)
4. Writing results to measurement_events and observations via storage_client
5. Emitting finalization.completed/failed events

The final-form library is a pure transform engine - it returns results,
this processor handles all IO and event emission.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any

from lorchestra.processors.base import (
    EventClient,
    JobContext,
    StorageClient,
    registry,
)

logger = logging.getLogger(__name__)


class FinalFormProcessor:
    """Processor for final_form jobs.

    Wraps the final-form library to process canonical objects through
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

        # Transform config
        instrument_id = transform_config.get("instrument_id")
        instrument_version = transform_config.get("instrument_version", "1.0.0")
        binding_id = transform_config.get("binding_id")
        finalform_spec = transform_config.get("finalform_spec")

        # Sink config
        measurement_table = sink.get("measurement_table", "measurement_events")
        observation_table = sink.get("observation_table", "observations")

        # Options
        limit = options.get("limit")

        # Event names
        on_complete = events_config.get("on_complete", "finalization.completed")
        on_fail = events_config.get("on_fail", "finalization.failed")

        logger.info(f"Starting finalization job: {job_id}")
        logger.info(f"  instrument: {instrument_id}@{instrument_version}")
        logger.info(f"  binding: {binding_id}")
        logger.info(f"  finalform_spec: {finalform_spec}")
        if context.dry_run:
            logger.info("  DRY RUN - no writes will occur")

        start_time = time.time()

        try:
            # Query canonical objects for this instrument
            records = list(storage_client.query_canonical(
                canonical_schema=canonical_schema,
                filters=source_filter,
                limit=limit,
            ))

            logger.info(f"Found {len(records)} canonical records to process")

            if not records:
                logger.info("No records to process")
                return

            # Get the finalization spec from final-form library
            finalizer = self._get_finalizer(finalform_spec)

            # Process each record
            measurement_events = []
            observations = []
            failed_records = []
            finalization_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            for i, record in enumerate(records):
                try:
                    payload = record.get("payload", {})

                    # Run through final-form library
                    result = finalizer.process(payload)

                    # Extract measurement event
                    measurement_event = {
                        "idem_key": f"{record['idem_key']}:measurement",
                        "canonical_idem_key": record["idem_key"],
                        "instrument_id": instrument_id,
                        "instrument_version": instrument_version,
                        "binding_id": binding_id,
                        "score": result.get("score"),
                        "score_interpretation": result.get("interpretation"),
                        "processed_at": finalization_stamp,
                        "correlation_id": record.get("correlation_id") or context.run_id,
                    }
                    measurement_events.append(measurement_event)

                    # Extract individual observations
                    for obs in result.get("observations", []):
                        observation = {
                            "idem_key": f"{record['idem_key']}:obs:{obs['item_id']}",
                            "measurement_idem_key": measurement_event["idem_key"],
                            "item_id": obs["item_id"],
                            "item_text": obs.get("item_text"),
                            "response_value": obs.get("response_value"),
                            "response_text": obs.get("response_text"),
                            "score_value": obs.get("score_value"),
                            "correlation_id": measurement_event["correlation_id"],
                        }
                        observations.append(observation)

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
                f"Results: {len(measurement_events)} measurements, "
                f"{len(observations)} observations, {len(failed_records)} failed"
            )

            # Insert results
            measurements_inserted = 0
            observations_inserted = 0

            if not context.dry_run:
                if measurement_events:
                    measurements_inserted = storage_client.insert_measurements(
                        measurements=measurement_events,
                        table=measurement_table,
                        correlation_id=context.run_id,
                    )
                    logger.info(f"Inserted {measurements_inserted} measurement events")

                if observations:
                    observations_inserted = storage_client.insert_observations(
                        observations=observations,
                        table=observation_table,
                        correlation_id=context.run_id,
                    )
                    logger.info(f"Inserted {observations_inserted} observations")

            duration_seconds = time.time() - start_time

            # Emit success event
            event_client.log_event(
                event_type=on_complete,
                source_system=instrument_id,
                target_object_type="measurement",
                correlation_id=context.run_id,
                status="success",
                payload={
                    "job_id": job_id,
                    "instrument_id": instrument_id,
                    "instrument_version": instrument_version,
                    "binding_id": binding_id,
                    "records_processed": len(records),
                    "measurements_created": len(measurement_events),
                    "observations_created": len(observations),
                    "measurements_inserted": measurements_inserted,
                    "observations_inserted": observations_inserted,
                    "failed": len(failed_records),
                    "duration_seconds": round(duration_seconds, 2),
                    "dry_run": context.dry_run,
                },
            )

            logger.info(
                f"Finalization complete: {len(measurement_events)} measurements, "
                f"{len(observations)} observations, duration={duration_seconds:.2f}s"
            )

        except Exception as e:
            duration_seconds = time.time() - start_time
            logger.error(f"Finalization failed: {e}", exc_info=True)

            event_client.log_event(
                event_type=on_fail,
                source_system=instrument_id or "unknown",
                target_object_type="measurement",
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "job_id": job_id,
                    "instrument_id": instrument_id,
                    "error_type": type(e).__name__,
                    "duration_seconds": round(duration_seconds, 2),
                },
            )

            raise

    def _get_finalizer(self, finalform_spec: str):
        """Get compiled finalizer for a finalform_spec reference.

        Args:
            finalform_spec: Finalizer reference (e.g., "phq9_finalform@1.0.0")

        Returns:
            Compiled finalizer from final-form library
        """
        from pathlib import Path

        # Parse finalform_spec: "name@version" or just "name"
        if "@" in finalform_spec:
            name_part, version = finalform_spec.rsplit("@", 1)
        else:
            name_part = finalform_spec
            version = "1.0.0"

        registry_root = Path("/workspace/lorchestra/.finalform/registry")
        spec_path = registry_root / f"specs/{name_part}/{version}/spec.yaml"

        if not spec_path.exists():
            raise FileNotFoundError(f"Finalform spec not found: {spec_path}")

        # Import final-form library
        # For now, return a mock finalizer until the library is implemented
        # In production: from finalform import load_spec
        # return load_spec(spec_path)

        # Placeholder that will be replaced when final-form library exists
        raise NotImplementedError(
            f"final-form library not yet implemented. "
            f"Would load spec from: {spec_path}"
        )


# Register with global registry
registry.register("final_form", FinalFormProcessor())
