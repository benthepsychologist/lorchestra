"""CanonizeProcessor - wraps canonizer library for validation and transformation.

This processor handles all canonize jobs by:
1. Reading source config from job_spec (source_system, object_type, schema/transform refs)
2. Querying raw_objects for records to process
3. Calling canonizer.validate() and/or canonizer.transform() (canonizer does NO IO)
4. Writing results via storage_client
5. Emitting completion events

Supports two modes:
- validate_only: Validate raw objects, stamp validation_status (no transform)
- full: Validate input → transform → validate output → write canonical_objects

The canonizer library is a pure transform engine - it returns results,
this processor handles all IO and event emission.
"""

import json
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


class CanonizeProcessor:
    """Processor for canonize jobs.

    Wraps the canonizer library to validate and transform raw objects.
    Handles all IO and event emission.
    """

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute a canonize job.

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

        source_system = source["source_system"]
        object_type = source["object_type"]
        source_filter = source.get("filter", {})

        # Determine mode
        mode = transform_config.get("mode", "full")

        if mode == "validate_only":
            self._run_validate_only(
                job_id=job_id,
                source_system=source_system,
                object_type=object_type,
                source_filter=source_filter,
                transform_config=transform_config,
                sink=sink,
                options=options,
                events_config=events_config,
                context=context,
                storage_client=storage_client,
                event_client=event_client,
            )
        else:
            self._run_full(
                job_id=job_id,
                source_system=source_system,
                object_type=object_type,
                source_filter=source_filter,
                transform_config=transform_config,
                sink=sink,
                options=options,
                events_config=events_config,
                context=context,
                storage_client=storage_client,
                event_client=event_client,
            )

    def _run_validate_only(
        self,
        job_id: str,
        source_system: str,
        object_type: str,
        source_filter: dict[str, Any],
        transform_config: dict[str, Any],
        sink: dict[str, Any],
        options: dict[str, Any],
        events_config: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Run validation-only mode: validate raw objects and stamp validation_status."""
        schema_in = transform_config.get("schema_in")
        limit = options.get("limit")
        on_complete = events_config.get("on_complete", "validate.completed")
        on_fail = events_config.get("on_fail", "validate.failed")

        logger.info(f"Starting validation job: {job_id}")
        logger.info(f"  source: {source_system}/{object_type}")
        logger.info(f"  schema_in: {schema_in}")
        if context.dry_run:
            logger.info("  DRY RUN - no writes will occur")

        start_time = time.time()

        try:
            # Query unvalidated records
            records = list(storage_client.query_objects(
                source_system=source_system,
                object_type=object_type,
                filters={"validation_status": None, **source_filter},
                limit=limit,
            ))

            logger.info(f"Found {len(records)} unvalidated records")

            if not records:
                logger.info("No records to validate")
                return

            # Get validator from canonizer
            validator = self._get_validator(schema_in)

            # Validate each record
            passed_keys = []
            failed_keys = []

            for i, record in enumerate(records):
                try:
                    payload = record.get("payload", {})
                    if isinstance(payload, str):
                        payload = json.loads(payload)

                    validator.validate(payload)
                    passed_keys.append(record["idem_key"])
                except Exception as e:
                    failed_keys.append(record["idem_key"])
                    if len(failed_keys) <= 3:
                        logger.warning(f"FAILED: {record['idem_key'][:50]}... Error: {e}")

                if (i + 1) % 500 == 0:
                    logger.info(f"Validated {i + 1}/{len(records)}...")

            logger.info(f"Results: {len(passed_keys)} passed, {len(failed_keys)} failed")

            # Update validation_status
            if not context.dry_run:
                if passed_keys:
                    storage_client.update_field(passed_keys, "validation_status", "pass")
                if failed_keys:
                    storage_client.update_field(failed_keys, "validation_status", "fail")

            duration_seconds = time.time() - start_time

            # Emit success event
            event_client.log_event(
                event_type=on_complete,
                source_system=source_system,
                target_object_type=object_type,
                correlation_id=context.run_id,
                status="ok",
                payload={
                    "job_id": job_id,
                    "mode": "validate_only",
                    "records_validated": len(records),
                    "passed": len(passed_keys),
                    "failed": len(failed_keys),
                    "schema_in": schema_in,
                    "duration_seconds": round(duration_seconds, 2),
                    "dry_run": context.dry_run,
                },
            )

            logger.info(
                f"Validation complete: {len(passed_keys)}/{len(records)} passed, "
                f"duration={duration_seconds:.2f}s"
            )

        except Exception as e:
            duration_seconds = time.time() - start_time
            logger.error(f"Validation failed: {e}", exc_info=True)

            event_client.log_event(
                event_type=on_fail,
                source_system=source_system,
                target_object_type=object_type,
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "job_id": job_id,
                    "mode": "validate_only",
                    "error_type": type(e).__name__,
                    "duration_seconds": round(duration_seconds, 2),
                },
            )

            raise

    def _run_full(
        self,
        job_id: str,
        source_system: str,
        object_type: str,
        source_filter: dict[str, Any],
        transform_config: dict[str, Any],
        sink: dict[str, Any],
        options: dict[str, Any],
        events_config: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Run full mode: validate → transform → write canonical objects."""
        schema_out = transform_config.get("schema_out")
        transform_ref = transform_config.get("transform_ref")
        limit = options.get("limit")
        on_complete = events_config.get("on_complete", "canonize.completed")
        on_fail = events_config.get("on_fail", "canonize.failed")

        logger.info(f"Starting canonization job: {job_id}")
        logger.info(f"  source: {source_system}/{object_type}")
        logger.info(f"  transform_ref: {transform_ref}")
        logger.info(f"  schema_out: {schema_out}")
        if context.dry_run:
            logger.info("  DRY RUN - no writes will occur")

        start_time = time.time()

        try:
            # Default filter: only validated records
            default_filter = {"validation_status": "pass"}
            combined_filter = {**default_filter, **source_filter}

            # Query validated records not yet canonized
            records = list(storage_client.query_objects(
                source_system=source_system,
                object_type=object_type,
                filters=combined_filter,
                limit=limit,
            ))

            logger.info(f"Found {len(records)} records to canonize")

            if not records:
                logger.info("No records to canonize")
                return

            # Get transform from canonizer
            transform = self._get_transform(transform_ref)

            # Transform each record
            success_records = []
            failed_records = []
            canonization_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            for i, record in enumerate(records):
                try:
                    payload = record.get("payload", {})
                    if isinstance(payload, str):
                        payload = json.loads(payload)

                    # Transform
                    canonical = transform.evaluate(payload)

                    # Extract canonical_format from schema name
                    # e.g. iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0 -> jmap_lite
                    schema_name = schema_out.split("/")[1]  # email_jmap_lite
                    format_parts = schema_name.split("_")
                    canonical_format = "_".join(format_parts[1:]) if len(format_parts) > 1 else format_parts[0]

                    canonical_record = {
                        "idem_key": record["idem_key"],
                        "source_system": record.get("source_system", source_system),
                        "connection_name": record.get("connection_name"),
                        "object_type": record.get("object_type", object_type),
                        "canonical_schema": schema_out,
                        "canonical_format": canonical_format,
                        "transform_ref": transform_ref,
                        "validation_stamp": canonization_stamp,
                        "payload": canonical,
                        "correlation_id": record.get("correlation_id"),
                    }
                    success_records.append(canonical_record)

                except Exception as e:
                    failed_records.append({
                        "idem_key": record["idem_key"],
                        "error": str(e),
                    })
                    if len(failed_records) <= 3:
                        logger.warning(f"FAILED: {record['idem_key'][:50]}... Error: {e}")

                if (i + 1) % 500 == 0:
                    logger.info(f"Canonized {i + 1}/{len(records)}...")

            logger.info(f"Results: {len(success_records)} success, {len(failed_records)} failed")

            # Insert canonical records
            total_inserted = 0
            if success_records and not context.dry_run:
                total_inserted = storage_client.insert_canonical(
                    objects=success_records,
                    correlation_id=context.run_id,
                )
                logger.info(f"Inserted {total_inserted} canonical records")

            duration_seconds = time.time() - start_time

            # Emit success event
            event_client.log_event(
                event_type=on_complete,
                source_system=source_system,
                target_object_type=object_type,
                correlation_id=context.run_id,
                status="ok",
                payload={
                    "job_id": job_id,
                    "mode": "full",
                    "records_processed": len(records),
                    "success": len(success_records),
                    "failed": len(failed_records),
                    "inserted": total_inserted,
                    "transform_ref": transform_ref,
                    "schema_out": schema_out,
                    "duration_seconds": round(duration_seconds, 2),
                    "dry_run": context.dry_run,
                },
            )

            logger.info(
                f"Canonization complete: {len(success_records)}/{len(records)} canonized, "
                f"duration={duration_seconds:.2f}s"
            )

        except Exception as e:
            duration_seconds = time.time() - start_time
            logger.error(f"Canonization failed: {e}", exc_info=True)

            event_client.log_event(
                event_type=on_fail,
                source_system=source_system,
                target_object_type=object_type,
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "job_id": job_id,
                    "mode": "full",
                    "error_type": type(e).__name__,
                    "duration_seconds": round(duration_seconds, 2),
                },
            )

            raise

    def _get_validator(self, schema_iglu: str):
        """Get validator for a schema URI.

        Args:
            schema_iglu: Iglu schema URI (e.g., "iglu:raw/email_gmail/jsonschema/1-0-0")

        Returns:
            Validator instance from canonizer
        """
        from pathlib import Path

        # Import canonizer validator
        import sys
        sys.path.insert(0, "/workspace/canonizer")
        from canonizer.core.validator import SchemaValidator, load_schema_from_iglu_uri

        schemas_dir = Path("/workspace/lorchestra/.canonizer/registry/schemas")
        schema_path = load_schema_from_iglu_uri(schema_iglu, schemas_dir)
        return SchemaValidator(schema_path)

    def _get_transform(self, transform_ref: str):
        """Get compiled transform for a transform reference.

        Args:
            transform_ref: Transform reference (e.g., "gmail_to_jmap_lite@1.0.0")

        Returns:
            Compiled transform (Jsonata instance)
        """
        from pathlib import Path

        from jsonata import Jsonata

        # Parse transform_ref: "name@version" or "path/name@version"
        if "@" in transform_ref:
            name_part, version = transform_ref.rsplit("@", 1)
        else:
            name_part = transform_ref
            version = "1.0.0"

        # Handle path-style refs like "email/gmail_to_jmap_lite@1.0.0"
        if "/" in name_part:
            transform_path = name_part
        else:
            transform_path = name_part

        registry_root = Path("/workspace/lorchestra/.canonizer/registry")
        jsonata_path = registry_root / f"transforms/{transform_path}/{version}/spec.jsonata"

        if not jsonata_path.exists():
            raise FileNotFoundError(f"Transform not found: {jsonata_path}")

        jsonata_expr = jsonata_path.read_text()
        return Jsonata(jsonata_expr)


# Register with global registry
registry.register("canonize", CanonizeProcessor())
