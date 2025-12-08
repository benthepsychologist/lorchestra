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
        on_started = events_config.get("on_started", "validation.started")
        on_complete = events_config.get("on_complete", "validation.completed")
        on_fail = events_config.get("on_fail", "validation.failed")

        logger.info(f"Starting validation job: {job_id}")
        logger.info(f"  source: {source_system}/{object_type}")
        logger.info(f"  schema_in: {schema_in}")
        if context.dry_run:
            logger.info("  DRY RUN - no writes will occur")

        # Emit started event
        event_client.log_event(
            event_type=on_started,
            source_system=source_system,
            target_object_type=object_type,
            correlation_id=context.run_id,
            status="success",
            payload={"job_id": job_id},
        )

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
            validator = self._get_validator(schema_in, context.config)

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
                status="success",
                payload={
                    "job_id": job_id,
                    "object_type": object_type,
                    "records_checked": len(records),
                    "records_pass": len(passed_keys),
                    "records_fail": len(failed_keys),
                    "schema_ref": schema_in,
                    "duration_seconds": round(duration_seconds, 2),
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
                    "object_type": object_type,
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
        # Optional: explicit suffix for idem_key when multiple transforms target same schema
        # e.g. session_note, session_summary both target clinical_document schema
        idem_key_suffix = transform_config.get("idem_key_suffix")
        limit = options.get("limit")
        on_started = events_config.get("on_started", "canonization.started")
        on_complete = events_config.get("on_complete", "canonization.completed")
        on_fail = events_config.get("on_fail", "canonization.failed")

        logger.info(f"Starting canonization job: {job_id}")
        logger.info(f"  source: {source_system}/{object_type}")
        logger.info(f"  transform_ref: {transform_ref}")
        logger.info(f"  schema_out: {schema_out}")
        if context.dry_run:
            logger.info("  DRY RUN - no writes will occur")

        # Emit started event
        event_client.log_event(
            event_type=on_started,
            source_system=source_system,
            target_object_type=object_type,
            correlation_id=context.run_id,
            status="success",
            payload={"job_id": job_id},
        )

        start_time = time.time()

        try:
            # Query validated records that need canonization
            # (never canonized for this schema OR raw updated since last canonization)
            # Pass idem_key_suffix for stable 1:N mapping that survives schema changes
            records = list(storage_client.query_objects_for_canonization(
                source_system=source_system,
                object_type=object_type,
                filters=source_filter,
                limit=limit,
                canonical_schema=schema_out,
                idem_key_suffix=idem_key_suffix,
            ))

            logger.info(f"Found {len(records)} records to canonize")

            if not records:
                logger.info("No records to canonize")
                return

            # Get transform from canonizer
            transform = self._get_transform(transform_ref, context.config)

            # Transform each record
            success_records = []
            failed_records = []

            skipped_count = 0
            for i, record in enumerate(records):
                try:
                    payload = record.get("payload", {})
                    if isinstance(payload, str):
                        payload = json.loads(payload)

                    # Transform
                    canonical = transform.evaluate(payload)

                    # Skip records where transform returns null (e.g., session without transcript)
                    if canonical is None:
                        skipped_count += 1
                        continue

                    # Determine the idem_key suffix for 1:N mapping
                    # Use explicit idem_key_suffix if provided (for multiple transforms to same schema)
                    # Otherwise extract from schema name (e.g. session_transcript from schema path)
                    if idem_key_suffix:
                        canonical_object_type = idem_key_suffix
                    else:
                        # e.g. iglu:org.canonical/session_transcript/jsonschema/2-0-0 -> session_transcript
                        canonical_object_type = schema_out.split("/")[1]

                    # Build canonical idem_key: raw_idem_key#canonical_object_type
                    # This allows one raw object to produce multiple canonical objects
                    # e.g. session -> clinical_session AND session_transcript
                    canonical_idem_key = f"{record['idem_key']}#{canonical_object_type}"

                    # Extract canonical_format (legacy field)
                    format_parts = canonical_object_type.split("_")
                    canonical_format = "_".join(format_parts[1:]) if len(format_parts) > 1 else format_parts[0]

                    canonical_record = {
                        "idem_key": canonical_idem_key,
                        "source_system": record.get("source_system", source_system),
                        "connection_name": record.get("connection_name"),
                        "object_type": record.get("object_type", object_type),
                        "canonical_schema": schema_out,
                        "canonical_format": canonical_format,
                        "transform_ref": transform_ref,
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

            logger.info(f"Results: {len(success_records)} success, {len(failed_records)} failed, {skipped_count} skipped (null transform)")

            # Upsert canonical records (insert new, update existing)
            upsert_result = {"inserted": 0, "updated": 0}
            if success_records and not context.dry_run:
                # Use smaller batch size for large payloads (like transcripts)
                # Default 500 can exceed BQ query size limits with large content
                # Transcripts can be 20KB+ each, so we need very small batches
                upsert_result = storage_client.upsert_canonical(
                    objects=success_records,
                    correlation_id=context.run_id,
                    batch_size=10,
                )
                logger.info(f"Upserted {upsert_result['inserted']} canonical records")

            duration_seconds = time.time() - start_time

            # Emit success event
            event_client.log_event(
                event_type=on_complete,
                source_system=source_system,
                target_object_type=object_type,
                correlation_id=context.run_id,
                status="success",
                payload={
                    "job_id": job_id,
                    "object_type": object_type,
                    "records_processed": len(records),
                    "records_inserted": upsert_result["inserted"],
                    "records_updated": upsert_result["updated"],
                    "schema_ref": schema_out,
                    "duration_seconds": round(duration_seconds, 2),
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
                    "object_type": object_type,
                    "error_type": type(e).__name__,
                    "duration_seconds": round(duration_seconds, 2),
                },
            )

            raise

    def _get_validator(self, schema_iglu: str, config: Any):
        """Get validator for a schema URI.

        Args:
            schema_iglu: Iglu schema URI (e.g., "iglu:raw/email_gmail/jsonschema/1-0-0")
            config: LorchestraConfig object

        Returns:
            Validator instance from canonizer
        """
        from pathlib import Path

        # Import canonizer validator
        # Ideally this should be installed in the environment
        try:
            from canonizer.core.validator import SchemaValidator, load_schema_from_iglu_uri
        except ImportError:
            # Fallback for dev environment
            import sys
            sys.path.insert(0, "/workspace/canonizer")
            from canonizer.core.validator import SchemaValidator, load_schema_from_iglu_uri

        registry_root = Path(config.canonizer_registry_root or "/workspace/lorchestra/.canonizer/registry")
        schemas_dir = registry_root / "schemas"
        schema_path = load_schema_from_iglu_uri(schema_iglu, schemas_dir)
        return SchemaValidator(schema_path)

    def _get_transform(self, transform_ref: str, config: Any):
        """Get a transform wrapper that uses canonizer-core CLI's run command.

        Uses the Node.js CLI directly to properly support extensions like htmlToMarkdown.

        Args:
            transform_ref: Transform reference (e.g., "email/gmail_to_jmap_lite@1.0.0")
            config: LorchestraConfig object

        Returns:
            Transform wrapper with evaluate() method
        """
        import subprocess
        import tempfile
        import os
        from pathlib import Path

        registry_root = config.canonizer_registry_root or "/workspace/lorchestra/.canonizer/registry"
        registry_path = str(registry_root)
        
        # This path is still hardcoded as it's the external tool location
        # TODO: Make this configurable too
        cli_bin = "/workspace/canonizer/packages/canonizer-core/bin/canonizer-core"

        class TransformWrapper:
            """Wrapper that calls canonizer-core CLI run command."""

            def __init__(self, ref: str):
                self.ref = ref

            def evaluate(self, payload: dict) -> dict:
                """Execute transform via canonizer-core CLI (supports extensions).

                Uses temp files instead of pipes to avoid stdout truncation at 65535 bytes.
                Large transcripts can exceed pipe buffer limits.
                """
                # Write input to temp file (avoids stdin pipe limits)
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(payload, f)
                    input_file = f.name

                # Create temp file for output (avoids stdout pipe limits)
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    output_file = f.name

                try:
                    # Run CLI with file redirection to avoid 65KB pipe buffer truncation
                    result = subprocess.run(
                        f"cat {input_file} | {cli_bin} run --transform {self.ref} "
                        f"--registry {registry_path} --no-validate > {output_file}",
                        shell=True,
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    if result.returncode != 0:
                        raise RuntimeError(f"Transform failed: {result.stderr.strip()}")

                    with open(output_file, 'r') as f:
                        return json.load(f)
                finally:
                    # Clean up temp files
                    os.unlink(input_file)
                    os.unlink(output_file)

        return TransformWrapper(transform_ref)


# Register with global registry
registry.register("canonize", CanonizeProcessor())
