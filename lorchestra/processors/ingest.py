"""IngestProcessor - wraps injest library for data ingestion.

This processor handles all ingest jobs by:
1. Reading stream config from job_spec
2. Calling injest.get_stream() to pull data (injest does NO IO)
3. Writing records to raw_objects via storage_client
4. Emitting ingest.completed/failed events

The injest library is a pure transform engine - it returns records,
this processor handles all IO and event emission.
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from google.cloud import bigquery

from lorchestra.processors.base import (
    EventClient,
    JobContext,
    StorageClient,
    UpsertResult,
    registry,
)

logger = logging.getLogger(__name__)


def _parse_date_to_datetime(value: str) -> datetime:
    """Parse date string to datetime.

    Supports:
    - Relative dates like "-7d" (7 days ago)
    - ISO format like "2024-01-01T00:00:00Z"

    Args:
        value: Date string to parse

    Returns:
        Parsed datetime in UTC
    """
    if value.startswith("-") and value.endswith("d"):
        days = int(value[1:-1])
        return datetime.now(timezone.utc) - timedelta(days=days)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _get_last_sync_timestamp(
    bq_client: bigquery.Client,
    source_system: str,
    connection_name: str,
    object_type: str,
) -> str | None:
    """Query BigQuery for the most recent object timestamp.

    Looks up MAX(last_seen) from raw_objects to determine the time cursor
    for incremental ingestion.

    Args:
        bq_client: BigQuery client
        source_system: Provider family (e.g., "gmail")
        connection_name: Account identifier (e.g., "gmail-acct1")
        object_type: Object type (e.g., "email")

    Returns:
        ISO timestamp string of last sync, or None if no previous sync
    """
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    query = f"""
        SELECT MAX(last_seen) as last_sync
        FROM `{dataset}.raw_objects`
        WHERE source_system = @source_system
          AND connection_name = @connection_name
          AND object_type = @object_type
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_system", "STRING", source_system),
            bigquery.ScalarQueryParameter("connection_name", "STRING", connection_name),
            bigquery.ScalarQueryParameter("object_type", "STRING", object_type),
        ]
    )

    try:
        result = bq_client.query(query, job_config=job_config).result()
        row = next(iter(result), None)
        if row and row.last_sync:
            return row.last_sync.isoformat()
        return None
    except Exception as e:
        logger.warning(f"Could not query last sync: {e}")
        return None


def _get_idem_key_fn(
    source_system: str,
    connection_name: str,
    object_type: str,
    stream: str,
) -> Callable[[dict[str, Any]], str]:
    """Get the appropriate idem_key function for a stream.

    Args:
        source_system: Provider family
        connection_name: Account identifier
        object_type: Domain object type
        stream: Stream identifier (e.g., "gmail.messages")

    Returns:
        Function that computes idem_key from object payload
    """
    from lorchestra.idem_keys import (
        dataverse_idem_key,
        exchange_idem_key,
        gmail_idem_key,
        google_forms_idem_key,
        stripe_idem_key,
    )

    # Map stream prefixes to idem_key functions
    if stream.startswith("gmail."):
        return gmail_idem_key(source_system, connection_name)
    elif stream.startswith("exchange."):
        return exchange_idem_key(source_system, connection_name)
    elif stream.startswith("google_forms."):
        return google_forms_idem_key(source_system, connection_name)
    elif stream.startswith("stripe."):
        return stripe_idem_key(source_system, connection_name, object_type)
    elif stream.startswith("dataverse."):
        # Dataverse needs the ID field name - derive from object_type
        id_field_map = {
            "contact": "contactid",
            "session": "cre92_clientsessionid",
            "report": "cre92_progressreportid",
        }
        id_field = id_field_map.get(object_type, f"{object_type}id")
        return dataverse_idem_key(source_system, connection_name, object_type, id_field)
    else:
        # Generic fallback - use 'id' field
        def generic_idem_key(obj: dict[str, Any]) -> str:
            obj_id = obj.get("id")
            if not obj_id:
                raise ValueError(f"Object missing 'id' field: {obj}")
            return f"{source_system}:{connection_name}:{object_type}:{obj_id}"

        return generic_idem_key


class IngestProcessor:
    """Processor for ingest jobs.

    Wraps the injest library to pull data from external sources
    and write to raw_objects. Handles all IO and event emission.
    """

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute an ingest job.

        Args:
            job_spec: Parsed job specification with source/sink config
            context: Execution context with BQ client and run_id
            storage_client: Storage operations interface
            event_client: Event emission interface
        """
        from injest import get_stream

        from lorchestra.injest_config import configure_injest

        # Extract config from job_spec
        job_id = job_spec["job_id"]
        source = job_spec["source"]
        sink = job_spec["sink"]
        options = job_spec.get("options", {})
        events_config = job_spec.get("events", {})

        stream_name = source["stream"]
        identity = source["identity"]
        source_system = sink["source_system"]
        connection_name = sink["connection_name"]
        object_type = sink["object_type"]

        # Options
        auto_since = options.get("auto_since", False)
        since = options.get("since")
        until = options.get("until")
        limit = options.get("limit")

        # Event names (with defaults)
        on_complete = events_config.get("on_complete", "ingest.completed")
        on_fail = events_config.get("on_fail", "ingest.failed")

        # Configure injest
        configure_injest()

        # Auto-detect last sync if requested and no explicit since
        if auto_since and since is None and not context.dry_run:
            logger.info("auto_since enabled, querying BigQuery for last sync...")
            last_sync = _get_last_sync_timestamp(
                context.bq_client, source_system, connection_name, object_type
            )
            if last_sync:
                since = last_sync
                logger.info(f"Found last sync: {since}")
            else:
                logger.info("No previous sync found, will extract all records")

        # Parse dates
        since_dt = _parse_date_to_datetime(since) if since else None
        until_dt = _parse_date_to_datetime(until) if until else None

        logger.info(f"Starting ingest: {job_id}")
        logger.info(f"  stream: {stream_name}, identity: {identity}")
        logger.info(f"  sink: {source_system}/{connection_name}/{object_type}")
        if since_dt:
            logger.info(f"  since: {since_dt.isoformat()}")
        if until_dt:
            logger.info(f"  until: {until_dt.isoformat()}")
        if context.dry_run:
            logger.info("  DRY RUN - no writes will occur")

        start_time = time.time()

        try:
            # Get stream from injest (handles auth + API calls)
            stream = get_stream(stream_name, identity=identity)

            # Get idem_key function
            idem_key_fn = _get_idem_key_fn(source_system, connection_name, object_type, stream_name)

            # Count records for telemetry
            record_count = 0

            def count_and_yield():
                nonlocal record_count
                for record in stream.extract(since=since_dt, until=until_dt):
                    record_count += 1
                    if limit and record_count > limit:
                        break
                    yield record

            # Upsert objects
            if context.dry_run:
                # In dry run, just count records
                for _ in count_and_yield():
                    pass
                result = UpsertResult(inserted=0, updated=0)
                logger.info(f"DRY RUN: Would have processed {record_count} records")
            else:
                result = storage_client.upsert_objects(
                    objects=count_and_yield(),
                    source_system=source_system,
                    connection_name=connection_name,
                    object_type=object_type,
                    idem_key_fn=idem_key_fn,
                    correlation_id=context.run_id,
                )

            duration_seconds = time.time() - start_time

            # Emit success event
            event_client.log_event(
                event_type=on_complete,
                source_system=source_system,
                connection_name=connection_name,
                target_object_type=object_type,
                correlation_id=context.run_id,
                status="ok",
                payload={
                    "job_id": job_id,
                    "records_extracted": record_count,
                    "inserted": result.inserted,
                    "updated": result.updated,
                    "duration_seconds": round(duration_seconds, 2),
                    "since": since_dt.isoformat() if since_dt else None,
                    "until": until_dt.isoformat() if until_dt else None,
                    "dry_run": context.dry_run,
                },
            )

            logger.info(
                f"Ingest complete: {record_count} records, "
                f"inserted={result.inserted}, updated={result.updated}, "
                f"duration={duration_seconds:.2f}s"
            )

        except Exception as e:
            duration_seconds = time.time() - start_time
            logger.error(f"Ingest failed: {e}", exc_info=True)

            # Emit failure event
            event_client.log_event(
                event_type=on_fail,
                source_system=source_system,
                connection_name=connection_name,
                target_object_type=object_type,
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


# Register with global registry
registry.register("ingest", IngestProcessor())
