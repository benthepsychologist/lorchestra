"""Google Forms ingestion jobs.

This module uses InJest for Google Forms extraction:
1. Calls GoogleFormsResponsesStream to get auth-aware form responses stream
2. Streams responses to batch upsert (no per-object events)
3. Logs telemetry events (ingest.completed) with small payloads

Target forms:
- ipip120_01: IPIP-120 personality assessment
- intake_01: Intake form 01
- intake_02: Intake form 02
- followup: Followup form

This is the ONLY place event_client is called for Google Forms ingestion.

Column Standards (aligned with Airbyte/Singer/Meltano):
- source_system: Provider family (always "google_forms" for this module)
- connection_name: Account identifier (e.g., "google-forms-ipip120")
- object_type: Domain object (always "form_response" for this module)

Date filtering:
- Supports --since and --until CLI parameters
- Filters on lastSubmittedTime field
- If no --since provided, queries BigQuery for last sync timestamp
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from injest.adapters.google_forms import GoogleFormsResponsesStream
from lorchestra.injest_config import configure_injest

logger = logging.getLogger(__name__)

# Form ID mappings (identity -> form_id)
FORM_IDS = {
    "google_forms:ipip120_01": "1J8xJrialw_DhE0R0vzqXbQreUKlfVP3nBiqxaALwi5U",
    "google_forms:intake_01": "1VM48DV-cXpe0ZJwPzB293JunEdk2D9i5GCsh9zaPSCU",
    "google_forms:intake_02": "1xlyCrjz7iKXDiA4hkMLHIjdzTnK4EUkFCQeAfG-gQNU",
    "google_forms:followup": "1j6riXznQkO6BpmfBgXUDmulI9XEW7aSDHwClBOPaDHM",
}


def _get_last_sync_timestamp(bq_client, source_system: str, connection_name: str, object_type: str) -> Optional[str]:
    """
    Query BigQuery for the most recent object timestamp.

    Args:
        bq_client: BigQuery client
        source_system: Provider family (e.g., "google_forms")
        connection_name: Account identifier (e.g., "google-forms-ipip120")
        object_type: Object type (e.g., "form_response")

    Returns:
        ISO timestamp string of last sync, or None if no previous sync
    """
    import os
    dataset = os.environ.get("EVENTS_BQ_DATASET")

    query = f"""
        SELECT MAX(last_seen) as last_sync
        FROM `{dataset}.raw_objects`
        WHERE source_system = @source_system
          AND connection_name = @connection_name
          AND object_type = @object_type
    """

    from google.cloud import bigquery
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


def _parse_date_to_datetime(value: str) -> datetime:
    """Parse date string to datetime for InJest (always returns UTC-aware)."""
    # Handle relative dates like "-7d"
    if value.startswith("-") and value.endswith("d"):
        days = int(value[1:-1])
        return datetime.now(timezone.utc) - timedelta(days=days)
    # Handle ISO format
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    # Ensure timezone-aware (assume UTC if naive)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _ingest_google_forms(
    source_system: str,
    connection_name: str,
    identity: str,
    form_id: str,
    form_name: str,
    bq_client,
    since: Optional[str] = None,
    until: Optional[str] = None
):
    """
    Generic Google Forms ingestion function using InJest.

    Pattern:
    - 1 log_event() for ingest.completed with telemetry payload
    - 1 batch upsert_objects() for all responses (no per-object events)

    Args:
        source_system: Provider family (always "google_forms")
        connection_name: Account identifier (e.g., "google-forms-ipip120")
        identity: InJest identity key (e.g., "google_forms:ipip120_01")
        form_id: Google Form ID
        form_name: Human-readable form name for logging
        bq_client: BigQuery client for event emission
        since: Start date (ISO format, "-7d" relative, or None for last sync)
        until: End date (ISO format or None for now)
    """
    from lorchestra.stack_clients.event_client import log_event, upsert_objects
    from lorchestra.idem_keys import google_forms_idem_key
    import time

    # Ensure InJest is configured
    configure_injest()

    object_type = "form_response"
    run_id = f"google-forms-{form_name}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    # Auto-detect last sync if no since provided
    if since is None:
        logger.info("No --since provided, querying BigQuery for last sync...")
        last_sync = _get_last_sync_timestamp(bq_client, source_system, connection_name, object_type)
        if last_sync:
            since = last_sync
            logger.info(f"Found last sync: {since}")
        else:
            logger.info("No previous sync found, will extract all responses")

    # Parse dates for InJest
    since_dt = _parse_date_to_datetime(since) if since else None
    until_dt = _parse_date_to_datetime(until) if until else None

    logger.info(f"Starting Google Forms ingestion: {source_system}/{connection_name}, form={form_name}, run_id={run_id}")
    if since_dt:
        logger.info(f"Date filter: since={since_dt.isoformat()}")
    if until_dt:
        logger.info(f"Date filter: until={until_dt.isoformat()}")

    start_time = time.time()

    try:
        # Get stream from InJest (handles auth + API calls)
        stream = GoogleFormsResponsesStream(identity=identity, form_id=form_id)

        # Create idem_key function with new signature
        idem_key_fn = google_forms_idem_key(source_system, connection_name)

        # Batch upsert objects (no per-object events)
        logger.info(f"Batch upserting {form_name} responses to raw_objects...")

        # Count records for telemetry
        record_count = 0
        def count_and_yield():
            nonlocal record_count
            for record in stream.extract(since=since_dt, until=until_dt):
                record_count += 1
                yield record

        result = upsert_objects(
            objects=count_and_yield(),
            source_system=source_system,
            connection_name=connection_name,
            object_type=object_type,
            correlation_id=run_id,
            idem_key_fn=idem_key_fn,
            bq_client=bq_client,
        )

        # Log ingest.completed event with telemetry
        duration_seconds = time.time() - start_time
        log_event(
            event_type="ingest.completed",
            source_system=source_system,
            connection_name=connection_name,
            target_object_type=object_type,
            correlation_id=run_id,
            status="ok",
            payload={
                "records_extracted": record_count,
                "inserted": result.inserted,
                "updated": result.updated,
                "form_name": form_name,
                "form_id": form_id,
                "duration_seconds": round(duration_seconds, 2),
                "since": since_dt.isoformat() if since_dt else None,
                "until": until_dt.isoformat() if until_dt else None,
            },
            bq_client=bq_client,
        )

        logger.info(
            f"Google Forms ingestion complete: {record_count} {form_name} responses, run_id={run_id}, duration={duration_seconds:.2f}s"
        )

    except Exception as e:
        logger.error(f"Google Forms ingestion failed: {e}", exc_info=True)

        # Log ingest.failed event
        duration_seconds = time.time() - start_time
        log_event(
            event_type="ingest.failed",
            source_system=source_system,
            connection_name=connection_name,
            target_object_type=object_type,
            correlation_id=run_id,
            status="failed",
            error_message=str(e),
            payload={
                "error_type": type(e).__name__,
                "form_name": form_name,
                "form_id": form_id,
                "duration_seconds": round(duration_seconds, 2),
            },
            bq_client=bq_client,
        )

        raise


def job_ingest_google_forms_ipip120(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest IPIP-120 personality assessment responses.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_google_forms(
        source_system="google_forms",
        connection_name="google-forms-ipip120",
        identity="google_forms:ipip120_01",
        form_id=FORM_IDS["google_forms:ipip120_01"],
        form_name="ipip120",
        bq_client=bq_client,
        since=since,
        until=until,
    )


def job_ingest_google_forms_intake_01(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest intake form 01 responses.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_google_forms(
        source_system="google_forms",
        connection_name="google-forms-intake-01",
        identity="google_forms:intake_01",
        form_id=FORM_IDS["google_forms:intake_01"],
        form_name="intake_01",
        bq_client=bq_client,
        since=since,
        until=until,
    )


def job_ingest_google_forms_intake_02(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest intake form 02 responses.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_google_forms(
        source_system="google_forms",
        connection_name="google-forms-intake-02",
        identity="google_forms:intake_02",
        form_id=FORM_IDS["google_forms:intake_02"],
        form_name="intake_02",
        bq_client=bq_client,
        since=since,
        until=until,
    )


def job_ingest_google_forms_followup(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest followup form responses.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_google_forms(
        source_system="google_forms",
        connection_name="google-forms-followup",
        identity="google_forms:followup",
        form_id=FORM_IDS["google_forms:followup"],
        form_name="followup",
        bq_client=bq_client,
        since=since,
        until=until,
    )
