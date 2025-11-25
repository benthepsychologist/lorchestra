"""Dataverse ingestion jobs for Mensio CRM.

This module uses InJest for Dataverse extraction:
1. Calls injest.get_stream() to get auth-aware Dataverse stream
2. Streams records to batch upsert (no per-object events)
3. Logs telemetry events (ingest.completed) with small payloads

Target entities:
- contacts: Client contact records
- cre92_clientsessions: Therapy session records
- cre92_clientreports: Progress reports

This is the ONLY place event_client is called for Dataverse ingestion.

Column Standards (aligned with Airbyte/Singer/Meltano):
- source_system: Provider family (always "dataverse" for this module)
- connection_name: Account identifier (e.g., "dataverse-clinic")
- object_type: Domain object (e.g., "contact", "session", "report")

Date filtering:
- Supports --since and --until CLI parameters
- Filters on modifiedon field via OData query
- If no --since provided, queries BigQuery for last sync timestamp
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from injest import get_stream
from lorchestra.injest_config import configure_injest

logger = logging.getLogger(__name__)


def _get_last_sync_timestamp(bq_client, source_system: str, connection_name: str, object_type: str) -> Optional[str]:
    """
    Query BigQuery for the most recent object timestamp.

    Args:
        bq_client: BigQuery client
        source_system: Provider family (e.g., "dataverse")
        connection_name: Account identifier (e.g., "dataverse-clinic")
        object_type: Object type (e.g., "contact", "session")

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
    """Parse date string to datetime for InJest."""
    # Handle relative dates like "-7d"
    if value.startswith("-") and value.endswith("d"):
        days = int(value[1:-1])
        return datetime.now(timezone.utc) - timedelta(days=days)
    # Handle ISO format
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _ingest_dataverse(
    source_system: str,
    connection_name: str,
    identity: str,
    entity: str,
    object_type: str,
    pk_field: str,
    bq_client,
    since: Optional[str] = None,
    until: Optional[str] = None
):
    """
    Generic Dataverse ingestion function using InJest.

    Pattern:
    - 1 log_event() for ingest.completed with telemetry payload
    - 1 batch upsert_objects() for all records (no per-object events)

    Args:
        source_system: Provider family (always "dataverse")
        connection_name: Account identifier (e.g., "dataverse-clinic")
        identity: InJest identity key (e.g., "dataverse:clinic")
        entity: OData entity name (e.g., "contacts", "cre92_clientsessions")
        object_type: Object type for BigQuery (e.g., "contact", "session")
        pk_field: Primary key field name (e.g., "contactid")
        bq_client: BigQuery client for event emission
        since: Start date (ISO format, "-7d" relative, or None for last sync)
        until: End date (ISO format or None for now)
    """
    from lorchestra.stack_clients.event_client import log_event, upsert_objects
    from lorchestra.idem_keys import dataverse_idem_key
    import time

    # Ensure InJest is configured
    configure_injest()

    run_id = f"dataverse-{entity}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    # Auto-detect last sync if no since provided
    if since is None:
        logger.info("No --since provided, querying BigQuery for last sync...")
        last_sync = _get_last_sync_timestamp(bq_client, source_system, connection_name, object_type)
        if last_sync:
            since = last_sync
            logger.info(f"Found last sync: {since}")
        else:
            logger.info("No previous sync found, will extract all records")

    # Parse dates for InJest
    since_dt = _parse_date_to_datetime(since) if since else None
    until_dt = _parse_date_to_datetime(until) if until else None

    logger.info(f"Starting Dataverse ingestion: {source_system}/{connection_name}, entity={entity}, run_id={run_id}")
    if since_dt:
        logger.info(f"Date filter: since={since_dt.isoformat()}")
    if until_dt:
        logger.info(f"Date filter: until={until_dt.isoformat()}")

    start_time = time.time()

    try:
        # Get stream from InJest (handles auth + API calls)
        stream = get_stream(f"dataverse.{entity}", identity=identity)

        # Create idem_key function with new signature
        idem_key_fn = dataverse_idem_key(source_system, connection_name, object_type, pk_field)

        # Batch upsert objects (no per-object events)
        logger.info(f"Batch upserting {entity} to raw_objects...")

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
                "entity": entity,
                "duration_seconds": round(duration_seconds, 2),
                "since": since_dt.isoformat() if since_dt else None,
                "until": until_dt.isoformat() if until_dt else None,
            },
            bq_client=bq_client,
        )

        logger.info(
            f"Dataverse ingestion complete: {record_count} {entity} records, run_id={run_id}, duration={duration_seconds:.2f}s"
        )

    except Exception as e:
        logger.error(f"Dataverse ingestion failed: {e}", exc_info=True)

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
                "entity": entity,
                "duration_seconds": round(duration_seconds, 2),
            },
            bq_client=bq_client,
        )

        raise


def job_ingest_dataverse_contacts(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Dataverse contacts from Mensio CRM.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_dataverse(
        source_system="dataverse",
        connection_name="dataverse-clinic",
        identity="dataverse:clinic",
        entity="contacts",
        object_type="contact",
        pk_field="contactid",
        bq_client=bq_client,
        since=since,
        until=until,
    )


def job_ingest_dataverse_sessions(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Dataverse cre92_clientsessions from Mensio CRM.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_dataverse(
        source_system="dataverse",
        connection_name="dataverse-clinic",
        identity="dataverse:clinic",
        entity="cre92_clientsessions",
        object_type="session",
        pk_field="cre92_clientsessionid",
        bq_client=bq_client,
        since=since,
        until=until,
    )


def job_ingest_dataverse_reports(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Dataverse cre92_clientreports from Mensio CRM.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_dataverse(
        source_system="dataverse",
        connection_name="dataverse-clinic",
        identity="dataverse:clinic",
        entity="cre92_clientreports",
        object_type="report",
        pk_field="cre92_clientreportid",
        bq_client=bq_client,
        since=since,
        until=until,
    )
