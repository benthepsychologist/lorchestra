"""Exchange/Microsoft Graph Mail ingestion jobs for LifeOS.

This module uses InJest for Exchange extraction:
1. Calls injest.get_stream() to get auth-aware Exchange stream
2. Streams records to batch upsert (no per-object events)
3. Logs telemetry events (ingestion.completed) with small payloads

This is the ONLY place event_client is called for Exchange ingestion.

Date filtering:
- Supports --since and --until CLI parameters
- If no --since provided, queries BigQuery for last sync timestamp
- InJest handles Graph API date filtering internally
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from injest import get_stream
from lorchestra.injest_config import configure_injest

logger = logging.getLogger(__name__)


def _get_last_sync_timestamp(bq_client, source_system: str, object_type: str) -> Optional[str]:
    """
    Query BigQuery for the most recent object timestamp.

    This is our "state store" - no separate state files needed!

    Args:
        bq_client: BigQuery client
        source_system: Source system identifier (e.g., "tap-msgraph-mail--ben-mensio")
        object_type: Object type (e.g., "email")

    Returns:
        ISO timestamp string of last sync, or None if no previous sync
    """
    import os
    dataset = os.environ.get("EVENTS_BQ_DATASET")

    query = f"""
        SELECT MAX(last_seen) as last_sync
        FROM `{dataset}.raw_objects`
        WHERE source_system = @source_system
          AND object_type = @object_type
    """

    from google.cloud import bigquery
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_system", "STRING", source_system),
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


def _format_date_filter(since: Optional[str] = None, until: Optional[str] = None) -> dict:
    """
    Format date filter config for tap-msgraph-mail.

    Args:
        since: Start date (ISO format, relative like "-7d", or None for auto-detect)
        until: End date (ISO format or None for now)

    Returns:
        Dictionary with start_date and end_date config for tap-msgraph-mail
    """
    config_overrides = {}

    if since:
        # Handle relative dates like "-7d"
        if since.startswith("-"):
            days = int(since[1:-1])  # Extract number from "-7d"
            since_date = datetime.now(timezone.utc) - timedelta(days=days)
            since_str = since_date.isoformat()
        else:
            # Parse ISO format and ensure it has timezone
            if "T" not in since:
                # Date only (e.g., "2024-01-01") - add time and timezone
                since_str = f"{since}T00:00:00+00:00"
            else:
                since_date = datetime.fromisoformat(since.replace("Z", "+00:00"))
                since_str = since_date.isoformat()

        config_overrides["start_date"] = since_str

    if until:
        # Handle relative dates like "-7d"
        if until.startswith("-"):
            days = int(until[1:-1])  # Extract number from "-7d"
            until_date = datetime.now(timezone.utc) - timedelta(days=days)
            until_str = until_date.isoformat()
        else:
            # Parse ISO format and ensure it has timezone
            if "T" not in until:
                # Date only (e.g., "2024-01-31") - add end of day time and timezone
                until_str = f"{until}T23:59:59+00:00"
            else:
                until_date = datetime.fromisoformat(until.replace("Z", "+00:00"))
                until_str = until_date.isoformat()

        config_overrides["end_date"] = until_str

    return config_overrides


def _parse_date_to_datetime(value: str) -> datetime:
    """Parse date string to datetime for InJest."""
    # Handle relative dates like "-7d"
    if value.startswith("-") and value.endswith("d"):
        days = int(value[1:-1])
        return datetime.now(timezone.utc) - timedelta(days=days)
    # Handle date-only format
    if "T" not in value:
        return datetime.fromisoformat(f"{value}T00:00:00+00:00")
    # Handle ISO format
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _ingest_exchange(
    source_system: str,
    identity: str,
    account_id: str,
    bq_client,
    since: Optional[str] = None,
    until: Optional[str] = None
):
    """
    Generic Exchange ingestion function using InJest.

    Pattern:
    - 1 log_event() for ingestion.completed with telemetry payload
    - 1 batch upsert_objects() for all emails (no per-object events)

    Args:
        source_system: Source system identifier (e.g., "exchange--ben-mensio")
        identity: InJest identity key (e.g., "exchange:ben-mensio")
        account_id: Account identifier for run_id (e.g., "ben-mensio")
        bq_client: BigQuery client for event emission
        since: Start date (ISO format, "-7d" relative, or None for last sync)
        until: End date (ISO format or None for now)
    """
    from lorchestra.stack_clients.event_client import log_event, upsert_objects
    from lorchestra.idem_keys import msgraph_idem_key
    import time

    # Ensure InJest is configured
    configure_injest()

    object_type = "email"
    run_id = f"exchange-{account_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    # Auto-detect last sync if no since provided
    if since is None:
        logger.info("No --since provided, querying BigQuery for last sync...")
        last_sync = _get_last_sync_timestamp(bq_client, source_system, object_type)
        if last_sync:
            since = last_sync
            logger.info(f"Found last sync: {since}")
        else:
            logger.info("No previous sync found, will extract all messages")

    # Parse dates for InJest
    since_dt = _parse_date_to_datetime(since) if since else None
    until_dt = _parse_date_to_datetime(until) if until else None

    logger.info(f"Starting Exchange ingestion: {source_system}, identity={identity}, run_id={run_id}")
    if since_dt:
        logger.info(f"Date filter: since={since_dt.isoformat()}")
    if until_dt:
        logger.info(f"Date filter: until={until_dt.isoformat()}")

    start_time = time.time()

    try:
        # Get stream from InJest (handles auth + API calls)
        stream = get_stream("exchange.messages", identity=identity)

        # Batch upsert objects (no per-object events)
        logger.info("Batch upserting emails to raw_objects...")

        # Count records for telemetry
        record_count = 0
        def count_and_yield():
            nonlocal record_count
            for record in stream.extract(since=since_dt, until=until_dt):
                record_count += 1
                yield record

        upsert_objects(
            objects=count_and_yield(),
            source_system=source_system,
            object_type=object_type,
            correlation_id=run_id,
            idem_key_fn=msgraph_idem_key(source_system),
            bq_client=bq_client,
        )

        # Step 3: Log ingestion.completed event with telemetry
        duration_seconds = time.time() - start_time
        log_event(
            event_type="ingestion.completed",
            source_system=source_system,
            correlation_id=run_id,
            status="ok",
            payload={
                "records_extracted": record_count,
                "duration_seconds": round(duration_seconds, 2),
                "since": since_dt.isoformat() if since_dt else None,
                "until": until_dt.isoformat() if until_dt else None,
            },
            bq_client=bq_client,
        )

        logger.info(
            f"Exchange ingestion complete: {record_count} records, run_id={run_id}, duration={duration_seconds:.2f}s"
        )

    except Exception as e:
        logger.error(f"Exchange ingestion failed: {e}", exc_info=True)

        # Log ingestion.failed event
        duration_seconds = time.time() - start_time
        log_event(
            event_type="ingestion.failed",
            source_system=source_system,
            correlation_id=run_id,
            status="failed",
            error_message=str(e),
            payload={
                "error_type": type(e).__name__,
                "duration_seconds": round(duration_seconds, 2),
            },
            bq_client=bq_client,
        )

        raise


def job_ingest_exchange_ben_mensio(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Exchange messages from ben@mensiomentalhealth.com using InJest.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_exchange("exchange--ben-mensio", "exchange:ben-mensio", "ben-mensio", bq_client, since=since, until=until)


def job_ingest_exchange_booking_mensio(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Exchange messages from booking@mensiomentalhealth.com using InJest.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_exchange("exchange--booking-mensio", "exchange:booking-mensio", "booking-mensio", bq_client, since=since, until=until)


def job_ingest_exchange_info_mensio(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Exchange messages from info@mensiomentalhealth.com using InJest.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_exchange("exchange--info-mensio", "exchange:info-mensio", "info-mensio", bq_client, since=since, until=until)


def job_ingest_exchange_ben_efs(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Exchange messages from ben@ethicalfootprintsolutions.com using InJest.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_exchange("exchange--ben-efs", "exchange:ben-efs", "ben-efs", bq_client, since=since, until=until)
