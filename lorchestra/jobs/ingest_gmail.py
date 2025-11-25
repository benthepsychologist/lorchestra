"""Gmail ingestion jobs for LifeOS.

This module uses InJest for Gmail extraction:
1. Calls injest.get_stream() to get auth-aware Gmail stream
2. Streams records to batch upsert (no per-object events)
3. Logs telemetry events (ingestion.completed) with small payloads

This is the ONLY place event_client is called for Gmail ingestion.

Date filtering:
- Supports --since and --until CLI parameters
- If no --since provided, queries BigQuery for last sync timestamp
- InJest handles Gmail query syntax internally
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
        source_system: Source system identifier (e.g., "tap-gmail--acct1-personal")
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


def _format_gmail_query(since: Optional[str] = None, until: Optional[str] = None) -> str:
    """
    Format Gmail search query from since/until parameters.

    Args:
        since: Start date (ISO format, relative like "-7d", or None for auto-detect)
        until: End date (ISO format or None for now)

    Returns:
        Gmail query string like "after:2024/01/01 before:2024/12/31"
    """
    parts = []

    if since:
        # Handle relative dates like "-7d"
        if since.startswith("-"):
            days = int(since[1:-1])  # Extract number from "-7d"
            since_date = datetime.now(timezone.utc) - timedelta(days=days)
            since_str = since_date.strftime("%Y/%m/%d")
        else:
            # Parse ISO format and convert to Gmail format
            since_date = datetime.fromisoformat(since.replace("Z", "+00:00"))
            since_str = since_date.strftime("%Y/%m/%d")
        parts.append(f"after:{since_str}")

    if until:
        until_date = datetime.fromisoformat(until.replace("Z", "+00:00"))
        until_str = until_date.strftime("%Y/%m/%d")
        parts.append(f"before:{until_str}")

    return " ".join(parts) if parts else ""


def _parse_date_to_datetime(value: str) -> datetime:
    """Parse date string to datetime for InJest."""
    # Handle relative dates like "-7d"
    if value.startswith("-") and value.endswith("d"):
        days = int(value[1:-1])
        return datetime.now(timezone.utc) - timedelta(days=days)
    # Handle ISO format
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _ingest_gmail(
    source_system: str,
    identity: str,
    account_id: str,
    bq_client,
    since: Optional[str] = None,
    until: Optional[str] = None
):
    """
    Generic Gmail ingestion function using InJest.

    Pattern:
    - 1 log_event() for ingestion.completed with telemetry payload
    - 1 batch upsert_objects() for all emails (no per-object events)

    Args:
        source_system: Source system identifier (e.g., "gmail--acct1-personal")
        identity: InJest identity key (e.g., "gmail:acct1")
        account_id: Account identifier for run_id (e.g., "acct1")
        bq_client: BigQuery client for event emission
        since: Start date (ISO format, "-7d" relative, or None for last sync)
        until: End date (ISO format or None for now)
    """
    from lorchestra.stack_clients.event_client import log_event, upsert_objects
    from lorchestra.idem_keys import gmail_idem_key
    import time

    # Ensure InJest is configured
    configure_injest()

    object_type = "email"
    run_id = f"gmail-{account_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

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

    logger.info(f"Starting Gmail ingestion: {source_system}, identity={identity}, run_id={run_id}")
    if since_dt:
        logger.info(f"Date filter: since={since_dt.isoformat()}")
    if until_dt:
        logger.info(f"Date filter: until={until_dt.isoformat()}")

    start_time = time.time()

    try:
        # Get stream from InJest (handles auth + API calls)
        stream = get_stream("gmail.messages", identity=identity)

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
            idem_key_fn=gmail_idem_key(source_system),
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
            f"Gmail ingestion complete: {record_count} records, run_id={run_id}, duration={duration_seconds:.2f}s"
        )

    except Exception as e:
        logger.error(f"Gmail ingestion failed: {e}", exc_info=True)

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


def job_ingest_gmail_acct1(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Gmail messages from acct1-personal using InJest.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_gmail("gmail--acct1-personal", "gmail:acct1", "acct1", bq_client, since=since, until=until)


def job_ingest_gmail_acct2(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Gmail messages from acct2-business1 using InJest.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_gmail("gmail--acct2-business1", "gmail:acct2", "acct2", bq_client, since=since, until=until)


def job_ingest_gmail_acct3(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Gmail messages from acct3-bfarmstrong using InJest.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    return _ingest_gmail("gmail--acct3-bfarmstrong", "gmail:acct3", "acct3", bq_client, since=since, until=until)
