"""Exchange/Microsoft Graph Mail ingestion jobs for LifeOS.

This module implements the same refactored event client pattern as Gmail:
1. Calls ingestor.extract_to_jsonl() to run Meltano → JSONL
2. Streams JSONL records to batch upsert (no per-object events)
3. Logs telemetry events (ingestion.completed) with small payloads

This is the ONLY place event_client is called for Exchange ingestion.

Date filtering:
- Supports --since and --until CLI parameters
- If no --since provided, queries BigQuery for last sync timestamp
- Uses ISO 8601 date format (Graph API standard)
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

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
        Dictionary with start_date config for tap-msgraph-mail
    """
    config_overrides = {}

    if since:
        # Handle relative dates like "-7d"
        if since.startswith("-"):
            days = int(since[1:-1])  # Extract number from "-7d"
            since_date = datetime.now(timezone.utc) - timedelta(days=days)
            since_str = since_date.strftime("%Y-%m-%d")
        else:
            # Parse ISO format
            since_date = datetime.fromisoformat(since.replace("Z", "+00:00"))
            since_str = since_date.strftime("%Y-%m-%d")

        config_overrides["start_date"] = since_str

    # Note: tap-msgraph-mail doesn't support end_date filtering
    # It always syncs up to "now"

    return config_overrides


def _ingest_exchange(
    tap_name: str,
    account_id: str,
    bq_client,
    since: Optional[str] = None,
    until: Optional[str] = None
):
    """
    Generic Exchange ingestion function with date filtering.

    Refactored pattern:
    - 1 log_event() for ingestion.completed with telemetry payload
    - 1 batch upsert_objects() for all emails (no per-object events)

    Args:
        tap_name: Meltano tap name (e.g., "tap-msgraph-mail--ben-mensio")
        account_id: Account identifier for run_id (e.g., "ben-mensio")
        bq_client: BigQuery client for event emission
        since: Start date (ISO format, "-7d" relative, or None for last sync)
        until: End date (ISO format or None for now) - Note: Graph API doesn't support end_date
    """
    from ingestor.extractors import extract_to_jsonl, iter_jsonl_records
    from lorchestra.stack_clients.event_client import log_event, upsert_objects
    from lorchestra.idem_keys import msgraph_idem_key
    import time

    source_system = tap_name
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
            logger.info("No previous sync found, will use default from meltano.yml")

    # Build date filter config
    config_overrides = _format_date_filter(since, until)
    filter_display = config_overrides.get("start_date", "default")

    logger.info(f"Starting Exchange ingestion: {tap_name}, run_id={run_id}")
    if config_overrides:
        logger.info(f"Date filter: start_date={filter_display}")

    start_time = time.time()

    try:
        # Step 1: Get JSONL from ingestor (Meltano wrapper)
        jsonl_dir = extract_to_jsonl(tap_name, run_id, config_overrides=config_overrides)
        logger.info(f"JSONL extracted to: {jsonl_dir}")

        # target-jsonl creates a directory with separate files per stream
        # For Exchange, we want the messages stream
        jsonl_path = jsonl_dir / "messages.jsonl"
        if not jsonl_path.exists():
            raise RuntimeError(f"Expected messages.jsonl not found in {jsonl_dir}")

        # Step 2: Batch upsert objects (no per-object events)
        logger.info("Batch upserting emails to raw_objects...")

        # Count records for telemetry (we need to track this)
        record_count = 0
        def count_and_yield():
            nonlocal record_count
            for record in iter_jsonl_records(jsonl_path):
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
                "date_filter": filter_display,
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
    Ingest Exchange messages from ben@getmensio.com.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now) - Note: Graph API doesn't support end_date
    """
    return _ingest_exchange("tap-msgraph-mail--ben-mensio", "ben-mensio", bq_client, since=since, until=until)


def job_ingest_exchange_booking_mensio(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Exchange messages from booking@getmensio.com.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now) - Note: Graph API doesn't support end_date
    """
    return _ingest_exchange("tap-msgraph-mail--booking-mensio", "booking-mensio", bq_client, since=since, until=until)


def job_ingest_exchange_info_mensio(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Exchange messages from info@getmensio.com.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now) - Note: Graph API doesn't support end_date
    """
    return _ingest_exchange("tap-msgraph-mail--info-mensio", "info-mensio", bq_client, since=since, until=until)


def job_ingest_exchange_ben_efs(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Exchange messages from ben@ethicalfootprintsolutions.com.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now) - Note: Graph API doesn't support end_date
    """
    return _ingest_exchange("tap-msgraph-mail--ben-efs", "ben-efs", bq_client, since=since, until=until)
