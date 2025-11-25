"""Stripe ingestion jobs.

This module uses InJest for Stripe extraction:
1. Calls InJest Stripe stream adapters for auth-aware API access
2. Streams records to batch upsert (no per-object events)
3. Logs telemetry events (ingest.completed) with small payloads

Target streams:
- customers: Customer records (~150)
- invoices: Invoice records with line items (~300)
- payment_intents: Payment intent records (~2600+)
- refunds: Refund records (~30)

This is the ONLY place event_client is called for Stripe ingestion.

Column Standards (aligned with Airbyte/Singer/Meltano):
- source_system: Provider family (always "stripe" for this module)
- connection_name: Account identifier (e.g., "stripe-mensio")
- object_type: Domain object (customer, invoice, payment_intent, refund)

Date filtering:
- Supports --since and --until CLI parameters
- Filters on 'created' timestamp via Stripe API
- If no --since provided, queries BigQuery for last sync timestamp
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from lorchestra.injest_config import configure_injest

logger = logging.getLogger(__name__)


def _get_last_sync_timestamp(bq_client, source_system: str, connection_name: str, object_type: str) -> Optional[str]:
    """
    Query BigQuery for the most recent object timestamp.

    Args:
        bq_client: BigQuery client
        source_system: Provider family (e.g., "stripe")
        connection_name: Account identifier (e.g., "stripe-mensio")
        object_type: Object type (e.g., "customer", "invoice")

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


def _ingest_stripe_stream(
    source_system: str,
    connection_name: str,
    identity: str,
    stream_class,
    object_type: str,
    bq_client,
    since: Optional[str] = None,
    until: Optional[str] = None
):
    """
    Generic Stripe ingestion function using InJest.

    Pattern:
    - 1 log_event() for ingest.completed with telemetry payload
    - 1 batch upsert_objects() for all records (no per-object events)

    Args:
        source_system: Provider family (always "stripe")
        connection_name: Account identifier (e.g., "stripe-mensio")
        identity: InJest identity key (e.g., "stripe:mensio")
        stream_class: InJest stream class (e.g., StripeCustomersStream)
        object_type: Object type for BigQuery (e.g., "customer", "invoice")
        bq_client: BigQuery client for event emission
        since: Start date (ISO format, "-7d" relative, or None for last sync)
        until: End date (ISO format or None for now)
    """
    from lorchestra.stack_clients.event_client import log_event, upsert_objects
    from lorchestra.idem_keys import stripe_idem_key
    import time

    # Ensure InJest is configured
    configure_injest()

    run_id = f"stripe-{object_type}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

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

    logger.info(f"Starting Stripe ingestion: {source_system}/{connection_name}, object_type={object_type}, run_id={run_id}")
    if since_dt:
        logger.info(f"Date filter: since={since_dt.isoformat()}")
    if until_dt:
        logger.info(f"Date filter: until={until_dt.isoformat()}")

    start_time = time.time()

    try:
        # Get stream from InJest (handles auth + API calls)
        stream = stream_class(identity)

        # Create idem_key function
        idem_key_fn = stripe_idem_key(source_system, connection_name, object_type)

        # Batch upsert objects (no per-object events)
        logger.info(f"Batch upserting {object_type}s to raw_objects...")

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
                "object_type": object_type,
                "duration_seconds": round(duration_seconds, 2),
                "since": since_dt.isoformat() if since_dt else None,
                "until": until_dt.isoformat() if until_dt else None,
            },
            bq_client=bq_client,
        )

        logger.info(
            f"Stripe ingestion complete: {record_count} {object_type}s, run_id={run_id}, duration={duration_seconds:.2f}s"
        )

    except Exception as e:
        logger.error(f"Stripe ingestion failed: {e}", exc_info=True)

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
                "object_type": object_type,
                "duration_seconds": round(duration_seconds, 2),
            },
            bq_client=bq_client,
        )

        raise


def job_ingest_stripe_customers(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Stripe customers from Mensio account.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    from injest.adapters.stripe_customers import StripeCustomersStream

    return _ingest_stripe_stream(
        source_system="stripe",
        connection_name="stripe-mensio",
        identity="stripe:mensio",
        stream_class=StripeCustomersStream,
        object_type="customer",
        bq_client=bq_client,
        since=since,
        until=until,
    )


def job_ingest_stripe_invoices(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Stripe invoices from Mensio account.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    from injest.adapters.stripe_invoices import StripeInvoicesStream

    return _ingest_stripe_stream(
        source_system="stripe",
        connection_name="stripe-mensio",
        identity="stripe:mensio",
        stream_class=StripeInvoicesStream,
        object_type="invoice",
        bq_client=bq_client,
        since=since,
        until=until,
    )


def job_ingest_stripe_payment_intents(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Stripe payment intents from Mensio account.

    Note: This is the largest stream (~2600+ records).

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    from injest.adapters.stripe_payment_intents import StripePaymentIntentsStream

    return _ingest_stripe_stream(
        source_system="stripe",
        connection_name="stripe-mensio",
        identity="stripe:mensio",
        stream_class=StripePaymentIntentsStream,
        object_type="payment_intent",
        bq_client=bq_client,
        since=since,
        until=until,
    )


def job_ingest_stripe_refunds(bq_client, since: Optional[str] = None, until: Optional[str] = None, **kwargs):
    """
    Ingest Stripe refunds from Mensio account.

    Args:
        bq_client: BigQuery client for event emission
        since: Start date (ISO, "-7d" relative, or None to auto-detect from BigQuery)
        until: End date (ISO or None for now)
    """
    from injest.adapters.stripe_refunds import StripeRefundsStream

    return _ingest_stripe_stream(
        source_system="stripe",
        connection_name="stripe-mensio",
        identity="stripe:mensio",
        stream_class=StripeRefundsStream,
        object_type="refund",
        bq_client=bq_client,
        since=since,
        until=until,
    )
