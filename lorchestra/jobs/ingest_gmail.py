"""Gmail ingestion jobs for LifeOS.

This module demonstrates the clean three-layer architecture:
1. Calls ingestor.extract_to_jsonl() to run Meltano → JSONL
2. Reads JSONL records
3. Emits events via event_client to BigQuery (event_log + raw_objects)

This is the ONLY place event_client is called for Gmail ingestion.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add ingestor to Python path if not installed
ingestor_path = Path("/workspace/ingestor")
if ingestor_path.exists() and str(ingestor_path) not in sys.path:
    sys.path.insert(0, str(ingestor_path))

logger = logging.getLogger(__name__)


def job_ingest_gmail_acct1(bq_client):
    """
    Ingest Gmail messages from acct1-personal.

    Args:
        bq_client: BigQuery client for event emission

    This job demonstrates the clean three-layer pattern:
    - Calls ingestor (Meltano wrapper)
    - Reads JSONL
    - Emits events via event_client
    """
    # Import here to avoid circular dependencies
    from ingestor.extractors import extract_to_jsonl, iter_jsonl_records
    from lorchestra.stack_clients.event_client import emit

    # Configuration
    tap_name = "tap-gmail--acct1-personal"
    source_system = tap_name
    object_type = "email"
    run_id = f"gmail-acct1-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    logger.info(f"Starting Gmail ingestion: {tap_name}, run_id={run_id}")

    try:
        # Step 1: Get JSONL from ingestor (Meltano wrapper)
        jsonl_path = extract_to_jsonl(tap_name, run_id)
        logger.info(f"JSONL extracted to: {jsonl_path}")

        # Step 2: Read records and emit events
        record_count = 0
        for record in iter_jsonl_records(jsonl_path):
            # Emit event (writes to event_log + raw_objects)
            # The event_client will compute the idem_key internally
            emit(
                event_type="gmail.email.received",
                payload=record,
                source=source_system,
                object_type=object_type,
                bq_client=bq_client,
                correlation_id=run_id
            )

            record_count += 1

        logger.info(
            f"Gmail ingestion complete: {record_count} records, run_id={run_id}"
        )

    except Exception as e:
        logger.error(f"Gmail ingestion failed: {e}", exc_info=True)
        raise


def job_ingest_gmail_acct2(bq_client):
    """
    Ingest Gmail messages from acct2-business1.

    Args:
        bq_client: BigQuery client for event emission
    """
    from ingestor.extractors import extract_to_jsonl, iter_jsonl_records
    from lorchestra.stack_clients.event_client import emit

    tap_name = "tap-gmail--acct2-business1"
    source_system = tap_name
    object_type = "email"
    run_id = f"gmail-acct2-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    logger.info(f"Starting Gmail ingestion: {tap_name}, run_id={run_id}")

    try:
        jsonl_path = extract_to_jsonl(tap_name, run_id)
        logger.info(f"JSONL extracted to: {jsonl_path}")

        record_count = 0
        for record in iter_jsonl_records(jsonl_path):
            emit(
                event_type="gmail.email.received",
                payload=record,
                source=source_system,
                object_type=object_type,
                bq_client=bq_client,
                correlation_id=run_id
            )
            record_count += 1

        logger.info(
            f"Gmail ingestion complete: {record_count} records, run_id={run_id}"
        )

    except Exception as e:
        logger.error(f"Gmail ingestion failed: {e}", exc_info=True)
        raise


def job_ingest_gmail_acct3(bq_client):
    """
    Ingest Gmail messages from acct3-bfarmstrong.

    Args:
        bq_client: BigQuery client for event emission
    """
    from ingestor.extractors import extract_to_jsonl, iter_jsonl_records
    from lorchestra.stack_clients.event_client import emit

    tap_name = "tap-gmail--acct3-bfarmstrong"
    source_system = tap_name
    object_type = "email"
    run_id = f"gmail-acct3-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    logger.info(f"Starting Gmail ingestion: {tap_name}, run_id={run_id}")

    try:
        jsonl_path = extract_to_jsonl(tap_name, run_id)
        logger.info(f"JSONL extracted to: {jsonl_path}")

        record_count = 0
        for record in iter_jsonl_records(jsonl_path):
            emit(
                event_type="gmail.email.received",
                payload=record,
                source=source_system,
                object_type=object_type,
                bq_client=bq_client,
                correlation_id=run_id
            )
            record_count += 1

        logger.info(
            f"Gmail ingestion complete: {record_count} records, run_id={run_id}"
        )

    except Exception as e:
        logger.error(f"Gmail ingestion failed: {e}", exc_info=True)
        raise
