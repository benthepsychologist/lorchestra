"""Gmail ingestion jobs for LifeOS.

This module demonstrates the clean three-layer architecture:
1. Calls ingestor.extract_to_jsonl() to run Meltano → JSONL
2. Reads JSONL records
3. Emits events via event_client to BigQuery (event_log + raw_objects)

This is the ONLY place event_client is called for Gmail ingestion.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add ingestor to Python path if not installed
ingestor_path = Path("/workspace/ingestor")
if ingestor_path.exists() and str(ingestor_path) not in sys.path:
    sys.path.insert(0, str(ingestor_path))

logger = logging.getLogger(__name__)


def _ingest_gmail(tap_name: str, account_id: str, bq_client):
    """
    Generic Gmail ingestion function.

    Args:
        tap_name: Meltano tap name (e.g., "tap-gmail--acct1-personal")
        account_id: Account identifier for run_id (e.g., "acct1")
        bq_client: BigQuery client for event emission
    """
    from ingestor.extractors import extract_to_jsonl, iter_jsonl_records
    from lorchestra.stack_clients.event_client import emit

    source_system = tap_name
    object_type = "email"
    run_id = f"gmail-{account_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    logger.info(f"Starting Gmail ingestion: {tap_name}, run_id={run_id}")

    try:
        # Step 1: Get JSONL from ingestor (Meltano wrapper)
        jsonl_path = extract_to_jsonl(tap_name, run_id)
        logger.info(f"JSONL extracted to: {jsonl_path}")

        # Step 2: Read records and emit events
        record_count = 0
        for record in iter_jsonl_records(jsonl_path):
            emit(
                event_type="email.received",
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


def job_ingest_gmail_acct1(bq_client):
    """
    Ingest Gmail messages from acct1-personal.

    Args:
        bq_client: BigQuery client for event emission
    """
    return _ingest_gmail("tap-gmail--acct1-personal", "acct1", bq_client)


def job_ingest_gmail_acct2(bq_client):
    """
    Ingest Gmail messages from acct2-business1.

    Args:
        bq_client: BigQuery client for event emission
    """
    return _ingest_gmail("tap-gmail--acct2-business1", "acct2", bq_client)


def job_ingest_gmail_acct3(bq_client):
    """
    Ingest Gmail messages from acct3-bfarmstrong.

    Args:
        bq_client: BigQuery client for event emission
    """
    return _ingest_gmail("tap-gmail--acct3-bfarmstrong", "acct3", bq_client)
