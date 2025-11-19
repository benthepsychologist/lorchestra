"""
Minimal event client for writing events to BigQuery.

This module provides a single public function, emit(), which writes
event envelopes to a BigQuery events table.

Key principles:
- Callers choose event_type and payload
- event_client does NOT know about domain-specific schemas
- schema_ref is optional and purely informational
- No schema validation, policy enforcement, or auto-gov integration
- BigQuery is the only storage target (no JSONL backup in v0)

Configuration via environment variables:
- EVENTS_BQ_DATASET: BigQuery dataset name
- EVENTS_BQ_TABLE: BigQuery table name

Envelope format:
- event_id: UUID4 string
- event_type: Caller-provided (e.g., "email.received")
- source: Caller-provided (e.g., "ingester/gmail/acct1")
- schema_ref: Optional schema reference
- created_at: ISO 8601 UTC string (e.g., "2025-11-18T20:45:00.123456+00:00")
- correlation_id: Optional correlation ID for tracing
- subject_id: Optional subject identifier (PHI)
- payload: JSON object (dict)

Important:
- Pass payload as a dict, NOT a JSON string
- created_at is stored as STRING in BQ, not TIMESTAMP (for v0 simplicity)
- payload must be a JSON column in BQ, not STRING

Usage:
    from google.cloud import bigquery
    from lorchestra.stack_clients.event_client import emit

    client = bigquery.Client()
    emit(
        event_type="email.received",
        payload={"subject": "Test", "from": "user@example.com"},
        source="ingester/gmail/acct1-personal",
        bq_client=client
    )
"""

from typing import Any, Dict, Optional
from datetime import datetime, timezone
import uuid
import os


def build_envelope(
    *,
    event_type: str,
    payload: Dict[str, Any],
    source: str,
    schema_ref: Optional[str] = None,
    correlation_id: Optional[str] = None,
    subject_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a standard event envelope.

    Args:
        event_type: Type of event (e.g. "email.received")
        payload: Event-specific data
        source: Source system/component (e.g. "ingester/gmail/acct1")
        schema_ref: Optional schema reference (e.g. "email.v1")
        correlation_id: Optional correlation ID for tracing
        subject_id: Optional subject identifier (PHI)

    Returns:
        Event envelope dict with all required fields

    Raises:
        ValueError: If event_type or source are empty

    Example:
        >>> envelope = build_envelope(
        ...     event_type="email.received",
        ...     payload={"subject": "Test"},
        ...     source="ingester/gmail/acct1"
        ... )
        >>> assert "event_id" in envelope
        >>> assert envelope["event_type"] == "email.received"
    """
    # Validate required fields
    if not event_type or not isinstance(event_type, str):
        raise ValueError("event_type must be a non-empty string")
    if not source or not isinstance(source, str):
        raise ValueError("source must be a non-empty string")

    # Generate envelope
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "source": source,
        "schema_ref": schema_ref,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": correlation_id,
        "subject_id": subject_id,
        "payload": payload,
    }


def _get_bq_table_ref(bq_client) -> str:
    """
    Get fully-qualified BigQuery table reference from environment.

    Reads EVENTS_BQ_DATASET and EVENTS_BQ_TABLE environment variables.

    Args:
        bq_client: BigQuery client (used for project context if needed)

    Returns:
        Table reference string: "dataset.table"

    Raises:
        RuntimeError: If required environment variables are missing
    """
    dataset = os.environ.get("EVENTS_BQ_DATASET")
    table = os.environ.get("EVENTS_BQ_TABLE")

    if not dataset or not table:
        raise RuntimeError(
            "Missing required environment variables: "
            "EVENTS_BQ_DATASET and/or EVENTS_BQ_TABLE"
        )

    return f"{dataset}.{table}"


def _write_to_bq(envelope: Dict[str, Any], bq_client) -> None:
    """
    Write event envelope to BigQuery events table.

    Args:
        envelope: Event envelope dict from build_envelope()
        bq_client: google.cloud.bigquery.Client instance

    Raises:
        RuntimeError: If BigQuery write fails
    """
    table_ref = _get_bq_table_ref(bq_client)

    # Use insert_rows_json for single row insert
    # Pass envelope as dict - BQ client handles JSON serialization
    errors = bq_client.insert_rows_json(table_ref, [envelope])

    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")


def emit(
    event_type: str,
    payload: Dict[str, Any],
    *,
    source: str,
    bq_client,
    schema_ref: Optional[str] = None,
    correlation_id: Optional[str] = None,
    subject_id: Optional[str] = None,
) -> None:
    """
    Emit an event to BigQuery.

    This is the primary interface for writing events to the event store.
    Callers are responsible for choosing appropriate event_type and payload.

    Args:
        event_type: Type of event (e.g. "email.received")
        payload: Event-specific data as a dict
        source: Source system/component (e.g. "ingester/gmail/acct1")
        bq_client: google.cloud.bigquery.Client instance
        schema_ref: Optional schema reference (e.g. "email.v1")
        correlation_id: Optional correlation ID for tracing
        subject_id: Optional subject identifier (PHI)

    Raises:
        ValueError: If event_type or source are invalid
        RuntimeError: If BigQuery write fails or env vars missing

    Example:
        >>> from google.cloud import bigquery
        >>> client = bigquery.Client()
        >>> emit(
        ...     event_type="email.received",
        ...     payload={"subject": "Hello", "from": "test@example.com"},
        ...     source="ingester/gmail/acct1",
        ...     bq_client=client
        ... )
    """
    envelope = build_envelope(
        event_type=event_type,
        payload=payload,
        source=source,
        schema_ref=schema_ref,
        correlation_id=correlation_id,
        subject_id=subject_id,
    )

    _write_to_bq(envelope, bq_client)
