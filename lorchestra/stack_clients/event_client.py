"""
Event client for writing to BigQuery event_log and raw_objects tables.

This module implements a two-table pattern:
1. event_log: WAL of event envelopes (audit trail, no payload)
2. raw_objects: Deduped object store (keyed by idem_key, with payload)

Architecture:
- Events are immutable audit records (who did what when)
- Objects are deduplicated data (what exists, with first/last seen)
- idem_key provides idempotency: re-ingesting same data is safe

Key principles:
- Callers choose event_type and payload
- event_client generates event_id (unique per call) and idem_key (content-based)
- No schema validation or policy enforcement
- BigQuery is the only storage target (no JSONL backup in v0)

Configuration via environment variables:
- EVENTS_BQ_DATASET: BigQuery dataset name
- EVENT_LOG_TABLE: Event log table name (default: "event_log")
- RAW_OBJECTS_TABLE: Raw objects table name (default: "raw_objects")

Usage:
    from google.cloud import bigquery
    from lorchestra.stack_clients.event_client import emit

    client = bigquery.Client()
    emit(
        event_type="email.received",
        payload={"id": "msg123", "subject": "Test"},
        source="tap-gmail--acct1-personal",
        object_type="email",
        bq_client=client
    )

This will:
1. Insert envelope into event_log (with event_id, idem_key)
2. MERGE payload into raw_objects (dedup by idem_key)
"""

from typing import Any, Dict, Optional
from datetime import datetime, timezone
import hashlib
import json
import uuid
import os

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None  # Allow import without BigQuery SDK for testing


def generate_idem_key(
    source: str,
    object_type: str,
    payload: Dict[str, Any]
) -> str:
    """
    Generate deterministic idempotency key from object content.

    The idem_key is content-addressable and deterministic:
    - Same object extracted multiple times → same idem_key
    - Different objects → different idem_key

    Strategy:
    1. Try to use natural ID from payload (e.g., message_id, id)
    2. Fall back to content hash if no natural ID exists

    Args:
        source: Source system (e.g., "tap-gmail--acct1-personal")
        object_type: Object type (e.g., "email", "questionnaire_response")
        payload: The object data

    Returns:
        Deterministic idem_key string

    Examples:
        >>> # Gmail message with natural ID
        >>> generate_idem_key("tap-gmail", "email", {"id": "msg123", "subject": "Hi"})
        'email:tap-gmail:msg123'

        >>> # Object without natural ID (uses content hash)
        >>> generate_idem_key("tap-forms", "response", {"answer": "yes"})
        'response:tap-forms:a3f2...'
    """
    # Try common natural ID fields
    natural_id = (
        payload.get('id') or
        payload.get('message_id') or
        payload.get('external_id') or
        payload.get('uuid')
    )

    if natural_id:
        # Use natural ID from the source system
        return f"{object_type}:{source}:{natural_id}"
    else:
        # Fall back to content hash
        # Sort keys for deterministic JSON representation
        payload_str = json.dumps(payload, sort_keys=True, separators=(',', ':'))
        content_hash = hashlib.sha256(payload_str.encode()).hexdigest()[:16]
        return f"{object_type}:{source}:{content_hash}"


def build_envelope(
    *,
    event_type: str,
    source: str,
    object_type: str,
    idem_key: str,
    correlation_id: Optional[str] = None,
    subject_id: Optional[str] = None,
    status: str = "ok",
    error_message: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build event envelope for event_log table.

    Note: This envelope does NOT contain the payload.
    Payload goes to raw_objects table instead.

    Args:
        event_type: Type of event (e.g., "email.received")
        source: Source system (e.g., "tap-gmail--acct1-personal")
        object_type: Object type (e.g., "email", "questionnaire_response")
        idem_key: Idempotency key (references raw_objects)
        correlation_id: Optional correlation ID for tracing
        subject_id: Optional subject identifier (PHI)
        status: Event status ("ok" | "error")
        error_message: Error message if status="error"

    Returns:
        Event envelope dict for event_log table

    Raises:
        ValueError: If required fields are invalid

    Example:
        >>> envelope = build_envelope(
        ...     event_type="email.received",
        ...     source="tap-gmail",
        ...     object_type="email",
        ...     idem_key="email:tap-gmail:msg123"
        ... )
        >>> assert "event_id" in envelope
        >>> assert "payload" not in envelope  # Payload goes to raw_objects!
    """
    # Validate required fields
    if not event_type or not isinstance(event_type, str):
        raise ValueError("event_type must be a non-empty string")
    if not source or not isinstance(source, str):
        raise ValueError("source must be a non-empty string")
    if not object_type or not isinstance(object_type, str):
        raise ValueError("object_type must be a non-empty string")
    if not idem_key or not isinstance(idem_key, str):
        raise ValueError("idem_key must be a non-empty string")

    # Generate envelope (NO PAYLOAD - that goes to raw_objects)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "source_system": source,
        "object_type": object_type,
        "idem_key": idem_key,
        "correlation_id": correlation_id,
        "subject_id": subject_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "error_message": error_message,
    }


def _get_event_log_table_ref(bq_client) -> str:
    """
    Get fully-qualified BigQuery event_log table reference from environment.

    Args:
        bq_client: BigQuery client (used for project context if needed)

    Returns:
        Table reference string: "dataset.event_log"

    Raises:
        RuntimeError: If required environment variables are missing
    """
    dataset = os.environ.get("EVENTS_BQ_DATASET")
    table = os.environ.get("EVENT_LOG_TABLE", "event_log")

    if not dataset:
        raise RuntimeError("Missing required environment variable: EVENTS_BQ_DATASET")

    return f"{dataset}.{table}"


def _get_raw_objects_table_ref(bq_client) -> str:
    """
    Get fully-qualified BigQuery raw_objects table reference from environment.

    Args:
        bq_client: BigQuery client (used for project context if needed)

    Returns:
        Table reference string: "dataset.raw_objects"

    Raises:
        RuntimeError: If required environment variables are missing
    """
    dataset = os.environ.get("EVENTS_BQ_DATASET")
    table = os.environ.get("RAW_OBJECTS_TABLE", "raw_objects")

    if not dataset:
        raise RuntimeError("Missing required environment variable: EVENTS_BQ_DATASET")

    return f"{dataset}.{table}"


def _write_to_event_log(envelope: Dict[str, Any], bq_client) -> None:
    """
    Write event envelope to event_log table.

    This is the audit trail - every emit() call creates one row.

    Args:
        envelope: Event envelope dict from build_envelope()
        bq_client: google.cloud.bigquery.Client instance

    Raises:
        RuntimeError: If BigQuery write fails
    """
    table_ref = _get_event_log_table_ref(bq_client)

    # Simple insert - every event is a new row
    errors = bq_client.insert_rows_json(table_ref, [envelope])

    if errors:
        raise RuntimeError(f"event_log insert failed: {errors}")


def _write_to_raw_objects(
    idem_key: str,
    source: str,
    object_type: str,
    payload: Dict[str, Any],
    bq_client
) -> None:
    """
    Write or update object in raw_objects table using MERGE (dedup).

    This provides idempotency - same object ingested multiple times
    only creates one row, with updated last_seen timestamp.

    Args:
        idem_key: Idempotency key (primary key)
        source: Source system
        object_type: Object type
        payload: The object data (must contain natural ID if available)
        bq_client: google.cloud.bigquery.Client instance

    Raises:
        RuntimeError: If BigQuery write fails
    """
    table_ref = _get_raw_objects_table_ref(bq_client)
    now = datetime.now(timezone.utc).isoformat()

    # Extract external_id from payload (natural key)
    external_id = (
        payload.get('id') or
        payload.get('message_id') or
        payload.get('external_id') or
        payload.get('uuid')
    )

    # Use MERGE to implement idempotent upsert
    # If idem_key exists → update last_seen
    # If idem_key doesn't exist → insert new row
    merge_query = f"""
        MERGE `{table_ref}` AS target
        USING (
            SELECT
                @idem_key AS idem_key,
                @source_system AS source_system,
                @object_type AS object_type,
                @external_id AS external_id,
                PARSE_JSON(@payload) AS payload,
                TIMESTAMP(@now) AS first_seen,
                TIMESTAMP(@now) AS last_seen
        ) AS source
        ON target.idem_key = source.idem_key
        WHEN MATCHED THEN
            UPDATE SET last_seen = source.last_seen
        WHEN NOT MATCHED THEN
            INSERT (idem_key, source_system, object_type, external_id, payload, first_seen, last_seen)
            VALUES (idem_key, source_system, object_type, external_id, payload, first_seen, last_seen)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("idem_key", "STRING", idem_key),
            bigquery.ScalarQueryParameter("source_system", "STRING", source),
            bigquery.ScalarQueryParameter("object_type", "STRING", object_type),
            bigquery.ScalarQueryParameter("external_id", "STRING", str(external_id) if external_id else None),
            bigquery.ScalarQueryParameter("payload", "STRING", json.dumps(payload)),
            bigquery.ScalarQueryParameter("now", "STRING", now),
        ]
    )

    try:
        query_job = bq_client.query(merge_query, job_config=job_config)
        query_job.result()  # Wait for completion
    except Exception as e:
        raise RuntimeError(f"raw_objects MERGE failed: {e}")


def emit(
    event_type: str,
    payload: Dict[str, Any],
    *,
    source: str,
    object_type: str,
    bq_client,
    correlation_id: Optional[str] = None,
    subject_id: Optional[str] = None,
) -> None:
    """
    Emit an event to BigQuery event_log and raw_objects tables.

    This function implements the two-table pattern:
    1. Writes event envelope to event_log (audit trail)
    2. Writes/updates payload to raw_objects (deduped by idem_key)

    Args:
        event_type: Type of event (e.g., "email.received")
        payload: Event-specific data as a dict
        source: Source system (e.g., "tap-gmail--acct1-personal")
        object_type: Object type (e.g., "email", "questionnaire_response")
        bq_client: google.cloud.bigquery.Client instance
        correlation_id: Optional correlation ID for tracing
        subject_id: Optional subject identifier (PHI)

    Raises:
        ValueError: If event_type, source, or object_type are invalid
        RuntimeError: If BigQuery write fails or env vars missing

    Example:
        >>> from google.cloud import bigquery
        >>> client = bigquery.Client()
        >>> emit(
        ...     event_type="email.received",
        ...     payload={"id": "msg123", "subject": "Hello"},
        ...     source="tap-gmail--acct1-personal",
        ...     object_type="email",
        ...     bq_client=client
        ... )

    Result:
        - One row in event_log (event_id=uuid, idem_key=email:tap-gmail:msg123)
        - One row in raw_objects (idem_key=email:tap-gmail:msg123, payload={...})
        - Re-running with same payload → new event_log row, same raw_objects row
    """
    # 1. Generate idem_key from content
    idem_key = generate_idem_key(source, object_type, payload)

    # 2. Build event envelope (no payload - just metadata)
    envelope = build_envelope(
        event_type=event_type,
        source=source,
        object_type=object_type,
        idem_key=idem_key,
        correlation_id=correlation_id,
        subject_id=subject_id,
        status="ok",
    )

    # 3. Write to event_log (audit trail - always insert)
    _write_to_event_log(envelope, bq_client)

    # 4. Write to raw_objects (dedup by idem_key via MERGE)
    _write_to_raw_objects(idem_key, source, object_type, payload, bq_client)
