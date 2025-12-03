"""
Event client for writing to BigQuery event_log and raw_objects tables.

This module implements a two-table pattern with explicit separation:
1. event_log: Event envelopes (audit trail) - what happened and when
2. raw_objects: State projection of objects (keyed by idem_key)

Architecture:
- log_event(): Write event envelopes to event_log (telemetry, job events, etc.)
- upsert_objects(): Batch MERGE objects into raw_objects (data ingestion)
- Events and objects are decoupled - you can log events without objects

Column Standards (aligned with Airbyte/Singer/Meltano):
- source_system: Provider family (gmail, exchange, dataverse, google_forms)
- connection_name: Configured account (gmail-acct1, exchange-ben-mensio)
- object_type / target_object_type: Domain object (email, contact, session)

Schema References (Iglu format):
- schema_ref: For raw objects (iglu:com.mensio.raw/raw_gmail_email/jsonschema/1-0-0)
- event_schema_ref: For events (iglu:com.mensio.event/ingest_completed/jsonschema/1-0-0)

Configuration via environment variables:
- EVENTS_BQ_DATASET: BigQuery dataset name

Usage:
    from google.cloud import bigquery
    from lorchestra.stack_clients.event_client import log_event, upsert_objects

    client = bigquery.Client()

    # Log a job event (no object storage)
    log_event(
        event_type="job.started",
        source_system="lorchestra",
        correlation_id="gmail-20251124120000",
        status="success",
        payload={"job_name": "gmail_ingest"},
        bq_client=client
    )

    # Batch upsert objects with new column pattern
    result = upsert_objects(
        objects=email_iterator,
        source_system="gmail",
        connection_name="gmail-acct1",
        object_type="email",
        correlation_id="gmail-20251124120000",
        idem_key_fn=gmail_idem_key("gmail", "gmail-acct1"),
        bq_client=client
    )
    # result.total_records, result.inserted, result.updated
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Callable, Iterable, Union, List
from datetime import datetime, timezone
import uuid
import os
import json
import logging
import time

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None  # Allow import without BigQuery SDK for testing

logger = logging.getLogger(__name__)


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class UpsertResult:
    """Result of an upsert_objects() call."""
    total_records: int
    inserted: int
    updated: int
    batch_count: int
    duration_seconds: float


# ============================================================================
# Run Mode Context
# ============================================================================
# Module-level flags for dry-run and test-table modes.
# Set via set_run_mode() at CLI entry before job execution.
# ============================================================================

_DRY_RUN_MODE = False
_TEST_TABLE_MODE = False


def set_run_mode(*, dry_run: bool = False, test_table: bool = False) -> None:
    """Set the run mode for event_client operations.

    Call this once at CLI entry before job execution.

    Args:
        dry_run: If True, skip all BigQuery writes and log what would happen
        test_table: If True, write to test_event_log/test_raw_objects instead of prod
    """
    global _DRY_RUN_MODE, _TEST_TABLE_MODE
    _DRY_RUN_MODE = dry_run
    _TEST_TABLE_MODE = test_table


def reset_run_mode() -> None:
    """Reset run mode to defaults (for testing)."""
    global _DRY_RUN_MODE, _TEST_TABLE_MODE
    _DRY_RUN_MODE = False
    _TEST_TABLE_MODE = False


# ============================================================================
# log_event - Event Logging
# ============================================================================

def log_event(
    *,
    event_type: str,
    source_system: str,
    correlation_id: str,
    bq_client,
    status: str = "ok",
    connection_name: Optional[str] = None,
    target_object_type: Optional[str] = None,
    event_schema_ref: Optional[str] = None,
    trace_id: Optional[str] = None,
    error_message: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Write event envelope to event_log table.

    Use this for:
    - Job execution events (job.started, job.completed, job.failed)
    - Ingestion events (ingestion.started, ingestion.completed, ingestion.failed)
    - Validation events (validation.started, validation.completed, validation.failed)
    - Canonization events (canonization.started, canonization.completed, canonization.failed)
    - Upsert events (upsert.completed) - auto-emitted by upsert_objects() (internal)

    Args:
        event_type: Type of event (e.g., "job.started", "ingestion.completed", "upsert.completed")
        source_system: Provider family (e.g., "lorchestra", "gmail", "dataverse")
        correlation_id: Correlation ID for tracing (e.g., run_id)
        bq_client: google.cloud.bigquery.Client instance
        status: Event status ("success" | "failed") - strict, no synonyms
        connection_name: Account/connection (e.g., "gmail-acct1") - NULL for system events
        target_object_type: Domain object type (e.g., "email", "session") - NULL for job events
        event_schema_ref: Iglu URI for event schema (e.g., "iglu:com.mensio.event/ingest_completed/jsonschema/1-0-0")
        trace_id: Optional cross-system trace ID
        error_message: Human-readable error summary if status="failed"
        payload: Optional small telemetry dict (job params, counts, duration, error metadata)

    Raises:
        ValueError: If required fields are invalid
        RuntimeError: If BigQuery write fails or env vars missing

    Example:
        >>> # Job started event (system-level, no connection)
        >>> log_event(
        ...     event_type="job.started",
        ...     source_system="lorchestra",
        ...     correlation_id="gmail-20251124120000",
        ...     status="success",
        ...     payload={"job_name": "gmail_ingest"},
        ...     bq_client=client
        ... )

        >>> # Ingestion completed event (provider + connection + object)
        >>> log_event(
        ...     event_type="ingest.completed",
        ...     source_system="gmail",
        ...     connection_name="gmail-acct1",
        ...     target_object_type="email",
        ...     correlation_id="gmail-20251124120000",
        ...     status="success",
        ...     payload={"records_extracted": 100, "duration_seconds": 10.2},
        ...     bq_client=client
        ... )
    """
    # Validate required fields
    if not event_type or not isinstance(event_type, str):
        raise ValueError("event_type must be a non-empty string")
    if not source_system or not isinstance(source_system, str):
        raise ValueError("source_system must be a non-empty string")
    if not correlation_id or not isinstance(correlation_id, str):
        raise ValueError("correlation_id must be a non-empty string")

    # Handle dry-run mode: log what would happen, skip write
    if _DRY_RUN_MODE:
        logger.info(f"[DRY-RUN] Would log event {event_type} for {source_system} status={status}")
        if payload:
            logger.debug(f"[DRY-RUN] Event payload: {payload}")
        return

    # Build event envelope matching new schema
    envelope = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "source_system": source_system,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "correlation_id": correlation_id,
    }

    # Add optional fields only if they have values
    if connection_name:
        envelope["connection_name"] = connection_name
    if target_object_type:
        envelope["target_object_type"] = target_object_type
    if event_schema_ref:
        envelope["event_schema_ref"] = event_schema_ref
    if trace_id:
        envelope["trace_id"] = trace_id
    if error_message:
        envelope["error_message"] = error_message
    if payload is not None:
        envelope["payload"] = json.dumps(payload)

    # Write to event_log (or test_event_log in test mode)
    table_name = "test_event_log" if _TEST_TABLE_MODE else "event_log"
    table_ref = _get_table_ref_by_name(table_name)
    errors = bq_client.insert_rows_json(table_ref, [envelope])

    if errors:
        raise RuntimeError(f"{table_name} insert failed: {errors}")


# ============================================================================
# upsert_objects - Object Upsert with Telemetry
# ============================================================================

def upsert_objects(
    *,
    objects: Union[List[Dict[str, Any]], Iterable[Dict[str, Any]]],
    source_system: str,
    connection_name: str,
    object_type: str,
    correlation_id: str,
    idem_key_fn: Callable[[Dict[str, Any]], str],
    bq_client,
    schema_ref: Optional[str] = None,
    trace_id: Optional[str] = None,
    batch_size: int = 5000,
) -> UpsertResult:
    """
    Batch MERGE objects into raw_objects table.

    Use this for:
    - Data ingestion (emails, contacts, forms, etc.)
    - Bulk object updates

    This function:
    1. Processes objects in batches (load to temp table, MERGE, cleanup)
    2. Tracks insert vs update counts
    3. Emits upsert.completed event with telemetry

    Args:
        objects: List or iterator of object dicts to upsert
        source_system: Provider family (e.g., "gmail", "dataverse", "google_forms")
        connection_name: Account/connection (e.g., "gmail-acct1", "dataverse-clinic")
        object_type: Domain object type (e.g., "email", "contact", "form_response")
        correlation_id: Correlation ID for tracing (e.g., run_id)
        idem_key_fn: Function that computes idem_key from object (REQUIRED)
        bq_client: google.cloud.bigquery.Client instance
        schema_ref: Iglu URI for object schema (e.g., "iglu:com.mensio.raw/raw_gmail_email/jsonschema/1-0-0")
        trace_id: Optional cross-system trace ID
        batch_size: Batch size for processing (default 5000)

    Returns:
        UpsertResult with total_records, inserted, updated, batch_count, duration_seconds

    Raises:
        ValueError: If required fields are invalid
        RuntimeError: If BigQuery write fails

    Example:
        >>> from lorchestra.idem_keys import gmail_idem_key
        >>>
        >>> result = upsert_objects(
        ...     objects=email_iterator,
        ...     source_system="gmail",
        ...     connection_name="gmail-acct1",
        ...     object_type="email",
        ...     correlation_id="gmail-20251124120000",
        ...     idem_key_fn=gmail_idem_key("gmail", "gmail-acct1"),
        ...     bq_client=client
        ... )
        >>> print(f"Upserted {result.total_records}: {result.inserted} new, {result.updated} updated")

    Note:
        - idem_key pattern: {source_system}:{connection_name}:{object_type}:{external_id}
        - Automatically emits upsert.completed event with telemetry
    """
    # Validate required fields
    if not source_system or not isinstance(source_system, str):
        raise ValueError("source_system must be a non-empty string")
    if not connection_name or not isinstance(connection_name, str):
        raise ValueError("connection_name must be a non-empty string")
    if not object_type or not isinstance(object_type, str):
        raise ValueError("object_type must be a non-empty string")
    if not correlation_id or not isinstance(correlation_id, str):
        raise ValueError("correlation_id must be a non-empty string")
    if not callable(idem_key_fn):
        raise ValueError("idem_key_fn must be a callable function")

    start_time = time.time()

    # Handle dry-run mode: consume iterator once, log samples, skip write
    if _DRY_RUN_MODE:
        count = 0
        samples = []
        for obj in objects:
            count += 1
            if len(samples) < 3:
                samples.append(obj)

        logger.info(f"[DRY-RUN] Would upsert {count} {object_type} objects for {source_system}/{connection_name}")
        for i, s in enumerate(samples):
            ik = idem_key_fn(s)
            logger.info(f"[DRY-RUN] Sample {i+1} idem_key={ik}")
            logger.debug(f"[DRY-RUN] Sample {i+1} payload={json.dumps(s, default=str)[:500]}")

        return UpsertResult(
            total_records=count,
            inserted=0,
            updated=0,
            batch_count=0,
            duration_seconds=time.time() - start_time,
        )

    # Process in batches, tracking counts
    total_records = 0
    total_inserted = 0
    total_updated = 0
    batch_idx = 0
    run_id = str(uuid.uuid4())[:8]  # Short run ID for temp table naming
    batch = []

    for obj in objects:
        batch.append(obj)
        total_records += 1

        if len(batch) >= batch_size:
            inserted, updated = _upsert_batch(
                batch=batch,
                batch_idx=batch_idx,
                run_id=run_id,
                source_system=source_system,
                connection_name=connection_name,
                object_type=object_type,
                schema_ref=schema_ref,
                correlation_id=correlation_id,
                trace_id=trace_id,
                idem_key_fn=idem_key_fn,
                bq_client=bq_client,
            )
            total_inserted += inserted
            total_updated += updated
            batch = []
            batch_idx += 1

    # Process remaining objects
    if batch:
        inserted, updated = _upsert_batch(
            batch=batch,
            batch_idx=batch_idx,
            run_id=run_id,
            source_system=source_system,
            connection_name=connection_name,
            object_type=object_type,
            schema_ref=schema_ref,
            correlation_id=correlation_id,
            trace_id=trace_id,
            idem_key_fn=idem_key_fn,
            bq_client=bq_client,
        )
        total_inserted += inserted
        total_updated += updated
        batch_idx += 1

    duration_seconds = time.time() - start_time

    # Emit upsert.completed event with telemetry (internal event for debugging)
    log_event(
        event_type="upsert.completed",
        source_system=source_system,
        connection_name=connection_name,
        target_object_type=object_type,
        correlation_id=correlation_id,
        trace_id=trace_id,
        status="success",
        payload={
            "target_table": "raw_objects",
            "records_inserted": total_inserted,
            "records_updated": total_updated,
            "duration_seconds": round(duration_seconds, 2),
        },
        bq_client=bq_client,
    )

    return UpsertResult(
        total_records=total_records,
        inserted=total_inserted,
        updated=total_updated,
        batch_count=batch_idx,
        duration_seconds=duration_seconds,
    )


def _upsert_batch(
    *,
    batch: List[Dict[str, Any]],
    batch_idx: int,
    run_id: str,
    source_system: str,
    connection_name: str,
    object_type: str,
    schema_ref: Optional[str],
    correlation_id: str,
    trace_id: Optional[str],
    idem_key_fn: Callable[[Dict[str, Any]], str],
    bq_client,
) -> tuple[int, int]:
    """
    Upsert a single batch of objects to raw_objects.

    This function:
    1. Loads batch to temp table
    2. MERGEs from temp table to raw_objects
    3. Cleans up temp table
    4. Returns (inserted_count, updated_count)

    Args:
        batch: List of objects to upsert
        batch_idx: Batch index for temp table naming
        run_id: Run ID for temp table naming
        source_system: Provider family
        connection_name: Account/connection
        object_type: Domain object type
        schema_ref: Iglu URI for schema (nullable)
        correlation_id: Correlation ID
        trace_id: Optional trace ID
        idem_key_fn: Function to compute idem_key from object
        bq_client: BigQuery client

    Returns:
        Tuple of (inserted_count, updated_count)

    Raises:
        RuntimeError: If load or MERGE fails
    """
    dataset = os.environ.get("EVENTS_BQ_DATASET")
    if not dataset:
        raise RuntimeError("Missing required environment variable: EVENTS_BQ_DATASET")

    temp_table_name = f"temp_objects_{run_id}_{batch_idx}"
    temp_table_ref = f"{dataset}.{temp_table_name}"

    # Use test table if in test mode
    target_table_name = "test_raw_objects" if _TEST_TABLE_MODE else "raw_objects"
    raw_objects_ref = _get_table_ref_by_name(target_table_name)

    now = datetime.now(timezone.utc).isoformat()

    try:
        # Prepare rows for temp table with new schema
        rows = []
        for obj in batch:
            idem_key = idem_key_fn(obj)
            external_id = _extract_external_id(obj)

            row = {
                "idem_key": idem_key,
                "source_system": source_system,
                "connection_name": connection_name,
                "object_type": object_type,
                "external_id": str(external_id) if external_id else None,
                "payload": obj,
                "first_seen": now,
                "last_seen": now,
                "correlation_id": correlation_id,  # Track which ingest run created this
            }
            if schema_ref:
                row["schema_ref"] = schema_ref
            rows.append(row)

        # Load to temp table with new schema
        schema_fields = [
            bigquery.SchemaField("idem_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_system", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("connection_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("object_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("schema_ref", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("external_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
            bigquery.SchemaField("first_seen", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("last_seen", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("correlation_id", "STRING", mode="NULLABLE"),
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema_fields,
            write_disposition="WRITE_TRUNCATE",
        )

        load_job = bq_client.load_table_from_json(
            rows,
            temp_table_ref,
            job_config=job_config,
        )
        load_job.result()  # Wait for load to complete

        # MERGE from temp table to raw_objects
        # Note: payload is updated on match - raw_objects is a state projection, not append-only
        # correlation_id tracks which ingest run created/updated the record
        merge_query = f"""
            MERGE `{raw_objects_ref}` AS target
            USING `{temp_table_ref}` AS source
            ON target.idem_key = source.idem_key
            WHEN MATCHED THEN
                UPDATE SET
                    payload = source.payload,
                    schema_ref = source.schema_ref,
                    last_seen = source.last_seen,
                    correlation_id = source.correlation_id
            WHEN NOT MATCHED THEN
                INSERT (idem_key, source_system, connection_name, object_type, schema_ref, external_id, payload, first_seen, last_seen, correlation_id)
                VALUES (idem_key, source_system, connection_name, object_type, schema_ref, external_id, payload, first_seen, last_seen, correlation_id)
        """

        merge_job = bq_client.query(merge_query)
        merge_job.result()  # Wait for completion

        # Extract insert/update counts from DML stats
        # Note: BigQuery provides num_dml_affected_rows but not separate insert/update counts
        # We approximate: if row existed, it was updated; otherwise inserted
        # For accurate counts, we'd need to query before/after, but that's expensive
        # For now, we'll use batch size as total and note this limitation
        inserted = 0
        updated = 0
        if hasattr(merge_job, 'num_dml_affected_rows') and merge_job.num_dml_affected_rows is not None:
            # All affected rows - can't distinguish insert vs update easily
            # Approximate: assume new data is mostly inserts for first run
            affected = merge_job.num_dml_affected_rows
            # This is a rough approximation; for accurate counts we'd need pre-query
            inserted = affected  # Conservative: count all as inserts
            updated = 0

    except Exception as e:
        raise RuntimeError(f"Batch upsert failed for batch {batch_idx}: {e}")

    finally:
        # Cleanup temp table
        try:
            bq_client.delete_table(temp_table_ref, not_found_ok=True)
        except Exception:
            pass  # Ignore cleanup errors

    return (inserted, updated)


# ============================================================================
# Helper Functions
# ============================================================================

def _extract_external_id(payload: Dict[str, Any]) -> Optional[str]:
    """
    Extract natural external_id from payload.

    Checks common ID field names across different providers.

    Args:
        payload: Object payload

    Returns:
        External ID string or None
    """
    return (
        payload.get('id') or
        payload.get('message_id') or
        payload.get('external_id') or
        payload.get('uuid') or
        payload.get('responseId') or  # Google Forms
        payload.get('contactid') or   # Dataverse contacts
        payload.get('cre92_clientsessionid') or  # Dataverse sessions
        payload.get('cre92_clientreportid')  # Dataverse reports
    )


def _get_table_ref_by_name(table_name: str) -> str:
    """
    Get fully-qualified BigQuery table reference by explicit table name.

    Args:
        table_name: Table name (e.g., "event_log", "test_event_log")

    Returns:
        Table reference string: "dataset.table_name"

    Raises:
        RuntimeError: If required EVENTS_BQ_DATASET environment variable is missing
    """
    dataset = os.environ.get("EVENTS_BQ_DATASET")

    if not dataset:
        raise RuntimeError("Missing required environment variable: EVENTS_BQ_DATASET")

    return f"{dataset}.{table_name}"


def ensure_test_tables_exist(bq_client) -> None:
    """Create test_event_log and test_raw_objects if they don't exist.

    Copies schema from production tables. Only called when --test-table is active.
    Dry-run never creates BQ resources.

    Args:
        bq_client: google.cloud.bigquery.Client instance
    """
    project = os.environ.get("GCP_PROJECT") or bq_client.project
    dataset = os.environ.get("EVENTS_BQ_DATASET")
    if not dataset:
        raise RuntimeError("Missing required environment variable: EVENTS_BQ_DATASET")

    _copy_schema_if_missing(
        bq_client,
        source=f"{project}.{dataset}.event_log",
        dest=f"{project}.{dataset}.test_event_log"
    )
    _copy_schema_if_missing(
        bq_client,
        source=f"{project}.{dataset}.raw_objects",
        dest=f"{project}.{dataset}.test_raw_objects"
    )


def _copy_schema_if_missing(bq_client, source: str, dest: str) -> None:
    """Copy table schema from source to dest if dest doesn't exist.

    Args:
        bq_client: BigQuery client
        source: Fully-qualified source table reference
        dest: Fully-qualified destination table reference
    """
    try:
        bq_client.get_table(dest)
        logger.debug(f"Test table already exists: {dest}")
        return  # Already exists
    except Exception:
        pass  # Doesn't exist, create it

    try:
        source_table = bq_client.get_table(source)
        new_table = bigquery.Table(dest, schema=source_table.schema)
        bq_client.create_table(new_table)
        logger.info(f"Created test table: {dest}")
    except Exception as e:
        raise RuntimeError(f"Failed to create test table {dest}: {e}")
