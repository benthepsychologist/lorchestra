"""
Event client for writing to BigQuery event_log and raw_objects tables.

This module implements a two-table pattern with explicit separation:
1. event_log: Event envelopes (audit trail) with optional small telemetry payload
2. raw_objects: Deduped object store (keyed by idem_key, with full payload)

Architecture:
- log_event(): Write event envelopes to event_log (telemetry, job events, etc.)
- upsert_objects(): Batch MERGE objects into raw_objects (data ingestion)
- Events and objects are decoupled - you can log events without objects

Key principles:
- Explicit API: log_event() vs upsert_objects() - caller chooses what they need
- idem_key is nullable in event_log (telemetry events don't have idem_keys)
- Payload in event_log is small telemetry only (job params, counts, duration)
- Full object payloads go to raw_objects via batch upsert
- trace_id supports cross-system tracing

Configuration via environment variables:
- EVENTS_BQ_DATASET: BigQuery dataset name
- EVENT_LOG_TABLE: Event log table name (default: "event_log")
- RAW_OBJECTS_TABLE: Raw objects table name (default: "raw_objects")

Usage:
    from google.cloud import bigquery
    from lorchestra.stack_clients.event_client import log_event, upsert_objects
    from lorchestra.idem_keys import gmail_idem_key

    client = bigquery.Client()

    # Log a job event (no object storage)
    log_event(
        event_type="job.started",
        source_system="lorchestra",
        correlation_id="gmail-20251124120000",
        object_type="job_run",
        status="ok",
        payload={"job_name": "gmail_ingest", "date_filter": "2025-11-20"},
        bq_client=client
    )

    # Batch upsert objects (no per-object events)
    upsert_objects(
        objects=email_iterator,
        source_system="tap-gmail--acct1",
        object_type="email",
        correlation_id="gmail-20251124120000",
        idem_key_fn=gmail_idem_key("tap-gmail--acct1"),
        bq_client=client
    )
"""

from typing import Any, Dict, Optional, Callable, Iterable, Union, List
from datetime import datetime, timezone
import uuid
import os
import json
import logging

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None  # Allow import without BigQuery SDK for testing

logger = logging.getLogger(__name__)

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


def log_event(
    *,
    event_type: str,
    source_system: str,
    correlation_id: str,
    bq_client,
    trace_id: Optional[str] = None,
    object_type: Optional[str] = None,
    status: str = "ok",
    error_message: Optional[str] = None,
    idem_key: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Write event envelope to event_log table.

    Use this for:
    - Job execution events (job.started, job.completed, job.failed)
    - Telemetry events (ingestion.completed with counts/duration)
    - Lifecycle events that don't correspond to a specific object

    Args:
        event_type: Type of event (e.g., "job.started", "ingestion.completed")
        source_system: Source system (e.g., "lorchestra", "tap-gmail--acct1")
        correlation_id: Correlation ID for tracing (e.g., run_id)
        bq_client: google.cloud.bigquery.Client instance
        trace_id: Optional cross-system trace ID
        object_type: Optional object type (e.g., "job_run" for job events)
        status: Event status ("ok" | "failed")
        error_message: Human-readable error summary if status="failed"
        idem_key: Optional idem_key to link event to specific object
        payload: Optional small telemetry dict (job params, counts, duration, error metadata)

    Raises:
        ValueError: If required fields are invalid
        RuntimeError: If BigQuery write fails or env vars missing

    Example:
        >>> # Job started event
        >>> log_event(
        ...     event_type="job.started",
        ...     source_system="lorchestra",
        ...     correlation_id="gmail-20251124120000",
        ...     object_type="job_run",
        ...     status="ok",
        ...     payload={"job_name": "gmail_ingest", "package": "tap-gmail"},
        ...     bq_client=client
        ... )

        >>> # Job completed with metrics
        >>> log_event(
        ...     event_type="job.completed",
        ...     source_system="lorchestra",
        ...     correlation_id="gmail-20251124120000",
        ...     object_type="job_run",
        ...     status="ok",
        ...     payload={"job_name": "gmail_ingest", "duration_seconds": 12.5},
        ...     bq_client=client
        ... )

        >>> # Ingestion completed with counts
        >>> log_event(
        ...     event_type="ingestion.completed",
        ...     source_system="tap-gmail--acct1",
        ...     correlation_id="gmail-20251124120000",
        ...     status="ok",
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

    # Build event envelope
    envelope = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "source_system": source_system,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "correlation_id": correlation_id,
    }

    # Add optional fields only if they have values (BQ doesn't like empty strings)
    if object_type:
        envelope["object_type"] = object_type
    if idem_key:
        envelope["idem_key"] = idem_key
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


def upsert_objects(
    *,
    objects: Union[List[Dict[str, Any]], Iterable[Dict[str, Any]]],
    source_system: str,
    object_type: str,
    correlation_id: str,
    idem_key_fn: Callable[[Dict[str, Any]], str],
    bq_client,
    trace_id: Optional[str] = None,
    batch_size: int = 5000,
) -> None:
    """
    Batch MERGE objects into raw_objects table.

    Use this for:
    - Data ingestion (emails, contacts, etc.)
    - Bulk object updates

    This function processes objects in batches to handle large datasets efficiently:
    1. Load batch to temp table via load_table_from_json()
    2. MERGE from temp table to raw_objects
    3. Cleanup temp table

    Args:
        objects: List or iterator of object dicts to upsert
        source_system: Source system (e.g., "tap-gmail--acct1")
        object_type: Object type (e.g., "email", "contact")
        correlation_id: Correlation ID for tracing (e.g., run_id)
        idem_key_fn: Function that computes idem_key from object (REQUIRED)
        bq_client: google.cloud.bigquery.Client instance
        trace_id: Optional cross-system trace ID
        batch_size: Batch size for processing (default 5000, lower if hitting BQ limits)

    Raises:
        ValueError: If required fields are invalid or idem_key_fn is missing
        RuntimeError: If BigQuery write fails

    Example:
        >>> from lorchestra.idem_keys import gmail_idem_key
        >>>
        >>> # Batch upsert emails
        >>> upsert_objects(
        ...     objects=email_iterator,
        ...     source_system="tap-gmail--acct1",
        ...     object_type="email",
        ...     correlation_id="gmail-20251124120000",
        ...     idem_key_fn=gmail_idem_key("tap-gmail--acct1"),
        ...     bq_client=client
        ... )

    Note:
        - idem_key_fn is REQUIRED to force callers to think about identity explicitly
        - Supports both list and iterator inputs (iterators don't materialize entire dataset)
        - Each batch is processed independently for memory efficiency
    """
    # Validate required fields
    if not source_system or not isinstance(source_system, str):
        raise ValueError("source_system must be a non-empty string")
    if not object_type or not isinstance(object_type, str):
        raise ValueError("object_type must be a non-empty string")
    if not correlation_id or not isinstance(correlation_id, str):
        raise ValueError("correlation_id must be a non-empty string")
    if not callable(idem_key_fn):
        raise ValueError("idem_key_fn must be a callable function")

    # Handle dry-run mode: consume iterator once, log samples, skip write
    if _DRY_RUN_MODE:
        count = 0
        samples = []
        for obj in objects:
            count += 1
            if len(samples) < 3:
                samples.append(obj)

        logger.info(f"[DRY-RUN] Would upsert {count} {object_type} objects for {source_system}")
        for i, s in enumerate(samples):
            ik = idem_key_fn(s)
            logger.info(f"[DRY-RUN] Sample {i+1} idem_key={ik}")
            logger.debug(f"[DRY-RUN] Sample {i+1} payload={json.dumps(s, default=str)[:500]}")
        return

    # Process in batches
    batch = []
    batch_idx = 0
    run_id = str(uuid.uuid4())[:8]  # Short run ID for temp table naming

    for obj in objects:
        batch.append(obj)

        if len(batch) >= batch_size:
            _upsert_batch(
                batch=batch,
                batch_idx=batch_idx,
                run_id=run_id,
                source_system=source_system,
                object_type=object_type,
                correlation_id=correlation_id,
                trace_id=trace_id,
                idem_key_fn=idem_key_fn,
                bq_client=bq_client,
            )
            batch = []
            batch_idx += 1

    # Process remaining objects
    if batch:
        _upsert_batch(
            batch=batch,
            batch_idx=batch_idx,
            run_id=run_id,
            source_system=source_system,
            object_type=object_type,
            correlation_id=correlation_id,
            trace_id=trace_id,
            idem_key_fn=idem_key_fn,
            bq_client=bq_client,
        )


def _upsert_batch(
    *,
    batch: List[Dict[str, Any]],
    batch_idx: int,
    run_id: str,
    source_system: str,
    object_type: str,
    correlation_id: str,
    trace_id: Optional[str],
    idem_key_fn: Callable[[Dict[str, Any]], str],
    bq_client,
) -> None:
    """
    Upsert a single batch of objects to raw_objects.

    This function:
    1. Loads batch to temp table
    2. MERGEs from temp table to raw_objects
    3. Cleans up temp table

    Args:
        batch: List of objects to upsert
        batch_idx: Batch index for temp table naming
        run_id: Run ID for temp table naming
        source_system: Source system
        object_type: Object type
        correlation_id: Correlation ID
        trace_id: Optional trace ID
        idem_key_fn: Function to compute idem_key from object
        bq_client: BigQuery client

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
        # Prepare rows for temp table
        rows = []
        for obj in batch:
            idem_key = idem_key_fn(obj)
            external_id = _extract_external_id(obj)

            rows.append({
                "idem_key": idem_key,
                "source_system": source_system,
                "object_type": object_type,
                "external_id": str(external_id) if external_id else None,
                "payload": obj,
                "first_seen": now,
                "last_seen": now,
            })

        # Load to temp table
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("idem_key", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("source_system", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("object_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("external_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
                bigquery.SchemaField("first_seen", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("last_seen", "TIMESTAMP", mode="REQUIRED"),
            ],
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
        merge_query = f"""
            MERGE `{raw_objects_ref}` AS target
            USING `{temp_table_ref}` AS source
            ON target.idem_key = source.idem_key
            WHEN MATCHED THEN
                UPDATE SET
                    payload = source.payload,
                    last_seen = source.last_seen
            WHEN NOT MATCHED THEN
                INSERT (idem_key, source_system, object_type, external_id, payload, first_seen, last_seen)
                VALUES (idem_key, source_system, object_type, external_id, payload, first_seen, last_seen)
        """

        merge_job = bq_client.query(merge_query)
        merge_job.result()  # Wait for MERGE to complete

    except Exception as e:
        raise RuntimeError(f"Batch upsert failed for batch {batch_idx}: {e}")

    finally:
        # Cleanup temp table
        try:
            bq_client.delete_table(temp_table_ref, not_found_ok=True)
        except Exception:
            pass  # Ignore cleanup errors


def _extract_external_id(payload: Dict[str, Any]) -> Optional[str]:
    """
    Extract natural external_id from payload.

    Args:
        payload: Object payload

    Returns:
        External ID string or None
    """
    return (
        payload.get('id') or
        payload.get('message_id') or
        payload.get('external_id') or
        payload.get('uuid')
    )


def _get_table_ref(table_env_var: str, default_table_name: str) -> str:
    """
    Get fully-qualified BigQuery table reference from environment.

    Args:
        table_env_var: Environment variable name for table (e.g., "EVENT_LOG_TABLE")
        default_table_name: Default table name if env var not set

    Returns:
        Table reference string: "dataset.table_name"

    Raises:
        RuntimeError: If required EVENTS_BQ_DATASET environment variable is missing
    """
    dataset = os.environ.get("EVENTS_BQ_DATASET")
    table = os.environ.get(table_env_var, default_table_name)

    if not dataset:
        raise RuntimeError("Missing required environment variable: EVENTS_BQ_DATASET")

    return f"{dataset}.{table}"


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


def _get_table_ref_by_name(table_name: str) -> str:
    """
    Get fully-qualified BigQuery table reference by explicit table name.

    Used for test tables where we specify the name directly rather than
    reading from environment variables.

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
