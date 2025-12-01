"""JobRunner - Central dispatcher for lorchestra jobs.

This module provides the main entry point for executing jobs:
1. Loads JSON job specs from jobs/specs/
2. Instantiates shared clients (BigQuery, EventClient, StorageClient)
3. Dispatches to the appropriate processor by job_type
4. Handles errors and emits job lifecycle events

Usage:
    from lorchestra.job_runner import run_job

    # Run a job by ID (loads from jobs/specs/{job_id}.json)
    run_job("gmail_ingest_acct1")

    # Run with options
    run_job("gmail_ingest_acct1", dry_run=True)
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from google.cloud import bigquery

from lorchestra.processors import registry
from lorchestra.processors.base import (
    JobContext,
    UpsertResult,
)
from lorchestra.stack_clients import event_client as ec

logger = logging.getLogger(__name__)

# Default job specs directory
SPECS_DIR = Path(__file__).parent / "jobs" / "specs"


class BigQueryStorageClient:
    """StorageClient implementation backed by BigQuery.

    Implements the StorageClient protocol using BigQuery as the backing store.
    """

    def __init__(self, bq_client: bigquery.Client, dataset: str, test_table: bool = False):
        self.bq_client = bq_client
        self.dataset = dataset
        self.test_table = test_table

    def _table_name(self, base_name: str) -> str:
        """Get actual table name (prefixed with test_ if in test mode)."""
        if self.test_table:
            return f"test_{base_name}"
        return base_name

    def query_objects(
        self,
        source_system: str,
        object_type: str,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query objects from raw_objects."""
        table = self._table_name("raw_objects")
        filters = filters or {}

        # Build WHERE clause
        conditions = [
            "source_system = @source_system",
            "object_type = @object_type",
        ]
        params = [
            bigquery.ScalarQueryParameter("source_system", "STRING", source_system),
            bigquery.ScalarQueryParameter("object_type", "STRING", object_type),
        ]

        for i, (key, value) in enumerate(filters.items()):
            param_name = f"filter_{i}"
            if value is None:
                conditions.append(f"{key} IS NULL")
            else:
                conditions.append(f"{key} = @{param_name}")
                params.append(bigquery.ScalarQueryParameter(param_name, "STRING", str(value)))

        where_clause = " AND ".join(conditions)
        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
            SELECT idem_key, source_system, connection_name, object_type,
                   payload, correlation_id, validation_status, last_seen
            FROM `{self.dataset}.{table}`
            WHERE {where_clause}
            {limit_clause}
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        result = self.bq_client.query(query, job_config=job_config).result()

        for row in result:
            yield dict(row)

    def upsert_objects(
        self,
        objects: Iterator[dict[str, Any]],
        source_system: str,
        connection_name: str,
        object_type: str,
        idem_key_fn: Callable[[dict[str, Any]], str],
        correlation_id: str,
    ) -> UpsertResult:
        """Batch upsert objects to raw_objects."""
        # Delegate to existing event_client.upsert_objects
        result = ec.upsert_objects(
            objects=objects,
            source_system=source_system,
            connection_name=connection_name,
            object_type=object_type,
            correlation_id=correlation_id,
            idem_key_fn=idem_key_fn,
            bq_client=self.bq_client,
        )
        return UpsertResult(inserted=result.inserted, updated=result.updated)

    def update_field(
        self,
        idem_keys: list[str],
        field: str,
        value: Any,
    ) -> int:
        """Update a field on existing objects."""
        if not idem_keys:
            return 0

        table = self._table_name("raw_objects")

        # Build UPDATE query
        query = f"""
            UPDATE `{self.dataset}.{table}`
            SET {field} = @value, last_seen = CURRENT_TIMESTAMP()
            WHERE idem_key IN UNNEST(@idem_keys)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("value", "STRING", str(value)),
                bigquery.ArrayQueryParameter("idem_keys", "STRING", idem_keys),
            ]
        )

        result = self.bq_client.query(query, job_config=job_config).result()
        return result.num_dml_affected_rows or 0

    def insert_canonical(
        self,
        objects: list[dict[str, Any]],
        correlation_id: str,
    ) -> int:
        """Insert canonical objects."""
        if not objects:
            return 0

        table = self._table_name("canonical_objects")

        # Prepare rows for insert
        rows = []
        for obj in objects:
            row = {
                "idem_key": obj["idem_key"],
                "source_system": obj.get("source_system"),
                "connection_name": obj.get("connection_name"),
                "object_type": obj.get("object_type"),
                "canonical_schema": obj.get("canonical_schema"),
                "canonical_format": obj.get("canonical_format"),
                "transform_ref": obj.get("transform_ref"),
                "validation_stamp": obj.get("validation_stamp"),
                "correlation_id": obj.get("correlation_id") or correlation_id,
                "payload": json.dumps(obj["payload"]) if isinstance(obj["payload"], dict) else obj["payload"],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            rows.append(row)

        table_ref = f"{self.dataset}.{table}"
        errors = self.bq_client.insert_rows_json(table_ref, rows)

        if errors:
            raise RuntimeError(f"canonical_objects insert failed: {errors}")

        return len(rows)

    def query_canonical(
        self,
        canonical_schema: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query canonical objects."""
        table = self._table_name("canonical_objects")
        filters = filters or {}

        # Build WHERE clause
        conditions = []
        params = []

        if canonical_schema:
            conditions.append("canonical_schema = @canonical_schema")
            params.append(bigquery.ScalarQueryParameter("canonical_schema", "STRING", canonical_schema))

        for i, (key, value) in enumerate(filters.items()):
            if key == "canonical_schema":
                continue  # Already handled
            param_name = f"filter_{i}"
            if value is None:
                conditions.append(f"{key} IS NULL")
            else:
                conditions.append(f"{key} = @{param_name}")
                params.append(bigquery.ScalarQueryParameter(param_name, "STRING", str(value)))

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
            SELECT idem_key, source_system, connection_name, object_type,
                   canonical_schema, transform_ref, payload, correlation_id
            FROM `{self.dataset}.{table}`
            WHERE {where_clause}
            {limit_clause}
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        result = self.bq_client.query(query, job_config=job_config).result()

        for row in result:
            yield dict(row)

    def insert_measurements(
        self,
        measurements: list[dict[str, Any]],
        table: str,
        correlation_id: str,
    ) -> int:
        """Insert measurement events."""
        if not measurements:
            return 0

        table_name = self._table_name(table)
        table_ref = f"{self.dataset}.{table_name}"

        rows = []
        for m in measurements:
            row = {
                "idem_key": m["idem_key"],
                "canonical_idem_key": m.get("canonical_idem_key"),
                "instrument_id": m.get("instrument_id"),
                "instrument_version": m.get("instrument_version"),
                "binding_id": m.get("binding_id"),
                "score": m.get("score"),
                "score_interpretation": m.get("score_interpretation"),
                "processed_at": m.get("processed_at"),
                "correlation_id": m.get("correlation_id") or correlation_id,
            }
            rows.append(row)

        errors = self.bq_client.insert_rows_json(table_ref, rows)
        if errors:
            raise RuntimeError(f"{table_name} insert failed: {errors}")

        return len(rows)

    def insert_observations(
        self,
        observations: list[dict[str, Any]],
        table: str,
        correlation_id: str,
    ) -> int:
        """Insert observations."""
        if not observations:
            return 0

        table_name = self._table_name(table)
        table_ref = f"{self.dataset}.{table_name}"

        rows = []
        for o in observations:
            row = {
                "idem_key": o["idem_key"],
                "measurement_idem_key": o.get("measurement_idem_key"),
                "item_id": o.get("item_id"),
                "item_text": o.get("item_text"),
                "response_value": o.get("response_value"),
                "response_text": o.get("response_text"),
                "score_value": o.get("score_value"),
                "correlation_id": o.get("correlation_id") or correlation_id,
            }
            rows.append(row)

        errors = self.bq_client.insert_rows_json(table_ref, rows)
        if errors:
            raise RuntimeError(f"{table_name} insert failed: {errors}")

        return len(rows)


class BigQueryEventClient:
    """EventClient implementation backed by BigQuery.

    Implements the EventClient protocol using the existing event_client module.
    """

    def __init__(self, bq_client: bigquery.Client):
        self.bq_client = bq_client

    def log_event(
        self,
        event_type: str,
        source_system: str,
        correlation_id: str,
        status: str,
        connection_name: str | None = None,
        target_object_type: str | None = None,
        payload: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> None:
        """Log an event to event_log."""
        ec.log_event(
            event_type=event_type,
            source_system=source_system,
            correlation_id=correlation_id,
            status=status,
            connection_name=connection_name,
            target_object_type=target_object_type,
            payload=payload,
            error_message=error_message,
            bq_client=self.bq_client,
        )


def load_job_spec(job_id: str, specs_dir: Path | None = None) -> dict[str, Any]:
    """Load a job spec from JSON file.

    Args:
        job_id: Job identifier (e.g., "gmail_ingest_acct1")
        specs_dir: Directory containing job specs (defaults to jobs/specs/)

    Returns:
        Parsed job specification dict

    Raises:
        FileNotFoundError: If job spec file doesn't exist
        json.JSONDecodeError: If job spec is invalid JSON
    """
    specs_dir = specs_dir or SPECS_DIR
    spec_path = specs_dir / f"{job_id}.json"

    if not spec_path.exists():
        raise FileNotFoundError(f"Job spec not found: {spec_path}")

    with open(spec_path) as f:
        spec = json.load(f)

    # Ensure job_id in spec matches filename
    if "job_id" not in spec:
        spec["job_id"] = job_id

    return spec


def run_job(
    job_id: str,
    *,
    dry_run: bool = False,
    test_table: bool = False,
    specs_dir: Path | None = None,
    bq_client: bigquery.Client | None = None,
) -> None:
    """Run a job by ID.

    This is the main entry point for job execution. It:
    1. Loads the job spec from jobs/specs/{job_id}.json
    2. Creates shared clients (BigQuery, StorageClient, EventClient)
    3. Dispatches to the appropriate processor by job_type
    4. Emits job.started and job.completed/job.failed events

    Args:
        job_id: Job identifier (e.g., "gmail_ingest_acct1")
        dry_run: If True, skip all writes and log what would happen
        test_table: If True, write to test tables instead of production
        specs_dir: Optional directory containing job specs
        bq_client: Optional BigQuery client (created if not provided)

    Raises:
        FileNotFoundError: If job spec doesn't exist
        KeyError: If job_type is not registered
        Exception: If processor raises an error
    """
    # Generate run ID for correlation
    run_id = f"{job_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"

    # Load job spec
    job_spec = load_job_spec(job_id, specs_dir)
    job_type = job_spec.get("job_type")

    if not job_type:
        raise ValueError(f"Job spec missing job_type: {job_id}")

    logger.info(f"Starting job: {job_id} (type={job_type}, run_id={run_id})")
    if dry_run:
        logger.info("  DRY RUN mode enabled")
    if test_table:
        logger.info("  TEST TABLE mode enabled")

    # Set run mode in event_client module
    ec.set_run_mode(dry_run=dry_run, test_table=test_table)

    # Create BigQuery client if not provided
    if bq_client is None:
        bq_client = bigquery.Client()

    # Get dataset from environment
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    # Create protocol implementations
    storage_client = BigQueryStorageClient(bq_client, dataset, test_table=test_table)
    event_client = BigQueryEventClient(bq_client)

    # Create job context
    context = JobContext(
        bq_client=bq_client,
        run_id=run_id,
        dry_run=dry_run,
        test_table=test_table,
    )

    # Emit job.started event
    event_client.log_event(
        event_type="job.started",
        source_system="lorchestra",
        correlation_id=run_id,
        status="ok",
        payload={
            "job_id": job_id,
            "job_type": job_type,
            "dry_run": dry_run,
            "test_table": test_table,
        },
    )

    try:
        # Import processors to ensure registration
        import lorchestra.processors.ingest  # noqa: F401
        import lorchestra.processors.canonize  # noqa: F401
        import lorchestra.processors.final_form  # noqa: F401

        # Get processor and run
        processor = registry.get(job_type)
        processor.run(job_spec, context, storage_client, event_client)

        # Emit job.completed event
        event_client.log_event(
            event_type="job.completed",
            source_system="lorchestra",
            correlation_id=run_id,
            status="ok",
            payload={
                "job_id": job_id,
                "job_type": job_type,
            },
        )

        logger.info(f"Job completed: {job_id}")

    except Exception as e:
        logger.error(f"Job failed: {job_id} - {e}", exc_info=True)

        # Emit job.failed event
        event_client.log_event(
            event_type="job.failed",
            source_system="lorchestra",
            correlation_id=run_id,
            status="failed",
            error_message=str(e),
            payload={
                "job_id": job_id,
                "job_type": job_type,
                "error_type": type(e).__name__,
            },
        )

        raise

    finally:
        # Reset run mode
        ec.reset_run_mode()
