"""JobRunner - Central dispatcher for lorchestra jobs.

This module provides the main entry point for executing jobs:
1. Loads JSON job definitions from jobs/definitions/
2. Instantiates shared clients (BigQuery, EventClient, StorageClient)
3. Dispatches to the appropriate processor by job_type
4. Handles errors and emits job lifecycle events

Usage:
    from lorchestra.job_runner import run_job

    # Run a job by ID (loads from jobs/definitions/{job_id}.json)
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

# Default job definitions directory
DEFINITIONS_DIR = Path(__file__).parent / "jobs" / "definitions"


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

    def query_objects_for_canonization(
        self,
        source_system: str,
        object_type: str,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query validated raw_objects that need canonization.

        Returns records where:
        - validation_status = 'pass'
        - AND either:
          - Not yet in canonical_objects (never canonized), OR
          - raw_objects.last_seen > canonical_objects.canonicalized_at (raw updated since)

        Args:
            source_system: Source system to filter by
            object_type: Object type to filter by
            filters: Additional filters (field=value)
            limit: Max records to return
        """
        raw_table = self._table_name("raw_objects")
        canonical_table = self._table_name("canonical_objects")
        filters = filters or {}

        # Build WHERE clause conditions
        conditions = [
            "r.source_system = @source_system",
            "r.object_type = @object_type",
            "r.validation_status = 'pass'",
            "(c.idem_key IS NULL OR r.last_seen > c.canonicalized_at)",
        ]
        params = [
            bigquery.ScalarQueryParameter("source_system", "STRING", source_system),
            bigquery.ScalarQueryParameter("object_type", "STRING", object_type),
        ]

        # Add additional filters (skip validation_status since we hardcode it)
        for i, (key, value) in enumerate(filters.items()):
            if key == "validation_status":
                continue  # Already handled above
            param_name = f"filter_{i}"
            if value is None:
                conditions.append(f"r.{key} IS NULL")
            else:
                conditions.append(f"r.{key} = @{param_name}")
                params.append(bigquery.ScalarQueryParameter(param_name, "STRING", str(value)))

        where_clause = " AND ".join(conditions)
        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
            SELECT r.idem_key, r.source_system, r.connection_name, r.object_type,
                   r.payload, r.correlation_id, r.validation_status, r.last_seen
            FROM `{self.dataset}.{raw_table}` r
            LEFT JOIN `{self.dataset}.{canonical_table}` c ON r.idem_key = c.idem_key
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

    def upsert_canonical(
        self,
        objects: list[dict[str, Any]],
        correlation_id: str,
        batch_size: int = 500,
    ) -> dict[str, int]:
        """Upsert canonical objects using MERGE.

        Uses MERGE to insert new records or update existing ones based on idem_key.
        This ensures no duplicates and allows re-canonization when raw data changes.

        Args:
            objects: List of canonical objects to upsert
            correlation_id: Correlation ID for the batch
            batch_size: Number of records per MERGE batch

        Returns:
            Dict with 'inserted' and 'updated' counts
        """
        if not objects:
            return {"inserted": 0, "updated": 0}

        table = self._table_name("canonical_objects")
        table_ref = f"{self.dataset}.{table}"

        total_inserted = 0
        total_updated = 0

        # Process in batches
        for i in range(0, len(objects), batch_size):
            batch = objects[i:i + batch_size]

            # Build source data as a query with UNION ALL
            source_rows = []
            for obj in batch:
                payload = json.dumps(obj["payload"]) if isinstance(obj["payload"], dict) else obj["payload"]
                # Escape single quotes in payload
                payload_escaped = payload.replace("'", "\\'") if payload else ""

                source_rows.append(f"""
                    SELECT
                        '{obj["idem_key"]}' as idem_key,
                        '{obj.get("source_system", "")}' as source_system,
                        {f"'{obj.get('connection_name')}'" if obj.get('connection_name') else 'NULL'} as connection_name,
                        '{obj.get("object_type", "")}' as object_type,
                        '{obj.get("canonical_schema", "")}' as canonical_schema,
                        '{obj.get("canonical_format", "")}' as canonical_format,
                        '{obj.get("transform_ref", "")}' as transform_ref,
                        '{obj.get("correlation_id") or correlation_id}' as correlation_id,
                        PARSE_JSON('{payload_escaped}') as payload,
                        CURRENT_TIMESTAMP() as canonicalized_at,
                        CURRENT_TIMESTAMP() as created_at
                """)

            source_query = " UNION ALL ".join(source_rows)

            merge_query = f"""
                MERGE `{table_ref}` T
                USING ({source_query}) S
                ON T.idem_key = S.idem_key
                WHEN MATCHED THEN
                    UPDATE SET
                        source_system = S.source_system,
                        connection_name = S.connection_name,
                        object_type = S.object_type,
                        canonical_schema = S.canonical_schema,
                        canonical_format = S.canonical_format,
                        transform_ref = S.transform_ref,
                        correlation_id = S.correlation_id,
                        payload = S.payload,
                        canonicalized_at = S.canonicalized_at
                WHEN NOT MATCHED THEN
                    INSERT (idem_key, source_system, connection_name, object_type,
                            canonical_schema, canonical_format, transform_ref,
                            correlation_id, payload, canonicalized_at, created_at)
                    VALUES (S.idem_key, S.source_system, S.connection_name, S.object_type,
                            S.canonical_schema, S.canonical_format, S.transform_ref,
                            S.correlation_id, S.payload, S.canonicalized_at, S.created_at)
            """

            result = self.bq_client.query(merge_query).result()

            # BigQuery MERGE returns num_dml_affected_rows for total affected
            # We can't distinguish inserts from updates in the result, so we estimate
            affected = result.num_dml_affected_rows or 0
            total_inserted += affected  # Approximate - includes updates

        return {"inserted": total_inserted, "updated": total_updated}

    # Columns that exist on canonical_objects table (vs payload fields)
    CANONICAL_TABLE_COLUMNS = {
        "idem_key", "source_system", "connection_name", "object_type",
        "canonical_schema", "canonical_format", "transform_ref",
        "correlation_id", "canonicalized_at", "created_at",
    }

    def query_canonical(
        self,
        canonical_schema: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query canonical objects.

        Filters can reference either table columns or payload fields.
        Payload fields are accessed via JSON_VALUE(payload, '$.field_name').
        """
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

            # Determine if this is a table column or a payload field
            if key in self.CANONICAL_TABLE_COLUMNS:
                # Direct column reference
                if value is None:
                    conditions.append(f"{key} IS NULL")
                else:
                    conditions.append(f"{key} = @{param_name}")
                    params.append(bigquery.ScalarQueryParameter(param_name, "STRING", str(value)))
            else:
                # Payload field - use JSON_VALUE
                if value is None:
                    conditions.append(f"JSON_VALUE(payload, '$.{key}') IS NULL")
                else:
                    conditions.append(f"JSON_VALUE(payload, '$.{key}') = @{param_name}")
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

    def query_canonical_for_formation(
        self,
        canonical_schema: str | None = None,
        filters: dict[str, Any] | None = None,
        measurement_table: str = "measurement_events",
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query canonical objects that need formation (incremental).

        Returns records where:
        - canonical_schema matches (if provided)
        - additional filters match
        - AND either:
          - Not yet in measurement_events (never formed), OR
          - canonical_objects.canonicalized_at > measurement_events.processed_at (re-canonized since last processing)

        This enables incremental processing - only new or updated canonical
        records are returned, avoiding re-processing the entire table.

        Args:
            canonical_schema: Filter by canonical schema
            filters: Additional filters (column or payload fields)
            measurement_table: Name of measurement events table
            limit: Max records to return
        """
        canonical_table = self._table_name("canonical_objects")
        measurement_table = self._table_name(measurement_table)
        filters = filters or {}

        # Build WHERE clause conditions
        conditions = []
        params = []

        if canonical_schema:
            conditions.append("c.canonical_schema = @canonical_schema")
            params.append(bigquery.ScalarQueryParameter("canonical_schema", "STRING", canonical_schema))

        for i, (key, value) in enumerate(filters.items()):
            if key == "canonical_schema":
                continue  # Already handled
            param_name = f"filter_{i}"

            # Determine if this is a table column or a payload field
            if key in self.CANONICAL_TABLE_COLUMNS:
                # Direct column reference
                if value is None:
                    conditions.append(f"c.{key} IS NULL")
                else:
                    conditions.append(f"c.{key} = @{param_name}")
                    params.append(bigquery.ScalarQueryParameter(param_name, "STRING", str(value)))
            else:
                # Payload field - use JSON_VALUE
                if value is None:
                    conditions.append(f"JSON_VALUE(c.payload, '$.{key}') IS NULL")
                else:
                    conditions.append(f"JSON_VALUE(c.payload, '$.{key}') = @{param_name}")
                    params.append(bigquery.ScalarQueryParameter(param_name, "STRING", str(value)))

        # Incremental condition: never formed OR re-canonized since last formation
        conditions.append("(m.canonical_idem_key IS NULL OR c.canonicalized_at > m.processed_at)")

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        limit_clause = f"LIMIT {limit}" if limit else ""

        # LEFT JOIN to find canonical records without measurement events or with stale events
        # Use a subquery to get the MAX(processed_at) per canonical_idem_key to handle
        # multiple measurement events per canonical record
        query = f"""
            SELECT c.idem_key, c.source_system, c.connection_name, c.object_type,
                   c.canonical_schema, c.transform_ref, c.payload, c.correlation_id
            FROM `{self.dataset}.{canonical_table}` c
            LEFT JOIN (
                SELECT canonical_idem_key, MAX(processed_at) as processed_at
                FROM `{self.dataset}.{measurement_table}`
                GROUP BY canonical_idem_key
            ) m ON c.idem_key = m.canonical_idem_key
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

    def upsert_measurements(
        self,
        measurements: list[dict[str, Any]],
        table: str,
        correlation_id: str,
        batch_size: int = 500,
    ) -> int:
        """Upsert measurement events using MERGE by idem_key.

        Inserts new rows or updates existing rows based on idem_key.
        This provides idempotent writes for re-processing.
        """
        if not measurements:
            return 0

        table_name = self._table_name(table)
        table_ref = f"{self.dataset}.{table_name}"
        total_affected = 0

        # Process in batches
        for i in range(0, len(measurements), batch_size):
            batch = measurements[i : i + batch_size]

            # Build source data as a query with UNION ALL
            source_rows = []
            for m in batch:
                # Escape single quotes
                def esc(v):
                    return str(v).replace("'", "\\'") if v is not None else ""

                source_rows.append(f"""
                    SELECT
                        '{esc(m["idem_key"])}' as idem_key,
                        '{esc(m.get("canonical_idem_key", ""))}' as canonical_idem_key,
                        '{esc(m.get("measurement_event_id", ""))}' as measurement_event_id,
                        '{esc(m.get("measure_id", ""))}' as measure_id,
                        '{esc(m.get("measure_version", ""))}' as measure_version,
                        '{esc(m.get("subject_id", ""))}' as subject_id,
                        '{esc(m.get("timestamp", ""))}' as timestamp,
                        '{esc(m.get("binding_id", ""))}' as binding_id,
                        '{esc(m.get("binding_version", ""))}' as binding_version,
                        '{esc(m.get("form_id", ""))}' as form_id,
                        '{esc(m.get("form_submission_id", ""))}' as form_submission_id,
                        TIMESTAMP('{esc(m.get("processed_at", ""))}') as processed_at,
                        '{esc(m.get("correlation_id") or correlation_id)}' as correlation_id,
                        CURRENT_TIMESTAMP() as created_at
                """)

            source_query = " UNION ALL ".join(source_rows)

            merge_query = f"""
                MERGE `{table_ref}` T
                USING ({source_query}) S
                ON T.idem_key = S.idem_key
                WHEN MATCHED THEN
                    UPDATE SET
                        canonical_idem_key = S.canonical_idem_key,
                        measurement_event_id = S.measurement_event_id,
                        measure_id = S.measure_id,
                        measure_version = S.measure_version,
                        subject_id = S.subject_id,
                        timestamp = S.timestamp,
                        binding_id = S.binding_id,
                        binding_version = S.binding_version,
                        form_id = S.form_id,
                        form_submission_id = S.form_submission_id,
                        processed_at = S.processed_at,
                        correlation_id = S.correlation_id
                WHEN NOT MATCHED THEN
                    INSERT (idem_key, canonical_idem_key, measurement_event_id, measure_id,
                            measure_version, subject_id, timestamp, binding_id, binding_version,
                            form_id, form_submission_id, processed_at, correlation_id, created_at)
                    VALUES (S.idem_key, S.canonical_idem_key, S.measurement_event_id, S.measure_id,
                            S.measure_version, S.subject_id, S.timestamp, S.binding_id, S.binding_version,
                            S.form_id, S.form_submission_id, S.processed_at, S.correlation_id, S.created_at)
            """

            result = self.bq_client.query(merge_query).result()
            total_affected += result.num_dml_affected_rows or 0

        return total_affected

    def upsert_observations(
        self,
        observations: list[dict[str, Any]],
        table: str,
        correlation_id: str,
        batch_size: int = 500,
    ) -> int:
        """Upsert observations using MERGE by idem_key.

        Inserts new rows or updates existing rows based on idem_key.
        This provides idempotent writes for re-processing.
        """
        if not observations:
            return 0

        table_name = self._table_name(table)
        table_ref = f"{self.dataset}.{table_name}"
        total_affected = 0

        # Process in batches
        for i in range(0, len(observations), batch_size):
            batch = observations[i : i + batch_size]

            # Build source data as a query with UNION ALL
            source_rows = []
            for o in batch:
                # Escape single quotes
                def esc(v):
                    return str(v).replace("'", "\\'") if v is not None else ""

                # Handle numeric value - could be int, float, or null
                value = o.get("value")
                if value is None:
                    value_sql = "NULL"
                elif isinstance(value, (int, float)):
                    value_sql = str(value)
                else:
                    value_sql = f"'{esc(value)}'"

                # Handle position (nullable int)
                position = o.get("position")
                position_sql = str(position) if position is not None else "NULL"

                # Handle missing (boolean)
                missing = o.get("missing", False)
                missing_sql = "TRUE" if missing else "FALSE"

                source_rows.append(f"""
                    SELECT
                        '{esc(o["idem_key"])}' as idem_key,
                        '{esc(o.get("measurement_idem_key", ""))}' as measurement_idem_key,
                        '{esc(o.get("observation_id", ""))}' as observation_id,
                        '{esc(o.get("measure_id", ""))}' as measure_id,
                        '{esc(o.get("code", ""))}' as code,
                        '{esc(o.get("kind", ""))}' as kind,
                        {value_sql} as value,
                        '{esc(o.get("value_type", ""))}' as value_type,
                        {f"'{esc(o.get('label'))}'" if o.get('label') else 'NULL'} as label,
                        {f"'{esc(o.get('raw_answer'))}'" if o.get('raw_answer') else 'NULL'} as raw_answer,
                        {position_sql} as position,
                        {missing_sql} as missing,
                        '{esc(o.get("correlation_id") or correlation_id)}' as correlation_id,
                        CURRENT_TIMESTAMP() as created_at
                """)

            source_query = " UNION ALL ".join(source_rows)

            merge_query = f"""
                MERGE `{table_ref}` T
                USING ({source_query}) S
                ON T.idem_key = S.idem_key
                WHEN MATCHED THEN
                    UPDATE SET
                        measurement_idem_key = S.measurement_idem_key,
                        observation_id = S.observation_id,
                        measure_id = S.measure_id,
                        code = S.code,
                        kind = S.kind,
                        value = S.value,
                        value_type = S.value_type,
                        label = S.label,
                        raw_answer = S.raw_answer,
                        position = S.position,
                        missing = S.missing,
                        correlation_id = S.correlation_id
                WHEN NOT MATCHED THEN
                    INSERT (idem_key, measurement_idem_key, observation_id, measure_id, code, kind,
                            value, value_type, label, raw_answer, position, missing, correlation_id, created_at)
                    VALUES (S.idem_key, S.measurement_idem_key, S.observation_id, S.measure_id, S.code, S.kind,
                            S.value, S.value_type, S.label, S.raw_answer, S.position, S.missing, S.correlation_id, S.created_at)
            """

            result = self.bq_client.query(merge_query).result()
            total_affected += result.num_dml_affected_rows or 0

        return total_affected


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


def load_job_definition(job_id: str, definitions_dir: Path | None = None) -> dict[str, Any]:
    """Load a job definition from JSON file.

    Args:
        job_id: Job identifier (e.g., "gmail_ingest_acct1")
        definitions_dir: Directory containing job definitions (defaults to jobs/definitions/)

    Returns:
        Parsed job definition dict

    Raises:
        FileNotFoundError: If job definition file doesn't exist
        json.JSONDecodeError: If job definition is invalid JSON
    """
    definitions_dir = definitions_dir or DEFINITIONS_DIR
    def_path = definitions_dir / f"{job_id}.json"

    if not def_path.exists():
        raise FileNotFoundError(f"Job definition not found: {def_path}")

    with open(def_path) as f:
        job_def = json.load(f)

    # Ensure job_id in definition matches filename
    if "job_id" not in job_def:
        job_def["job_id"] = job_id

    return job_def


def run_job(
    job_id: str,
    *,
    dry_run: bool = False,
    test_table: bool = False,
    definitions_dir: Path | None = None,
    bq_client: bigquery.Client | None = None,
) -> None:
    """Run a job by ID.

    This is the main entry point for job execution. It:
    1. Loads the job definition from jobs/definitions/{job_id}.json
    2. Creates shared clients (BigQuery, StorageClient, EventClient)
    3. Dispatches to the appropriate processor by job_type
    4. Emits job.started and job.completed/job.failed events

    Args:
        job_id: Job identifier (e.g., "gmail_ingest_acct1")
        dry_run: If True, skip all writes and log what would happen
        test_table: If True, write to test tables instead of production
        definitions_dir: Optional directory containing job definitions
        bq_client: Optional BigQuery client (created if not provided)

    Raises:
        FileNotFoundError: If job definition doesn't exist
        KeyError: If job_type is not registered
        Exception: If processor raises an error
    """
    # Generate run ID for correlation
    run_id = f"{job_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"

    # Load job definition
    job_def = load_job_definition(job_id, definitions_dir)
    job_type = job_def.get("job_type")

    if not job_type:
        raise ValueError(f"Job definition missing job_type: {job_id}")

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
        status="success",
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
        processor.run(job_def, context, storage_client, event_client)

        # Emit job.completed event
        event_client.log_event(
            event_type="job.completed",
            source_system="lorchestra",
            correlation_id=run_id,
            status="success",
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
