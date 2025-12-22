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

import importlib
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from google.cloud import bigquery

from lorchestra.config import LorchestraConfig
from lorchestra.processors import registry
from lorchestra.processors.base import JobContext, UpsertResult
from lorchestra.stack_clients import event_client as ec

logger = logging.getLogger(__name__)


# =============================================================================
# SECURE FACTORY LOADER
# =============================================================================

# Allowlist for service factory modules - security restriction
# Only exact matches or submodules allowed (prefix + ".")
# Add new modules only when needed - don't pre-authorize
ALLOWED_FACTORY_MODULES = [
    "gorch.sheets.factories",
]


def _is_allowed_module(module_path: str) -> bool:
    """Check if module is in allowlist (exact match or submodule)."""
    for allowed in ALLOWED_FACTORY_MODULES:
        if module_path == allowed or module_path.startswith(allowed + "."):
            return True
    return False


def load_service_factory(factory_path: str) -> Callable[..., Any]:
    """Load a service factory by dotted path string.

    Only allows factories from allowlisted modules (exact match or submodules).

    Args:
        factory_path: e.g. "gorch.sheets.factories:build_sheets_write_service"

    Returns:
        The callable factory function

    Raises:
        ValueError: If path not in allowlist or malformed
        ImportError: If module not found
        AttributeError: If function not found in module
        TypeError: If attribute is not callable
    """
    if ":" not in factory_path:
        raise ValueError(f"Factory path must be 'module:function', got: {factory_path}")

    module_path, func_name = factory_path.rsplit(":", 1)

    # Security: restrict to allowed modules (exact or submodule)
    if not _is_allowed_module(module_path):
        raise ValueError(
            f"Factory module '{module_path}' not in allowlist. "
            f"Allowed: {ALLOWED_FACTORY_MODULES}"
        )

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise ImportError(f"Cannot import factory module '{module_path}': {e}") from e

    try:
        factory = getattr(module, func_name)
    except AttributeError as e:
        raise AttributeError(
            f"Factory function '{func_name}' not found in '{module_path}': {e}"
        ) from e

    if not callable(factory):
        raise TypeError(f"{factory_path} is not callable")

    return factory

# Default job definitions directory
DEFINITIONS_DIR = Path(__file__).parent / "jobs" / "definitions"


class BigQueryStorageClient:
    """StorageClient implementation backed by BigQuery.

    Implements the StorageClient protocol using the BigQuery client.
    """

    def __init__(self, bq_client: bigquery.Client, config: LorchestraConfig, test_table: bool = False):
        self.bq_client = bq_client
        self.config = config
        self.dataset = config.dataset_raw  # Default to raw, but methods might use others
        self.test_table = test_table

    def _table_name(self, base_name: str) -> str:
        """Get actual table name (prefixed with test_ if in test mode)."""
        if self.test_table:
            return f"test_{base_name}"
        return base_name
    
    def _dataset_for_table(self, table_name: str) -> str:
        """Resolve dataset based on table type/name."""
        # This is a bit of a heuristic. Ideally we'd map table -> dataset explicitly.
        # But for now, we know:
        # raw_objects -> dataset_raw
        # canonical_objects -> dataset_canonical
        # measurement_events, observations -> dataset_derived (or canonical? Usually derived/analytics)
        # Using config to resolve instead of self.dataset
        if "raw_objects" in table_name:
            return self.config.dataset_raw
        elif "canonical_objects" in table_name:
            return self.config.dataset_canonical
        else:
            return self.config.dataset_derived  # measurements, observations, etc.

    def query_objects(
        self,
        source_system: str,
        object_type: str,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query objects from raw_objects."""
        table_base = "raw_objects"
        table = self._table_name(table_base)
        dataset = self._dataset_for_table(table_base)
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
            FROM `{dataset}.{table}`
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
        canonical_schema: str | None = None,
        idem_key_suffix: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query validated raw_objects that need canonization.

        Returns records where:
        - validation_status = 'pass'
        - AND either:
          - Not yet in canonical_objects for this schema (never canonized), OR
          - raw_objects.last_seen > canonical_objects.canonicalized_at (raw updated since)

        Args:
            source_system: Source system to filter by
            object_type: Object type to filter by
            filters: Additional filters (field=value)
            limit: Max records to return
            canonical_schema: Target canonical schema (important for 1:N raw->canonical mappings)
            idem_key_suffix: Explicit suffix for idem_key matching (e.g. 'session_note')
        """
        raw_base = "raw_objects"
        raw_table = self._table_name(raw_base)
        raw_dataset = self._dataset_for_table(raw_base)
        
        canonical_base = "canonical_objects"
        canonical_table = self._table_name(canonical_base)
        canonical_dataset = self._dataset_for_table(canonical_base)
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

        # Build the JOIN condition - must include canonical object type to support
        # 1:N mappings where one raw object produces multiple canonical objects
        # (e.g., session -> clinical_session AND session -> session_transcript)
        #
        # Canonical idem_keys use format: raw_idem_key#suffix
        # e.g. "dataverse:session:abc123#session_note"
        #
        # Use explicit idem_key_suffix when provided (preferred for stability across schema changes)
        # Otherwise fall back to extracting from schema name
        if idem_key_suffix:
            # Explicit suffix provided - use it directly
            canonical_object_type = idem_key_suffix
        elif canonical_schema:
            # Extract canonical object type from schema:
            # iglu:org.canonical/session_transcript/jsonschema/2-0-0 -> session_transcript
            schema_parts = canonical_schema.split("/")
            canonical_object_type = schema_parts[1] if len(schema_parts) > 1 else ""
        else:
            canonical_object_type = None

        if canonical_object_type:
            # Match canonical idem_key = raw_idem_key + '#' + suffix
            # This handles the 1:N mapping correctly
            join_condition = f"c.idem_key = CONCAT(r.idem_key, '#{canonical_object_type}')"
        else:
            # Fallback: match any canonical record that starts with raw idem_key + '#'
            join_condition = "c.idem_key LIKE CONCAT(r.idem_key, '#%')"

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
            FROM `{raw_dataset}.{raw_table}` r
            LEFT JOIN `{canonical_dataset}.{canonical_table}` c ON {join_condition}
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
            dataset=self.config.dataset_raw,
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

        table_base = "raw_objects"
        table = self._table_name(table_base)
        dataset = self._dataset_for_table(table_base)

        # Build UPDATE query
        query = f"""
            UPDATE `{dataset}.{table}`
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

        table_base = "canonical_objects"
        table = self._table_name(table_base)
        dataset = self._dataset_for_table(table_base)
        table_ref = f"{dataset}.{table}"

        total_inserted = 0
        total_updated = 0

        # Process in batches
        for i in range(0, len(objects), batch_size):
            batch = objects[i:i + batch_size]

            # Build source data as a query with UNION ALL
            source_rows = []
            for obj in batch:
                payload = json.dumps(obj["payload"]) if isinstance(obj["payload"], dict) else obj["payload"]
                # Escape special characters in payload for SQL string literal
                # - Single quotes need to be escaped for SQL
                # - Newlines/carriage returns need to be escaped for PARSE_JSON
                # - Backslashes need to be escaped first to avoid double-escaping
                if payload:
                    payload_escaped = (
                        payload
                        .replace("\\", "\\\\")  # Escape backslashes first
                        .replace("'", "\\'")    # Escape single quotes for SQL
                        .replace("\n", "\\n")   # Escape newlines for PARSE_JSON
                        .replace("\r", "\\r")   # Escape carriage returns
                    )
                else:
                    payload_escaped = ""

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
        table_base = "canonical_objects"
        table = self._table_name(table_base)
        dataset = self._dataset_for_table(table_base)
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
            FROM `{dataset}.{table}`
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

        New schema (FHIR-aligned):
        - measurement_events now has 1 row per form submission
        - Join on measurement_events.canonical_object_id = canonical_objects.idem_key

        Args:
            canonical_schema: Filter by canonical schema
            filters: Additional filters (column or payload fields)
            measurement_table: Name of measurement events table
            limit: Max records to return
        """
        canonical_base = "canonical_objects"
        canonical_table = self._table_name(canonical_base)
        canonical_dataset = self._dataset_for_table(canonical_base)

        measurement_base = measurement_table
        measurement_table_name = self._table_name(measurement_base)
        measurement_dataset = self._dataset_for_table(measurement_base)

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
        # New schema uses canonical_object_id instead of canonical_idem_key
        conditions.append("(m.canonical_object_id IS NULL OR c.canonicalized_at > m.processed_at)")

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        limit_clause = f"LIMIT {limit}" if limit else ""

        # LEFT JOIN to find canonical records without measurement events or with stale events
        # New schema: measurement_events.canonical_object_id = canonical_objects.idem_key
        # Now 1:1 relationship (1 measurement_event per canonical form_response)
        query = f"""
            SELECT c.idem_key, c.source_system, c.connection_name, c.object_type,
                   c.canonical_schema, c.transform_ref, c.payload, c.correlation_id
            FROM `{canonical_dataset}.{canonical_table}` c
            LEFT JOIN `{measurement_dataset}.{measurement_table_name}` m
                ON c.idem_key = m.canonical_object_id
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

        table_base = table
        table_name = self._table_name(table_base)
        dataset = self._dataset_for_table(table_base)
        table_ref = f"{dataset}.{table_name}"

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

        table_base = table
        table_name = self._table_name(table_base)
        dataset = self._dataset_for_table(table_base)
        table_ref = f"{dataset}.{table_name}"

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

        New schema (FHIR-aligned):
        - 1 row per form submission (the measurement event)
        - Columns: idem_key, measurement_event_id, subject_id, subject_contact_id,
                   event_type, event_subtype, occurred_at, received_at, source_system,
                   source_entity, source_id, canonical_object_id, form_id, binding_id,
                   binding_version, metadata, correlation_id, processed_at, created_at
        """
        if not measurements:
            return 0

        table_base = table
        table_name = self._table_name(table_base)
        dataset = self._dataset_for_table(table_base)
        table_ref = f"{dataset}.{table_name}"
        total_affected = 0

        # Process in batches
        for i in range(0, len(measurements), batch_size):
            batch = measurements[i : i + batch_size]

            # Build source data as a query with UNION ALL
            source_rows = []
            for m in batch:
                # Escape single quotes and special chars
                def esc(v):
                    if v is None:
                        return ""
                    return str(v).replace("\\", "\\\\").replace("'", "\\'")

                # Handle nullable fields - cast NULL to STRING for BigQuery
                subject_contact_id = m.get("subject_contact_id")
                subject_contact_id_sql = f"'{esc(subject_contact_id)}'" if subject_contact_id else "CAST(NULL AS STRING)"

                # Handle timestamps
                occurred_at = m.get("occurred_at")
                occurred_at_sql = f"TIMESTAMP('{esc(occurred_at)}')" if occurred_at else "NULL"

                received_at = m.get("received_at")
                received_at_sql = f"TIMESTAMP('{esc(received_at)}')" if received_at else "CURRENT_TIMESTAMP()"

                processed_at = m.get("processed_at")
                processed_at_sql = f"TIMESTAMP('{esc(processed_at)}')" if processed_at else "CURRENT_TIMESTAMP()"

                # Handle JSON metadata
                metadata = m.get("metadata", "{}")
                metadata_sql = f"PARSE_JSON('{esc(metadata)}')" if metadata else "PARSE_JSON('{}')"

                source_rows.append(f"""
                    SELECT
                        '{esc(m["idem_key"])}' as idem_key,
                        '{esc(m.get("measurement_event_id", ""))}' as measurement_event_id,
                        '{esc(m.get("subject_id", ""))}' as subject_id,
                        {subject_contact_id_sql} as subject_contact_id,
                        '{esc(m.get("event_type", "form"))}' as event_type,
                        '{esc(m.get("event_subtype", ""))}' as event_subtype,
                        {occurred_at_sql} as occurred_at,
                        {received_at_sql} as received_at,
                        '{esc(m.get("source_system", ""))}' as source_system,
                        '{esc(m.get("source_entity", ""))}' as source_entity,
                        '{esc(m.get("source_id", ""))}' as source_id,
                        '{esc(m.get("canonical_object_id", ""))}' as canonical_object_id,
                        '{esc(m.get("form_id", ""))}' as form_id,
                        '{esc(m.get("binding_id", ""))}' as binding_id,
                        '{esc(m.get("binding_version", ""))}' as binding_version,
                        {metadata_sql} as metadata,
                        '{esc(m.get("correlation_id") or correlation_id)}' as correlation_id,
                        {processed_at_sql} as processed_at,
                        CURRENT_TIMESTAMP() as created_at
                """)

            source_query = " UNION ALL ".join(source_rows)

            merge_query = f"""
                MERGE `{table_ref}` T
                USING ({source_query}) S
                ON T.idem_key = S.idem_key
                WHEN MATCHED THEN
                    UPDATE SET
                        measurement_event_id = S.measurement_event_id,
                        subject_id = S.subject_id,
                        subject_contact_id = S.subject_contact_id,
                        event_type = S.event_type,
                        event_subtype = S.event_subtype,
                        occurred_at = S.occurred_at,
                        received_at = S.received_at,
                        source_system = S.source_system,
                        source_entity = S.source_entity,
                        source_id = S.source_id,
                        canonical_object_id = S.canonical_object_id,
                        form_id = S.form_id,
                        binding_id = S.binding_id,
                        binding_version = S.binding_version,
                        metadata = S.metadata,
                        correlation_id = S.correlation_id,
                        processed_at = S.processed_at
                WHEN NOT MATCHED THEN
                    INSERT (idem_key, measurement_event_id, subject_id, subject_contact_id,
                            event_type, event_subtype, occurred_at, received_at, source_system,
                            source_entity, source_id, canonical_object_id, form_id, binding_id,
                            binding_version, metadata, correlation_id, processed_at, created_at)
                    VALUES (S.idem_key, S.measurement_event_id, S.subject_id, S.subject_contact_id,
                            S.event_type, S.event_subtype, S.occurred_at, S.received_at, S.source_system,
                            S.source_entity, S.source_id, S.canonical_object_id, S.form_id, S.binding_id,
                            S.binding_version, S.metadata, S.correlation_id, S.processed_at, S.created_at)
            """

            result = self.bq_client.query(merge_query).result()
            total_affected += result.num_dml_affected_rows or 0

        return total_affected

    def upsert_observations(
        self,
        observations: list[dict[str, Any]],
        table: str,
        correlation_id: str,
        batch_size: int = 50,
    ) -> int:
        """Upsert observations using MERGE by idem_key.

        Inserts new rows or updates existing rows based on idem_key.
        This provides idempotent writes for re-processing.

        New schema (FHIR-aligned):
        - 1 row per scored construct (PHQ-9, GAD-7, etc.) with components JSON array
        - Columns: idem_key, observation_id, measurement_event_id, subject_id,
                   measure_code, measure_version, value_numeric, value_text, unit,
                   severity_code, severity_label, components, metadata, correlation_id,
                   processed_at, created_at

        Note: batch_size is small (50) because components JSON can be large.
        """
        if not observations:
            return 0

        table_base = table
        table_name = self._table_name(table_base)
        dataset = self._dataset_for_table(table_base)
        table_ref = f"{dataset}.{table_name}"
        total_affected = 0

        # Process in batches
        for i in range(0, len(observations), batch_size):
            batch = observations[i : i + batch_size]

            # Build source data as a query with UNION ALL
            source_rows = []
            for o in batch:
                # Escape single quotes and special chars
                def esc(v):
                    if v is None:
                        return ""
                    return str(v).replace("\\", "\\\\").replace("'", "\\'")

                # Handle numeric value - could be int, float, or null
                value_numeric = o.get("value_numeric")
                if value_numeric is None:
                    value_numeric_sql = "NULL"
                else:
                    value_numeric_sql = str(float(value_numeric))

                # Handle nullable string fields - cast NULL to STRING for BigQuery
                value_text = o.get("value_text")
                value_text_sql = f"'{esc(value_text)}'" if value_text else "CAST(NULL AS STRING)"

                severity_code = o.get("severity_code")
                severity_code_sql = f"'{esc(severity_code)}'" if severity_code else "CAST(NULL AS STRING)"

                severity_label = o.get("severity_label")
                severity_label_sql = f"'{esc(severity_label)}'" if severity_label else "CAST(NULL AS STRING)"

                # Handle JSON fields
                components = o.get("components", "[]")
                components_sql = f"PARSE_JSON('{esc(components)}')" if components else "PARSE_JSON('[]')"

                metadata = o.get("metadata", "{}")
                metadata_sql = f"PARSE_JSON('{esc(metadata)}')" if metadata else "PARSE_JSON('{}')"

                # Handle timestamps
                processed_at = o.get("processed_at")
                processed_at_sql = f"TIMESTAMP('{esc(processed_at)}')" if processed_at else "CURRENT_TIMESTAMP()"

                source_rows.append(f"""
                    SELECT
                        '{esc(o["idem_key"])}' as idem_key,
                        '{esc(o.get("observation_id", ""))}' as observation_id,
                        '{esc(o.get("measurement_event_id", ""))}' as measurement_event_id,
                        '{esc(o.get("subject_id", ""))}' as subject_id,
                        '{esc(o.get("measure_code", ""))}' as measure_code,
                        '{esc(o.get("measure_version", ""))}' as measure_version,
                        {value_numeric_sql} as value_numeric,
                        {value_text_sql} as value_text,
                        '{esc(o.get("unit", "score"))}' as unit,
                        {severity_code_sql} as severity_code,
                        {severity_label_sql} as severity_label,
                        {components_sql} as components,
                        {metadata_sql} as metadata,
                        '{esc(o.get("correlation_id") or correlation_id)}' as correlation_id,
                        {processed_at_sql} as processed_at,
                        CURRENT_TIMESTAMP() as created_at
                """)

            source_query = " UNION ALL ".join(source_rows)

            merge_query = f"""
                MERGE `{table_ref}` T
                USING ({source_query}) S
                ON T.idem_key = S.idem_key
                WHEN MATCHED THEN
                    UPDATE SET
                        observation_id = S.observation_id,
                        measurement_event_id = S.measurement_event_id,
                        subject_id = S.subject_id,
                        measure_code = S.measure_code,
                        measure_version = S.measure_version,
                        value_numeric = S.value_numeric,
                        value_text = S.value_text,
                        unit = S.unit,
                        severity_code = S.severity_code,
                        severity_label = S.severity_label,
                        components = S.components,
                        metadata = S.metadata,
                        correlation_id = S.correlation_id,
                        processed_at = S.processed_at
                WHEN NOT MATCHED THEN
                    INSERT (idem_key, observation_id, measurement_event_id, subject_id,
                            measure_code, measure_version, value_numeric, value_text, unit,
                            severity_code, severity_label, components, metadata, correlation_id,
                            processed_at, created_at)
                    VALUES (S.idem_key, S.observation_id, S.measurement_event_id, S.subject_id,
                            S.measure_code, S.measure_version, S.value_numeric, S.value_text, S.unit,
                            S.severity_code, S.severity_label, S.components, S.metadata, S.correlation_id,
                            S.processed_at, S.created_at)
            """

            result = self.bq_client.query(merge_query).result()
            total_affected += result.num_dml_affected_rows or 0

        return total_affected

    def execute_sql(
        self,
        sql: str,
    ) -> dict[str, Any]:
        """Execute arbitrary SQL and return result metadata.

        Used for DDL operations like CREATE VIEW.

        Args:
            sql: SQL statement to execute

        Returns:
            Dict with execution metadata
        """
        result = self.bq_client.query(sql).result()
        return {
            "rows_affected": result.num_dml_affected_rows or 0,
            "total_rows": result.total_rows or 0,
        }

    def query_to_dataframe(
        self,
        sql: str,
    ) -> list[dict[str, Any]]:
        """Execute query and return results as list of dicts.

        Used for sync operations that need to read data.

        Args:
            sql: SQL query to execute

        Returns:
            List of row dicts
        """
        result = self.bq_client.query(sql).result()
        return [dict(row) for row in result]

    def get_canonical_by_idem_key(
        self,
        idem_key: str,
    ) -> dict[str, Any] | None:
        """Get a canonical object by its idem_key.

        Args:
            idem_key: The idem_key to look up

        Returns:
            Dict with canonical object data, or None if not found
        """
        canonical_base = "canonical_objects"
        canonical_table = self._table_name(canonical_base)
        canonical_dataset = self._dataset_for_table(canonical_base)

        query = f"""
            SELECT idem_key, source_system, connection_name, object_type,
                   canonical_schema, transform_ref, payload, correlation_id
            FROM `{canonical_dataset}.{canonical_table}`
            WHERE idem_key = @idem_key
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("idem_key", "STRING", idem_key)
            ]
        )
        result = self.bq_client.query(query, job_config=job_config).result()

        for row in result:
            return dict(row)
        return None

    def query_measurement_events_for_scoring(
        self,
        measurement_table: str = "measurement_events",
        observation_table: str = "observations",
        binding_id: str | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query measurement_events that don't have observations yet.

        Returns measurement_events where:
        - No observations exist for that measurement_event_id
        - Optionally filtered by binding_id

        Args:
            measurement_table: Name of measurement_events table
            observation_table: Name of observations table
            binding_id: Optional filter by binding_id
            limit: Max records to return
        """
        me_base = measurement_table
        me_table = self._table_name(me_base)
        me_dataset = self._dataset_for_table(me_base)

        obs_base = observation_table
        obs_table = self._table_name(obs_base)
        obs_dataset = self._dataset_for_table(obs_base)

        conditions = []
        params = []

        # Only get measurement_events without observations
        conditions.append("o.measurement_event_id IS NULL")

        if binding_id:
            conditions.append("m.binding_id = @binding_id")
            params.append(bigquery.ScalarQueryParameter("binding_id", "STRING", binding_id))

        where_clause = " AND ".join(conditions)
        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
            SELECT m.idem_key, m.measurement_event_id, m.subject_id, m.event_type,
                   m.event_subtype, m.occurred_at, m.source_system, m.source_entity,
                   m.source_id, m.canonical_object_id, m.form_id, m.binding_id,
                   m.binding_version, m.correlation_id
            FROM `{me_dataset}.{me_table}` m
            LEFT JOIN (
                SELECT DISTINCT measurement_event_id
                FROM `{obs_dataset}.{obs_table}`
            ) o ON m.measurement_event_id = o.measurement_event_id
            WHERE {where_clause}
            {limit_clause}
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        result = self.bq_client.query(query, job_config=job_config).result()

        for row in result:
            yield dict(row)


class BigQueryEventClient:
    """EventClient implementation backed by BigQuery.

    Implements the EventClient protocol using the existing event_client module.
    """

    def __init__(self, bq_client: bigquery.Client, config: LorchestraConfig):
        self.bq_client = bq_client
        self.config = config

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
            dataset=self.config.dataset_raw,
        )


def load_job_definition(job_id: str, definitions_dir: Path | None = None) -> dict[str, Any]:
    """Load a job definition from JSON file.

    Searches for {job_id}.json in the definitions directory and all subdirectories.
    Subdirectories are organized by processor type (ingest/, canonize/, projection/, etc.)

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
    filename = f"{job_id}.json"

    # First check root directory
    def_path = definitions_dir / filename
    if def_path.exists():
        pass  # Use this path
    else:
        # Search subdirectories
        found = list(definitions_dir.glob(f"**/{filename}"))
        if not found:
            raise FileNotFoundError(f"Job definition not found: {def_path} (also searched subdirectories)")
        if len(found) > 1:
            raise ValueError(f"Multiple job definitions found for {job_id}: {found}")
        def_path = found[0]

    with open(def_path) as f:
        job_def = json.load(f)

    # Ensure job_id in definition matches filename
    if "job_id" not in job_def:
        job_def["job_id"] = job_id

    return job_def

def run_job(
    job_id: str,
    *,
    config: LorchestraConfig | None = None,
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
        config: Configuration object (required, though optional locally for test compat)
        dry_run: If True, skip all writes and log what would happen
        test_table: If True, write to test tables instead of production
        definitions_dir: Optional directory containing job definitions
        bq_client: Optional BigQuery client (created if not provided)

    Raises:
        FileNotFoundError: If job definition doesn't exist
        KeyError: If job_type is not registered
        Exception: If processor raises an error
        ValueError: If config is missing
    """
    if config is None:
         # Try to load if not provided, for backward compat or direct testing
         # But ideally caller should provide it
        from lorchestra.config import load_config
        try:
             config = load_config()
        except Exception as e:
             # If we are in a test env that mocks env vars but didn't pass config, we might want to allow it
             # But generally we should enforce config.
             # For now, let's just log and try to proceed if we can default (which we can't really properly)
             # or just fail.
             raise ValueError(f"Configuration required to run job: {e}")

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
        bq_client = bigquery.Client(project=config.project)

    # Initialize shared clients with config
    storage_client = BigQueryStorageClient(bq_client, config, test_table=test_table)
    event_client = BigQueryEventClient(bq_client, config)

    # Create job context
    context = JobContext(
        bq_client=bq_client,
        run_id=run_id,
        config=config,
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
        import lorchestra.processors.formation  # noqa: F401
        import lorchestra.processors.projection  # noqa: F401
        import lorchestra.processors.composite  # noqa: F401
        import lorchestra.processors.projectionist  # noqa: F401

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
