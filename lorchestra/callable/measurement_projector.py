"""
measurement_projector callable — Project canonical form_responses to measurement_events.

Replaces MeasurementEventProjection processor logic:
1. Reads canonical form_response objects via BQ (incremental — skip already processed)
2. Transforms each record into a measurement_event row
3. Returns CallableResult with items for bq.upsert

Each item is a batch: {"dataset": ..., "table": ..., "rows": [...], "key_columns": [...]}
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from lorchestra.callable.result import CallableResult

logger = logging.getLogger(__name__)

# Columns on the canonical_objects table (used for filter dispatch)
CANONICAL_TABLE_COLUMNS = frozenset({
    "idem_key", "source_system", "connection_name", "object_type",
    "canonical_schema", "transform_ref", "correlation_id",
    "received_at", "canonicalized_at",
})


def execute(params: dict[str, Any]) -> dict[str, Any]:
    """Project canonical form_responses into measurement_event rows.

    Params:
        canonical_schema: Iglu schema URI for source canonical objects
        connection_name: Filter for connection_name column
        measurement_table: Target table name (default: "measurement_events")
        event_type: Measurement event type (default: "form")
        event_subtype: Measurement event subtype / binding_id

    Returns:
        CallableResult with a single item containing:
        {"dataset": ..., "table": ..., "rows": [...], "key_columns": ["idem_key"]}
    """
    from lorchestra.config import load_config

    canonical_schema = params.get("canonical_schema")
    connection_name = params.get("connection_name")
    measurement_table = params.get("measurement_table", "measurement_events")
    event_type = params.get("event_type", "form")
    event_subtype = params.get("event_subtype")

    if not canonical_schema:
        raise ValueError("Missing required param: 'canonical_schema'")
    if not event_subtype:
        raise ValueError("Missing required param: 'event_subtype'")

    config = load_config()

    # Read canonical objects that need processing (incremental)
    records = _query_canonical_incremental(
        project=config.project,
        dataset_canonical=config.dataset_canonical,
        dataset_derived=config.dataset_derived,
        canonical_schema=canonical_schema,
        connection_name=connection_name,
        measurement_table=measurement_table,
    )

    logger.info(f"Found {len(records)} canonical records to process")

    # Transform each record into a measurement_event row
    measurement_rows = []
    errors = 0

    for record in records:
        try:
            row = _canonical_to_measurement_event(
                record=record,
                event_type=event_type,
                event_subtype=event_subtype,
            )
            measurement_rows.append(row)
        except Exception as e:
            errors += 1
            if errors <= 3:
                idem_key = record.get("idem_key", "unknown")
                logger.warning(f"Failed to transform record {idem_key}: {e}")

    logger.info(f"Transformed {len(measurement_rows)} measurement events ({errors} errors)")

    # Package as a single bq.upsert item
    items = []
    if measurement_rows:
        items.append({
            "dataset": config.dataset_derived,
            "table": measurement_table,
            "rows": measurement_rows,
            "key_columns": ["idem_key"],
        })

    result = CallableResult(
        items=items,
        stats={
            "input": len(records),
            "output": len(measurement_rows),
            "skipped": 0,
            "errors": errors,
        },
    )
    return result.to_dict()


def _query_canonical_incremental(
    project: str,
    dataset_canonical: str,
    dataset_derived: str,
    canonical_schema: str,
    connection_name: str | None,
    measurement_table: str,
) -> list[dict[str, Any]]:
    """Query canonical objects that need formation (incremental).

    Returns records where:
    - canonical_schema matches
    - connection_name matches (if provided)
    - AND either:
      - Not yet in measurement_events (never formed), OR
      - canonical_objects.canonicalized_at > measurement_events.processed_at
    """
    from storacle.clients.bigquery import BigQueryClient

    bq_client = BigQueryClient(project=project)

    # Build WHERE conditions
    conditions = ["c.canonical_schema = @canonical_schema"]
    query_params = [{"name": "canonical_schema", "type": "STRING", "value": canonical_schema}]

    if connection_name:
        conditions.append("c.connection_name = @connection_name")
        query_params.append({"name": "connection_name", "type": "STRING", "value": connection_name})

    # Incremental: not yet in measurement_events OR re-canonized since last processing
    conditions.append(
        "(m.canonical_object_id IS NULL OR c.canonicalized_at > m.processed_at)"
    )

    where_clause = " AND ".join(conditions)

    sql = f"""
        SELECT c.idem_key, c.source_system, c.connection_name, c.object_type,
               c.canonical_schema, c.transform_ref, c.payload, c.correlation_id
        FROM `{project}.{dataset_canonical}.canonical_objects` c
        LEFT JOIN `{project}.{dataset_derived}.{measurement_table}` m
            ON c.idem_key = m.canonical_object_id
        WHERE {where_clause}
    """

    result = bq_client.query(sql, params=query_params)
    rows = []
    for row in result.rows:
        row_dict = dict(row) if not isinstance(row, dict) else row
        # Parse payload from JSON string if needed
        if "payload" in row_dict and isinstance(row_dict["payload"], str):
            row_dict["payload"] = json.loads(row_dict["payload"])
        rows.append(row_dict)
    return rows


def _canonical_to_measurement_event(
    record: dict[str, Any],
    event_type: str,
    event_subtype: str | None,
) -> dict[str, Any]:
    """Create a measurement_event row from a canonical form_response.

    Extracts subject_id, form metadata, and wraps in FHIR-adjacent
    measurement event structure.
    """
    payload = record.get("payload", {})
    if isinstance(payload, str):
        payload = json.loads(payload)

    # Use canonical idem_key as measurement_event_id (1:1 mapping)
    idem_key = record["idem_key"]

    # Get subject_id from respondent
    respondent = payload.get("respondent", {})
    subject_id = respondent.get("id") or respondent.get("email")

    # Get form metadata
    form_id = payload.get("form_id")
    form_submission_id = payload.get("response_id") or payload.get("submission_id")
    occurred_at = payload.get("submitted_at")

    now = datetime.now(timezone.utc).isoformat()

    return {
        "idem_key": idem_key,
        "measurement_event_id": idem_key,
        "subject_id": subject_id,
        "subject_contact_id": None,
        "event_type": event_type,
        "event_subtype": event_subtype,
        "occurred_at": occurred_at,
        "received_at": now,
        "source_system": "google_forms",
        "source_entity": "form_response",
        "source_id": form_submission_id,
        "canonical_object_id": idem_key,
        "form_id": form_id,
        "binding_id": event_subtype,
        "binding_version": None,
        "metadata": json.dumps({}),
        "correlation_id": record.get("correlation_id"),
        "processed_at": now,
        "created_at": now,
    }
