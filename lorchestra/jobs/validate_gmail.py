"""Gmail validation job.

Validates raw Gmail email payloads against source schema.
Only validates records with validation_status IS NULL.

This is a separate job from ingest - ingest just writes data, validation
runs afterward to confirm schema compliance and gate canonization.

Column Standards:
- source_system: "gmail"
- object_type: "email"
- schema_iglu: "iglu:com.google/gmail_email/jsonschema/1-0-0"
"""

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Schema configuration
SCHEMA_SOURCE = "gmail/v1/users.messages"
SCHEMA_IGLU = "iglu:com.google/gmail_email/jsonschema/1-0-0"
SOURCE_SYSTEM = "gmail"
OBJECT_TYPE = "email"


def _get_validator():
    """Get cached validator for Gmail schema."""
    import sys
    sys.path.insert(0, "/workspace/canonizer")
    from canonizer.core.validator import SchemaValidator, load_schema_from_iglu_uri

    schemas_dir = Path("/workspace/lorchestra/.canonizer/registry/schemas")
    schema_path = load_schema_from_iglu_uri(SCHEMA_IGLU, schemas_dir)
    return SchemaValidator(schema_path)


def _get_schema():
    """Get raw schema dict for drift detection."""
    import sys
    sys.path.insert(0, "/workspace/canonizer")
    from canonizer.core.validator import load_schema_from_iglu_uri

    schemas_dir = Path("/workspace/lorchestra/.canonizer/registry/schemas")
    schema_path = load_schema_from_iglu_uri(SCHEMA_IGLU, schemas_dir)
    with open(schema_path) as f:
        return json.load(f)


def _get_schema_properties(schema: dict, prefix: str = "", root_schema: dict = None, depth: int = 0) -> set[str]:
    """Recursively get all known property paths from schema."""
    MAX_DEPTH = 5
    if depth > MAX_DEPTH:
        return set()

    if root_schema is None:
        root_schema = schema

    properties = set()

    if "properties" in schema:
        for prop_name, prop_schema in schema["properties"].items():
            full_path = f"{prefix}.{prop_name}" if prefix else prop_name
            properties.add(full_path)

            if "$ref" in prop_schema:
                ref_name = prop_schema["$ref"].split("/")[-1]
                if "definitions" in root_schema and ref_name in root_schema["definitions"]:
                    ref_schema = root_schema["definitions"][ref_name]
                    properties.update(_get_schema_properties(ref_schema, full_path, root_schema, depth + 1))
            elif "oneOf" in prop_schema:
                for option in prop_schema["oneOf"]:
                    if "$ref" in option:
                        ref_name = option["$ref"].split("/")[-1]
                        if "definitions" in root_schema and ref_name in root_schema["definitions"]:
                            ref_schema = root_schema["definitions"][ref_name]
                            properties.update(_get_schema_properties(ref_schema, full_path, root_schema, depth + 1))
                    elif "properties" in option:
                        properties.update(_get_schema_properties(option, full_path, root_schema, depth + 1))
            elif prop_schema.get("type") == "object" or "properties" in prop_schema:
                properties.update(_get_schema_properties(prop_schema, full_path, root_schema, depth + 1))
            elif prop_schema.get("type") == "array" and "items" in prop_schema:
                items = prop_schema["items"]
                if "$ref" in items:
                    ref_name = items["$ref"].split("/")[-1]
                    if "definitions" in root_schema and ref_name in root_schema["definitions"]:
                        ref_schema = root_schema["definitions"][ref_name]
                        properties.update(_get_schema_properties(ref_schema, full_path, root_schema, depth + 1))
                elif "properties" in items:
                    properties.update(_get_schema_properties(items, full_path, root_schema, depth + 1))

    return properties


def _get_payload_fields(payload: dict, prefix: str = "") -> set[str]:
    """Recursively get all field paths from payload."""
    fields = set()

    if isinstance(payload, dict):
        for key, value in payload.items():
            full_path = f"{prefix}.{key}" if prefix else key
            fields.add(full_path)
            if isinstance(value, dict):
                fields.update(_get_payload_fields(value, full_path))

    return fields


def _detect_unknown_fields(payload: dict, schema: dict) -> list[str]:
    """Find fields in payload that aren't in schema."""
    schema_props = _get_schema_properties(schema)
    payload_fields = _get_payload_fields(payload)
    unknown = payload_fields - schema_props
    return sorted(unknown)


def _format_validation_error(errors: list[str]) -> str:
    """Format validation errors with clear descriptions."""
    missing_required = []
    type_errors = []
    other_errors = []

    for err in errors:
        if "is a required property" in err or "required property" in err.lower():
            missing_required.append(err)
        elif "is not of type" in err or "type" in err.lower():
            type_errors.append(err)
        else:
            other_errors.append(err)

    parts = []
    if missing_required:
        parts.append(f"Missing required: {'; '.join(missing_required[:3])}")
    if type_errors:
        parts.append(f"Type mismatch: {'; '.join(type_errors[:3])}")
    if other_errors:
        parts.append(f"Other: {'; '.join(other_errors[:3])}")

    return " | ".join(parts)[:1000]


def _validate_payload(payload: dict, validator, schema: dict) -> tuple[str, Optional[str], list[str]]:
    """Validate payload and detect drift."""
    import sys
    sys.path.insert(0, "/workspace/canonizer")
    from canonizer.core.validator import ValidationError

    unknown_fields = _detect_unknown_fields(payload, schema)

    try:
        validator.validate(payload)
        return "pass", None, unknown_fields
    except ValidationError as e:
        error_msg = _format_validation_error(e.errors)
        return "fail", error_msg, unknown_fields


def job_validate_gmail_source(bq_client, limit: Optional[int] = None, **kwargs):
    """
    Validate Gmail email records in raw_objects.

    Validates raw payloads against iglu:com.google/gmail_email/jsonschema/1-0-0.
    Sets validation_status, validation_error, unknown_fields, schema_source, schema_iglu.

    Args:
        bq_client: BigQuery client
        limit: Optional limit on records to process (for testing)
    """
    from lorchestra.stack_clients.event_client import log_event

    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")
    run_id = f"validate-gmail-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    logger.info(f"Starting Gmail validation job: run_id={run_id}")
    logger.info(f"Schema: {SCHEMA_IGLU}")

    start_time = time.time()

    # Query unvalidated records
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
        SELECT idem_key, TO_JSON_STRING(payload) as payload_json
        FROM `{dataset}.raw_objects`
        WHERE source_system = '{SOURCE_SYSTEM}'
          AND object_type = '{OBJECT_TYPE}'
          AND validation_status IS NULL
        {limit_clause}
    """

    logger.info("Querying unvalidated Gmail records...")
    results = list(bq_client.query(query).result())
    logger.info(f"Found {len(results)} unvalidated records")

    if not results:
        logger.info("No records to validate")
        return

    # Get validator and schema
    validator = _get_validator()
    schema = _get_schema()

    # Validate each record
    passed_records = []
    failed_records = []
    validation_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for i, row in enumerate(results):
        payload = json.loads(row.payload_json)
        status, error, unknown = _validate_payload(payload, validator, schema)

        record = {
            "idem_key": row.idem_key,
            "status": status,
            "error": error,
            "unknown_fields": unknown if unknown else None,
        }

        if status == "pass":
            passed_records.append(record)
        else:
            failed_records.append(record)
            if len(failed_records) <= 3:
                logger.warning(f"FAILED: {row.idem_key[:50]}... Error: {error}")

        if (i + 1) % 500 == 0:
            logger.info(f"Validated {i + 1}/{len(results)}...")

    logger.info(f"Results: {len(passed_records)} passed, {len(failed_records)} failed")

    # Collect drift stats
    all_unknown = set()
    for r in passed_records + failed_records:
        if r["unknown_fields"]:
            all_unknown.update(r["unknown_fields"])
    if all_unknown:
        logger.info(f"Drift detected - {len(all_unknown)} unknown fields: {sorted(all_unknown)[:5]}...")

    # Batch update records using temp table + MERGE
    all_records = passed_records + failed_records
    logger.info(f"Updating {len(all_records)} records...")

    temp_table_id = f"{dataset}._validation_gmail_temp_{int(time.time())}"

    rows_to_insert = []
    for r in all_records:
        rows_to_insert.append({
            "idem_key": r["idem_key"],
            "validation_status": r["status"],
            "validation_error": r["error"],
            "unknown_fields": json.dumps(r["unknown_fields"]) if r["unknown_fields"] else None,
        })

    temp_schema = [
        bigquery.SchemaField("idem_key", "STRING"),
        bigquery.SchemaField("validation_status", "STRING"),
        bigquery.SchemaField("validation_error", "STRING"),
        bigquery.SchemaField("unknown_fields", "STRING"),
    ]

    temp_table = bigquery.Table(f"{bq_client.project}.{temp_table_id}", schema=temp_schema)
    temp_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    bq_client.create_table(temp_table)

    errors = bq_client.insert_rows_json(temp_table, rows_to_insert)
    if errors:
        logger.warning(f"Warning: {len(errors)} insert errors")

    merge_query = f"""
        MERGE `{dataset}.raw_objects` AS target
        USING `{temp_table_id}` AS source
        ON target.idem_key = source.idem_key
        WHEN MATCHED THEN UPDATE SET
            schema_source = '{SCHEMA_SOURCE}',
            schema_iglu = '{SCHEMA_IGLU}',
            validation_status = source.validation_status,
            validation_stamp = '{validation_stamp}',
            validation_error = source.validation_error,
            unknown_fields = PARSE_JSON(source.unknown_fields)
    """

    job = bq_client.query(merge_query)
    job.result()
    logger.info(f"MERGE updated {job.num_dml_affected_rows} rows")

    bq_client.delete_table(temp_table)

    duration_seconds = time.time() - start_time

    # Log validation.completed event
    log_event(
        event_type="validation.completed",
        source_system=SOURCE_SYSTEM,
        connection_name=None,
        target_object_type=OBJECT_TYPE,
        correlation_id=run_id,
        status="ok",
        payload={
            "records_validated": len(all_records),
            "passed": len(passed_records),
            "failed": len(failed_records),
            "unknown_field_count": len(all_unknown),
            "schema_iglu": SCHEMA_IGLU,
            "duration_seconds": round(duration_seconds, 2),
        },
        bq_client=bq_client,
    )

    logger.info(f"Gmail validation complete: {len(passed_records)}/{len(all_records)} passed, duration={duration_seconds:.2f}s")
