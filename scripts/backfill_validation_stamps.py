#!/usr/bin/env python3
"""
Validation job for raw_objects.

Validates raw email payloads against source schemas and detects drift.
This is a separate job from ingest - ingest just writes data, validation
runs afterward to confirm schema compliance.

Validation (gate for canonization):
- Does payload match schema structure we need for transforms?
- Required fields present? Types correct?
- validation_status = 'pass' | 'fail'
- validation_error = descriptive error (null if pass)

Drift (awareness/observability):
- What fields exist that we don't have in our schema?
- unknown_fields = JSON array of field paths we saw but don't recognize
- Purely informational - never blocks anything

Usage:
    python scripts/backfill_validation_stamps.py [--dry-run] [--limit N]
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# Add paths for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, "/workspace/canonizer")

from google.cloud import bigquery
from canonizer.core.validator import SchemaValidator, ValidationError, load_schema_from_iglu_uri


# Schema mappings: source_system + object_type -> schema info
SCHEMA_MAPPINGS = {
    ("gmail", "email"): {
        "schema_source": "gmail/v1/users.messages",
        "schema_iglu": "iglu:com.google/gmail_email/jsonschema/1-0-0",
    },
    ("exchange", "email"): {
        "schema_source": "microsoft.graph/v1.0/message",
        "schema_iglu": "iglu:com.microsoft/exchange_email/jsonschema/1-0-2",
    },
}

# Cache validators
_validators: dict[str, SchemaValidator] = {}
_schemas: dict[str, dict] = {}


def get_validator(schema_iglu: str) -> SchemaValidator:
    """Get or create a cached validator."""
    if schema_iglu not in _validators:
        schemas_dir = Path(".canonizer/registry/schemas")
        schema_path = load_schema_from_iglu_uri(schema_iglu, schemas_dir)
        _validators[schema_iglu] = SchemaValidator(schema_path)
    return _validators[schema_iglu]


def get_schema(schema_iglu: str) -> dict:
    """Get the raw schema dict for drift detection."""
    if schema_iglu not in _schemas:
        schemas_dir = Path(".canonizer/registry/schemas")
        schema_path = load_schema_from_iglu_uri(schema_iglu, schemas_dir)
        with open(schema_path) as f:
            _schemas[schema_iglu] = json.load(f)
    return _schemas[schema_iglu]


def get_schema_properties(schema: dict, prefix: str = "", root_schema: dict = None, depth: int = 0) -> set[str]:
    """Recursively get all known property paths from schema.

    Args:
        schema: Current schema node to process
        prefix: Path prefix for nested properties
        root_schema: Root schema containing definitions (for $ref resolution)
        depth: Current recursion depth (to prevent infinite recursion)
    """
    # Limit recursion depth to handle recursive schemas (like Gmail's MessagePart)
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

            # Handle $ref to definitions
            if "$ref" in prop_schema:
                ref_name = prop_schema["$ref"].split("/")[-1]
                if "definitions" in root_schema and ref_name in root_schema["definitions"]:
                    ref_schema = root_schema["definitions"][ref_name]
                    properties.update(get_schema_properties(ref_schema, full_path, root_schema, depth + 1))
            # Handle oneOf patterns (e.g., oneOf: [{$ref: ...}, {type: null}])
            elif "oneOf" in prop_schema:
                for option in prop_schema["oneOf"]:
                    if "$ref" in option:
                        ref_name = option["$ref"].split("/")[-1]
                        if "definitions" in root_schema and ref_name in root_schema["definitions"]:
                            ref_schema = root_schema["definitions"][ref_name]
                            properties.update(get_schema_properties(ref_schema, full_path, root_schema, depth + 1))
                    elif "properties" in option:
                        properties.update(get_schema_properties(option, full_path, root_schema, depth + 1))
            # Recurse into nested objects
            elif prop_schema.get("type") == "object" or "properties" in prop_schema:
                properties.update(get_schema_properties(prop_schema, full_path, root_schema, depth + 1))
            # Handle arrays of objects
            elif prop_schema.get("type") == "array" and "items" in prop_schema:
                items = prop_schema["items"]
                if "$ref" in items:
                    ref_name = items["$ref"].split("/")[-1]
                    if "definitions" in root_schema and ref_name in root_schema["definitions"]:
                        ref_schema = root_schema["definitions"][ref_name]
                        properties.update(get_schema_properties(ref_schema, full_path, root_schema, depth + 1))
                elif "properties" in items:
                    properties.update(get_schema_properties(items, full_path, root_schema, depth + 1))

    return properties


def get_payload_fields(payload: dict, prefix: str = "") -> set[str]:
    """Recursively get all field paths from payload."""
    fields = set()

    if isinstance(payload, dict):
        for key, value in payload.items():
            full_path = f"{prefix}.{key}" if prefix else key
            fields.add(full_path)
            if isinstance(value, dict):
                fields.update(get_payload_fields(value, full_path))

    return fields


def detect_unknown_fields(payload: dict, schema: dict) -> list[str]:
    """Find fields in payload that aren't in schema."""
    schema_props = get_schema_properties(schema)
    payload_fields = get_payload_fields(payload)

    # Filter to only top-level unknowns and their children
    # (don't report $.foo.bar if $.foo is already unknown)
    unknown = payload_fields - schema_props

    # Sort for consistent output
    return sorted(unknown)


def format_validation_error(errors: list[str]) -> str:
    """Format validation errors with clear descriptions of what broke.

    Groups errors by type:
    - Missing required fields
    - Type mismatches
    - Other validation errors
    """
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

    return " | ".join(parts)[:1000]  # Truncate to reasonable length


def validate_payload(payload: dict, schema_iglu: str) -> tuple[str, Optional[str], list[str]]:
    """
    Validate payload and detect drift.

    Returns:
        (validation_status, validation_error, unknown_fields)
    """
    validator = get_validator(schema_iglu)
    schema = get_schema(schema_iglu)

    # Check for unknown fields (drift detection)
    unknown_fields = detect_unknown_fields(payload, schema)

    # Validate
    try:
        validator.validate(payload)
        return "pass", None, unknown_fields
    except ValidationError as e:
        error_msg = format_validation_error(e.errors)
        return "fail", error_msg, unknown_fields


def make_validation_stamp() -> str:
    """Create validation timestamp."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_source(
    client: bigquery.Client,
    source_system: str,
    object_type: str,
    dry_run: bool,
    limit: Optional[int] = None
) -> dict:
    """Validate all unvalidated records for a source system + object type."""

    key = (source_system, object_type)
    if key not in SCHEMA_MAPPINGS:
        print(f"No schema mapping for {source_system}/{object_type}, skipping")
        return {"total": 0, "passed": 0, "failed": 0}

    schema_info = SCHEMA_MAPPINGS[key]
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    print(f"\n{'='*60}")
    print(f"Validating {source_system}/{object_type}")
    print(f"Schema: {schema_info['schema_iglu']}")
    print(f"{'='*60}")

    # Query unvalidated records
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
        SELECT idem_key, TO_JSON_STRING(payload) as payload_json
        FROM `{dataset}.raw_objects`
        WHERE source_system = '{source_system}'
          AND object_type = '{object_type}'
          AND validation_status IS NULL
        {limit_clause}
    """

    print(f"Querying unvalidated {source_system}/{object_type} records...")
    results = list(client.query(query).result())
    print(f"Found {len(results)} unvalidated records")

    if not results:
        return {"total": 0, "passed": 0, "failed": 0}

    # Validate each record
    passed_records = []
    failed_records = []
    validation_stamp = make_validation_stamp()

    for i, row in enumerate(results):
        payload = json.loads(row.payload_json)
        status, error, unknown = validate_payload(payload, schema_info["schema_iglu"])

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
                print(f"  FAILED: {row.idem_key[:50]}...")
                print(f"    Error: {error}")

        if (i + 1) % 500 == 0:
            print(f"  Validated {i + 1}/{len(results)}...")

    print(f"\nResults: {len(passed_records)} passed, {len(failed_records)} failed")

    # Collect drift stats
    all_unknown = set()
    for r in passed_records + failed_records:
        if r["unknown_fields"]:
            all_unknown.update(r["unknown_fields"])
    if all_unknown:
        print(f"Drift detected - {len(all_unknown)} unknown fields: {sorted(all_unknown)[:5]}...")

    # Batch update records using temp table + MERGE
    if not dry_run:
        all_records = passed_records + failed_records
        print(f"Updating {len(all_records)} records...")

        # Create temp table with validation results
        temp_table_id = f"{dataset}._validation_temp_{int(time.time())}"

        # Build rows for temp table insert
        rows_to_insert = []
        for r in all_records:
            rows_to_insert.append({
                "idem_key": r["idem_key"],
                "validation_status": r["status"],
                "validation_error": r["error"],
                "unknown_fields": json.dumps(r["unknown_fields"]) if r["unknown_fields"] else None,
            })

        # Create temp table schema
        temp_schema = [
            bigquery.SchemaField("idem_key", "STRING"),
            bigquery.SchemaField("validation_status", "STRING"),
            bigquery.SchemaField("validation_error", "STRING"),
            bigquery.SchemaField("unknown_fields", "STRING"),
        ]

        # Create and load temp table
        temp_table = bigquery.Table(f"{client.project}.{temp_table_id}", schema=temp_schema)
        temp_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)  # Auto-delete
        client.create_table(temp_table)
        print(f"  Created temp table: {temp_table_id}")

        # Insert rows in batches
        errors = client.insert_rows_json(temp_table, rows_to_insert)
        if errors:
            print(f"  Warning: {len(errors)} insert errors")

        print(f"  Inserted {len(rows_to_insert)} rows into temp table")

        # Run MERGE to update raw_objects
        merge_query = f"""
            MERGE `{dataset}.raw_objects` AS target
            USING `{temp_table_id}` AS source
            ON target.idem_key = source.idem_key
            WHEN MATCHED THEN UPDATE SET
                schema_source = '{schema_info["schema_source"]}',
                schema_iglu = '{schema_info["schema_iglu"]}',
                validation_status = source.validation_status,
                validation_stamp = '{validation_stamp}',
                validation_error = source.validation_error,
                unknown_fields = PARSE_JSON(source.unknown_fields)
        """

        job = client.query(merge_query)
        result = job.result()
        print(f"  MERGE updated {job.num_dml_affected_rows} rows")

        # Clean up temp table
        client.delete_table(temp_table)
        print(f"✓ Updated {len(all_records)} records")
    else:
        print(f"[DRY-RUN] Would update {len(passed_records) + len(failed_records)} records")

    return {
        "total": len(results),
        "passed": len(passed_records),
        "failed": len(failed_records),
        "unknown_field_count": len(all_unknown),
    }


def main():
    parser = argparse.ArgumentParser(description="Validate raw_objects against schemas")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually update records")
    parser.add_argument("--limit", type=int, help="Limit records per source (for testing)")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    client = bigquery.Client(project=os.environ.get("GCP_PROJECT", "local-orchestration"))

    start_time = time.time()
    results = {}

    # Validate all known source/object combinations
    for (source_system, object_type) in SCHEMA_MAPPINGS.keys():
        results[f"{source_system}/{object_type}"] = validate_source(
            client, source_system, object_type, args.dry_run, args.limit
        )

    duration = time.time() - start_time

    # Summary
    print(f"\n{'='*60}")
    print("VALIDATION COMPLETE")
    print(f"{'='*60}")
    for key, counts in results.items():
        print(f"{key}: {counts['passed']}/{counts['total']} passed, {counts['failed']} failed")
        if counts.get('unknown_field_count', 0) > 0:
            print(f"  Drift: {counts['unknown_field_count']} unknown fields detected")
    print(f"Duration: {duration:.1f}s")

    if args.dry_run:
        print("\n[DRY-RUN] No records were actually updated")


if __name__ == "__main__":
    main()
