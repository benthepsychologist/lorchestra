#!/usr/bin/env python3
"""
Canonization job for emails.

Transforms validated raw email payloads to canonical JMAP Lite format.
Only processes records with validation_status = 'pass'.

Usage:
    python scripts/canonize_emails.py [--dry-run] [--limit N] [--source gmail|exchange]
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
from jsonata import Jsonata  # Use Python jsonata directly for performance

# Cache compiled transforms to avoid re-parsing
_compiled_transforms = {}


def get_transform(source_system: str) -> Jsonata:
    """Get cached compiled transform for source system."""
    if source_system not in _compiled_transforms:
        # Load jsonata file
        registry_root = Path("/workspace/lorchestra/.canonizer/registry")
        if source_system == "gmail":
            jsonata_path = registry_root / "transforms/email/gmail_to_jmap_lite/1.0.0/spec.jsonata"
        elif source_system == "exchange":
            jsonata_path = registry_root / "transforms/email/exchange_to_jmap_lite/1.0.0/spec.jsonata"
        else:
            raise ValueError(f"Unknown source_system: {source_system}")

        jsonata_expr = jsonata_path.read_text()
        _compiled_transforms[source_system] = Jsonata(jsonata_expr)

    return _compiled_transforms[source_system]


# Transform mappings: source_system -> transform info
TRANSFORM_MAPPINGS = {
    "gmail": {
        "transform_id": "email/gmail_to_jmap_lite@1.0.0",
        "canonical_schema": "iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0",
        "canonical_format": "jmap_lite",
    },
    "exchange": {
        "transform_id": "email/exchange_to_jmap_lite@1.0.0",
        "canonical_schema": "iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0",
        "canonical_format": "jmap_lite",
    },
}


def make_canonization_stamp() -> str:
    """Create canonization timestamp."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canonize_source(
    client: bigquery.Client,
    source_system: str,
    dry_run: bool,
    limit: Optional[int] = None
) -> dict:
    """Canonize all validated records for a source system."""

    if source_system not in TRANSFORM_MAPPINGS:
        print(f"No transform mapping for {source_system}, skipping")
        return {"total": 0, "success": 0, "failed": 0}

    transform_info = TRANSFORM_MAPPINGS[source_system]
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")

    print(f"\n{'='*60}")
    print(f"Canonizing {source_system}/email")
    print(f"Transform: {transform_info['transform_id']}")
    print(f"Output schema: {transform_info['canonical_schema']}")
    print(f"{'='*60}")

    # Query validated but not yet canonized records
    # Check if already canonized by looking for existing idem_key in canonical_objects
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
        SELECT
            r.idem_key,
            r.source_system,
            r.connection_name,
            r.object_type,
            r.schema_iglu,
            r.first_seen,
            r.last_seen,
            r.correlation_id,
            TO_JSON_STRING(r.payload) as payload_json
        FROM `{dataset}.raw_objects` r
        LEFT JOIN `{dataset}.canonical_objects` c ON r.idem_key = c.idem_key
        WHERE r.source_system = '{source_system}'
          AND r.object_type = 'email'
          AND r.validation_status = 'pass'
          AND c.idem_key IS NULL
        {limit_clause}
    """

    print(f"Querying validated {source_system}/email records not yet canonized...")
    results = list(client.query(query).result())
    print(f"Found {len(results)} records to canonize")

    if not results:
        return {"total": 0, "success": 0, "failed": 0}

    # Canonize each record using Python jsonata (much faster than node subprocess)
    success_records = []
    failed_records = []
    canonization_stamp = make_canonization_stamp()

    # Get compiled transform (cached)
    transform = get_transform(source_system)

    for i, row in enumerate(results):
        try:
            payload = json.loads(row.payload_json)

            # Transform using Python jsonata
            canonical = transform.evaluate(payload)

            record = {
                "idem_key": row.idem_key,
                "source_system": row.source_system,
                "connection_name": row.connection_name,
                "object_type": row.object_type,
                "canonical_schema": transform_info["canonical_schema"],
                "canonical_format": transform_info["canonical_format"],
                "transform_ref": transform_info["transform_id"],
                "validation_stamp": canonization_stamp,
                "payload": canonical,
                "correlation_id": row.correlation_id,  # Propagate from raw_objects
                "trace_id": None,  # Could be added later for distributed tracing
                "created_at": datetime.now(timezone.utc).isoformat(),
                "source_first_seen": row.first_seen.isoformat() if row.first_seen else None,
                "source_last_seen": row.last_seen.isoformat() if row.last_seen else None,
            }
            success_records.append(record)

        except Exception as e:
            failed_records.append({
                "idem_key": row.idem_key,
                "error": str(e),
            })
            if len(failed_records) <= 3:
                print(f"  FAILED: {row.idem_key[:50]}...")
                print(f"    Error: {e}")

        if (i + 1) % 500 == 0:
            print(f"  Canonized {i + 1}/{len(results)}...")

    print(f"\nResults: {len(success_records)} success, {len(failed_records)} failed")

    # Insert canonical records in batches (BigQuery has ~10MB request limit)
    if not dry_run and success_records:
        print(f"Inserting {len(success_records)} canonical records...")

        batch_size = 100  # Smaller batches to avoid request size limits
        total_inserted = 0

        for batch_start in range(0, len(success_records), batch_size):
            batch = success_records[batch_start:batch_start + batch_size]

            # Create temp table for this batch
            temp_table_id = f"{dataset}._canonical_temp_{int(time.time())}_{batch_start}"

            # Prepare rows for insert (convert payload to JSON string for temp table)
            rows_to_insert = []
            for r in batch:
                rows_to_insert.append({
                    "idem_key": r["idem_key"],
                    "source_system": r["source_system"],
                    "connection_name": r["connection_name"],
                    "object_type": r["object_type"],
                    "canonical_schema": r["canonical_schema"],
                    "canonical_format": r["canonical_format"],
                    "transform_ref": r["transform_ref"],
                    "validation_stamp": r["validation_stamp"],
                    "payload": json.dumps(r["payload"]),  # JSON string for temp table
                    "correlation_id": r["correlation_id"],
                    "trace_id": r["trace_id"],
                    "created_at": r["created_at"],
                    "source_first_seen": r["source_first_seen"],
                    "source_last_seen": r["source_last_seen"],
                })

            # Create temp table schema
            temp_schema = [
                bigquery.SchemaField("idem_key", "STRING"),
                bigquery.SchemaField("source_system", "STRING"),
                bigquery.SchemaField("connection_name", "STRING"),
                bigquery.SchemaField("object_type", "STRING"),
                bigquery.SchemaField("canonical_schema", "STRING"),
                bigquery.SchemaField("canonical_format", "STRING"),
                bigquery.SchemaField("transform_ref", "STRING"),
                bigquery.SchemaField("validation_stamp", "STRING"),
                bigquery.SchemaField("payload", "STRING"),  # JSON as string
                bigquery.SchemaField("correlation_id", "STRING"),
                bigquery.SchemaField("trace_id", "STRING"),
                bigquery.SchemaField("created_at", "STRING"),
                bigquery.SchemaField("source_first_seen", "STRING"),
                bigquery.SchemaField("source_last_seen", "STRING"),
            ]

            temp_table = bigquery.Table(f"{client.project}.{temp_table_id}", schema=temp_schema)
            temp_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
            client.create_table(temp_table)

            # Insert rows
            errors = client.insert_rows_json(temp_table, rows_to_insert)
            if errors:
                print(f"  Warning: {len(errors)} insert errors in batch {batch_start}")

            # Insert from temp to canonical_objects
            insert_query = f"""
                INSERT INTO `{dataset}.canonical_objects` (
                    idem_key, source_system, connection_name, object_type,
                    canonical_schema, canonical_format, transform_ref, validation_stamp,
                    payload, correlation_id, trace_id, created_at, source_first_seen, source_last_seen
                )
                SELECT
                    idem_key, source_system, connection_name, object_type,
                    canonical_schema, canonical_format, transform_ref, validation_stamp,
                    PARSE_JSON(payload), correlation_id, trace_id,
                    TIMESTAMP(created_at),
                    TIMESTAMP(source_first_seen),
                    TIMESTAMP(source_last_seen)
                FROM `{temp_table_id}`
            """

            job = client.query(insert_query)
            job.result()
            total_inserted += job.num_dml_affected_rows

            # Clean up temp table
            client.delete_table(temp_table)

            print(f"  Inserted {min(batch_start + batch_size, len(success_records))}/{len(success_records)}...")

        print(f"✓ Canonized {total_inserted} records")
    else:
        print(f"[DRY-RUN] Would insert {len(success_records)} canonical records")

    return {
        "total": len(results),
        "success": len(success_records),
        "failed": len(failed_records),
    }


def main():
    parser = argparse.ArgumentParser(description="Canonize validated emails to JMAP Lite format")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually insert records")
    parser.add_argument("--limit", type=int, help="Limit records per source (for testing)")
    parser.add_argument("--source", choices=["gmail", "exchange"], help="Only process specific source")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    client = bigquery.Client(project=os.environ.get("GCP_PROJECT", "local-orchestration"))

    start_time = time.time()
    results = {}

    # Canonize specified or all sources
    sources = [args.source] if args.source else list(TRANSFORM_MAPPINGS.keys())

    for source_system in sources:
        results[source_system] = canonize_source(
            client, source_system, args.dry_run, args.limit
        )

    duration = time.time() - start_time

    # Summary
    print(f"\n{'='*60}")
    print("CANONIZATION COMPLETE")
    print(f"{'='*60}")
    for key, counts in results.items():
        print(f"{key}: {counts['success']}/{counts['total']} canonized, {counts['failed']} failed")
    print(f"Duration: {duration:.1f}s")

    if args.dry_run:
        print("\n[DRY-RUN] No records were actually inserted")


if __name__ == "__main__":
    main()
