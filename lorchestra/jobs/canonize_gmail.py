"""Gmail canonization job.

Transforms validated raw Gmail payloads to canonical JMAP Lite format.
Only processes records with validation_status = 'pass' that haven't been canonized yet.

Column Standards:
- source_system: "gmail"
- object_type: "email"
- transform_ref: "email/gmail_to_jmap_lite@1.0.0"
- canonical_schema: "iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0"
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

# Transform configuration
SOURCE_SYSTEM = "gmail"
OBJECT_TYPE = "email"
TRANSFORM_ID = "email/gmail_to_jmap_lite@1.0.0"
CANONICAL_SCHEMA = "iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0"
CANONICAL_FORMAT = "jmap_lite"

# Cache compiled transform
_compiled_transform = None


def _get_transform():
    """Get cached compiled transform for Gmail."""
    global _compiled_transform
    if _compiled_transform is None:
        from jsonata import Jsonata

        registry_root = Path("/workspace/lorchestra/.canonizer/registry")
        jsonata_path = registry_root / "transforms/email/gmail_to_jmap_lite/1.0.0/spec.jsonata"
        jsonata_expr = jsonata_path.read_text()
        _compiled_transform = Jsonata(jsonata_expr)

    return _compiled_transform


def job_canonize_gmail_jmap(bq_client, limit: Optional[int] = None, **kwargs):
    """
    Canonize Gmail email records to JMAP Lite format.

    Transforms validated payloads using email/gmail_to_jmap_lite@1.0.0.
    Only processes records with validation_status='pass' not yet in canonical_objects.

    Args:
        bq_client: BigQuery client
        limit: Optional limit on records to process (for testing)
    """
    from lorchestra.stack_clients.event_client import log_event

    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")
    run_id = f"canonize-gmail-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    logger.info(f"Starting Gmail canonization job: run_id={run_id}")
    logger.info(f"Transform: {TRANSFORM_ID}")
    logger.info(f"Output schema: {CANONICAL_SCHEMA}")

    start_time = time.time()

    # Query validated but not yet canonized records
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
        WHERE r.source_system = '{SOURCE_SYSTEM}'
          AND r.object_type = '{OBJECT_TYPE}'
          AND r.validation_status = 'pass'
          AND c.idem_key IS NULL
        {limit_clause}
    """

    logger.info("Querying validated Gmail records not yet canonized...")
    results = list(bq_client.query(query).result())
    logger.info(f"Found {len(results)} records to canonize")

    if not results:
        logger.info("No records to canonize")
        return

    # Get compiled transform (cached)
    transform = _get_transform()

    # Canonize each record
    success_records = []
    failed_records = []
    canonization_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for i, row in enumerate(results):
        try:
            payload = json.loads(row.payload_json)
            canonical = transform.evaluate(payload)

            record = {
                "idem_key": row.idem_key,
                "source_system": row.source_system,
                "connection_name": row.connection_name,
                "object_type": row.object_type,
                "canonical_schema": CANONICAL_SCHEMA,
                "canonical_format": CANONICAL_FORMAT,
                "transform_ref": TRANSFORM_ID,
                "validation_stamp": canonization_stamp,
                "payload": canonical,
                "correlation_id": row.correlation_id,
                "trace_id": None,
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
                logger.warning(f"FAILED: {row.idem_key[:50]}... Error: {e}")

        if (i + 1) % 500 == 0:
            logger.info(f"Canonized {i + 1}/{len(results)}...")

    logger.info(f"Results: {len(success_records)} success, {len(failed_records)} failed")

    # Insert canonical records in batches
    if success_records:
        logger.info(f"Inserting {len(success_records)} canonical records...")

        batch_size = 100
        total_inserted = 0

        for batch_start in range(0, len(success_records), batch_size):
            batch = success_records[batch_start:batch_start + batch_size]

            temp_table_id = f"{dataset}._canonical_gmail_temp_{int(time.time())}_{batch_start}"

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
                    "payload": json.dumps(r["payload"]),
                    "correlation_id": r["correlation_id"],
                    "trace_id": r["trace_id"],
                    "created_at": r["created_at"],
                    "source_first_seen": r["source_first_seen"],
                    "source_last_seen": r["source_last_seen"],
                })

            temp_schema = [
                bigquery.SchemaField("idem_key", "STRING"),
                bigquery.SchemaField("source_system", "STRING"),
                bigquery.SchemaField("connection_name", "STRING"),
                bigquery.SchemaField("object_type", "STRING"),
                bigquery.SchemaField("canonical_schema", "STRING"),
                bigquery.SchemaField("canonical_format", "STRING"),
                bigquery.SchemaField("transform_ref", "STRING"),
                bigquery.SchemaField("validation_stamp", "STRING"),
                bigquery.SchemaField("payload", "STRING"),
                bigquery.SchemaField("correlation_id", "STRING"),
                bigquery.SchemaField("trace_id", "STRING"),
                bigquery.SchemaField("created_at", "STRING"),
                bigquery.SchemaField("source_first_seen", "STRING"),
                bigquery.SchemaField("source_last_seen", "STRING"),
            ]

            temp_table = bigquery.Table(f"{bq_client.project}.{temp_table_id}", schema=temp_schema)
            temp_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
            bq_client.create_table(temp_table)

            errors = bq_client.insert_rows_json(temp_table, rows_to_insert)
            if errors:
                logger.warning(f"Warning: {len(errors)} insert errors in batch {batch_start}")

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

            job = bq_client.query(insert_query)
            job.result()
            total_inserted += job.num_dml_affected_rows

            bq_client.delete_table(temp_table)

            logger.info(f"Inserted {min(batch_start + batch_size, len(success_records))}/{len(success_records)}...")

        logger.info(f"Canonized {total_inserted} records")

    duration_seconds = time.time() - start_time

    # Log canonize.completed event
    log_event(
        event_type="canonize.completed",
        source_system=SOURCE_SYSTEM,
        connection_name=None,
        target_object_type=OBJECT_TYPE,
        correlation_id=run_id,
        status="ok",
        payload={
            "records_processed": len(results),
            "success": len(success_records),
            "failed": len(failed_records),
            "transform_ref": TRANSFORM_ID,
            "canonical_schema": CANONICAL_SCHEMA,
            "duration_seconds": round(duration_seconds, 2),
        },
        bq_client=bq_client,
    )

    logger.info(f"Gmail canonization complete: {len(success_records)}/{len(results)} canonized, duration={duration_seconds:.2f}s")
