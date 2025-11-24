#!/usr/bin/env python
"""Verify BigQuery schema for refactored event_client."""

import os
from google.cloud import bigquery

def verify_schema():
    """Check if event_log schema has been updated."""
    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")
    table_ref = f"{client.project}.{dataset}.event_log"

    print(f"Checking schema for {table_ref}...")

    table = client.get_table(table_ref)

    # Check for expected fields
    schema_dict = {field.name: field for field in table.schema}

    checks = []

    # Check idem_key is nullable
    if "idem_key" in schema_dict:
        is_nullable = schema_dict["idem_key"].mode == "NULLABLE"
        checks.append(("idem_key is NULLABLE", is_nullable))
    else:
        checks.append(("idem_key exists", False))

    # Check payload exists and is JSON
    if "payload" in schema_dict:
        is_json = schema_dict["payload"].field_type == "JSON"
        is_nullable = schema_dict["payload"].mode == "NULLABLE"
        checks.append(("payload is JSON and NULLABLE", is_json and is_nullable))
    else:
        checks.append(("payload exists", False))

    # Check trace_id exists
    if "trace_id" in schema_dict:
        is_nullable = schema_dict["trace_id"].mode == "NULLABLE"
        checks.append(("trace_id is NULLABLE", is_nullable))
    else:
        checks.append(("trace_id exists", False))

    # Print results
    print("\nSchema verification:")
    all_passed = True
    for check_name, passed in checks:
        status = "✓" if passed else "✗"
        print(f"  {status} {check_name}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n✓ Schema is ready for refactored event_client!")
        return True
    else:
        print("\n✗ Schema needs to be updated. Run alter_event_log.sql first.")
        print("\nMissing fields in current schema:")
        for field in table.schema:
            print(f"  - {field.name}: {field.field_type} ({field.mode})")
        return False

if __name__ == "__main__":
    verify_schema()
