#!/usr/bin/env python
"""Apply schema changes to event_log table."""

import os
from google.cloud import bigquery

def apply_schema_changes():
    """Apply ALTER statements to event_log."""
    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")
    table_ref = f"{client.project}.{dataset}.event_log"

    print(f"Applying schema changes to {table_ref}...")

    # 1. Make idem_key nullable
    print("\n1. Making idem_key nullable...")
    query1 = f"""
    ALTER TABLE `{table_ref}`
    ALTER COLUMN idem_key DROP NOT NULL
    """
    job1 = client.query(query1)
    job1.result()
    print("   ✓ idem_key is now nullable")

    # 2. Add payload column
    print("\n2. Adding payload column (JSON)...")
    query2 = f"""
    ALTER TABLE `{table_ref}`
    ADD COLUMN IF NOT EXISTS payload JSON OPTIONS(description="Small telemetry payload for event metadata (NOT full objects)")
    """
    job2 = client.query(query2)
    job2.result()
    print("   ✓ payload column added")

    # 3. Add trace_id column
    print("\n3. Adding trace_id column...")
    query3 = f"""
    ALTER TABLE `{table_ref}`
    ADD COLUMN IF NOT EXISTS trace_id STRING OPTIONS(description="Optional trace ID for cross-system request tracing")
    """
    job3 = client.query(query3)
    job3.result()
    print("   ✓ trace_id column added")

    print("\n✓ All schema changes applied successfully!")

if __name__ == "__main__":
    apply_schema_changes()
