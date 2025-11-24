#!/usr/bin/env python
"""Make object_type nullable in event_log."""

import os
from google.cloud import bigquery

def fix_object_type():
    """Make object_type nullable."""
    client = bigquery.Client()
    dataset = os.environ.get("EVENTS_BQ_DATASET", "events_dev")
    table_ref = f"{client.project}.{dataset}.event_log"

    print(f"Making object_type nullable in {table_ref}...")

    query = f"""
    ALTER TABLE `{table_ref}`
    ALTER COLUMN object_type DROP NOT NULL
    """
    job = client.query(query)
    job.result()
    print("✓ object_type is now nullable")

if __name__ == "__main__":
    fix_object_type()
