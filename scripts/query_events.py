#!/usr/bin/env python3
"""
Query events from BigQuery.

Quick utility to view events and verify data is being written correctly.

Usage:
    source .env
    python3 scripts/query_events.py
"""

import os
import sys
from datetime import datetime, timedelta

try:
    from google.cloud import bigquery
except ImportError:
    print("ERROR: google-cloud-bigquery not installed")
    print("Run: pip install google-cloud-bigquery")
    sys.exit(1)

# ANSI colors
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'


def main():
    # Get config from environment
    project = os.environ.get('GCP_PROJECT')
    dataset = os.environ.get('EVENTS_BQ_DATASET')

    if not project or not dataset:
        print("ERROR: Environment variables not set")
        print("Run: source .env")
        sys.exit(1)

    # Initialize client
    client = bigquery.Client()

    print("")
    print("=" * 80)
    print("Event Log Summary")
    print("=" * 80)
    print("")

    # Query 1: Event counts
    query = f"""
    SELECT
      event_type,
      source_system,
      object_type,
      COUNT(*) as event_count,
      MIN(created_at) as first_event,
      MAX(created_at) as last_event
    FROM `{project}.{dataset}.event_log`
    GROUP BY event_type, source_system, object_type
    ORDER BY event_count DESC
    """

    print("Event Counts by Type:")
    print("-" * 80)
    results = client.query(query).result()

    total_events = 0
    for row in results:
        total_events += row.event_count
        print(f"{GREEN}{row.event_type:40s}{NC} "
              f"{row.source_system:30s} "
              f"{BLUE}{row.event_count:>8,}{NC} events "
              f"({row.first_event.strftime('%Y-%m-%d %H:%M')} - "
              f"{row.last_event.strftime('%Y-%m-%d %H:%M')})")

    if total_events == 0:
        print(f"{YELLOW}No events found{NC}")
        print("")
        print("Run an ingestion job to see data here:")
        print("  lorchestra run-job lorchestra gmail_ingest_acct1")

    print("")
    print(f"Total Events: {total_events:,}")

    # Query 2: Object counts
    print("")
    print("=" * 80)
    print("Raw Objects Summary")
    print("=" * 80)
    print("")

    query = f"""
    SELECT
      object_type,
      source_system,
      COUNT(*) as object_count,
      MIN(first_seen) as oldest,
      MAX(last_seen) as newest
    FROM `{project}.{dataset}.raw_objects`
    GROUP BY object_type, source_system
    ORDER BY object_count DESC
    """

    print("Object Counts (Deduplicated):")
    print("-" * 80)
    results = client.query(query).result()

    total_objects = 0
    for row in results:
        total_objects += row.object_count
        print(f"{GREEN}{row.object_type:20s}{NC} "
              f"{row.source_system:40s} "
              f"{BLUE}{row.object_count:>8,}{NC} objects")

    if total_objects == 0:
        print(f"{YELLOW}No objects found{NC}")

    print("")
    print(f"Total Unique Objects: {total_objects:,}")

    # Query 3: Recent events
    if total_events > 0:
        print("")
        print("=" * 80)
        print("Recent Events (Last 10)")
        print("=" * 80)
        print("")

        query = f"""
        SELECT
          event_id,
          event_type,
          source_system,
          object_type,
          created_at,
          status
        FROM `{project}.{dataset}.event_log`
        ORDER BY created_at DESC
        LIMIT 10
        """

        results = client.query(query).result()

        for row in results:
            status_color = GREEN if row.status == 'ok' else YELLOW
            print(f"{row.created_at.strftime('%Y-%m-%d %H:%M:%S')} "
                  f"{status_color}{row.status:5s}{NC} "
                  f"{row.event_type:30s} "
                  f"{row.source_system:30s}")

    # Query 4: Idempotency check
    if total_events > 0 and total_objects > 0:
        print("")
        print("=" * 80)
        print("Idempotency Check")
        print("=" * 80)
        print("")

        query = f"""
        SELECT
          COUNT(DISTINCT event_id) as total_events,
          COUNT(DISTINCT idem_key) as unique_objects,
          ROUND(COUNT(DISTINCT event_id) / COUNT(DISTINCT idem_key), 2) as duplication_ratio
        FROM `{project}.{dataset}.event_log`
        """

        result = list(client.query(query).result())[0]

        print(f"Total Events:     {result.total_events:>10,}")
        print(f"Unique Objects:   {result.unique_objects:>10,}")
        print(f"Duplication:      {result.duplication_ratio:>10.2f}x")

        if result.duplication_ratio > 1:
            print("")
            print(f"{GREEN}✓ Idempotency working!{NC}")
            print(f"  Same objects ingested multiple times → deduplicated in raw_objects")
        else:
            print("")
            print(f"{BLUE}Each object ingested once{NC}")

    print("")


if __name__ == "__main__":
    main()
