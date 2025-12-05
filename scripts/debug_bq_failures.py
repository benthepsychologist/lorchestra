#!/usr/bin/env python3
"""Debug BigQuery ingestion issues."""

import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspace/lorchestra/credentials/lorchestra-events-key.json'
os.environ['GCP_PROJECT'] = 'local-orchestration'
os.environ['EVENTS_BQ_DATASET'] = 'events_dev'

client = bigquery.Client(project='local-orchestration')

# Check failed events
print("\n" + "="*80)
print("FAILED EVENTS (Last 24 Hours)")
print("="*80)

query_failures = """
SELECT
  created_at,
  event_type,
  source_system,
  status,
  error_message,
  payload
FROM `local-orchestration.events_dev.event_log`
WHERE DATE(created_at) = CURRENT_DATE()
  AND status = 'failed'
ORDER BY created_at DESC
LIMIT 20
"""

result = client.query(query_failures).result()
for row in result:
    print(f"\n[{row.created_at}] {row.event_type} - {row.source_system}")
    print(f"  Error: {row.error_message}")
    if row.payload:
        print(f"  Payload: {row.payload}")

# Check successful events with details
print("\n" + "="*80)
print("SUCCESSFUL EVENTS (Last 24 Hours)")
print("="*80)

query_success = """
SELECT
  created_at,
  event_type,
  source_system,
  status,
  payload
FROM `local-orchestration.events_dev.event_log`
WHERE DATE(created_at) = CURRENT_DATE()
  AND status = 'ok'
  AND event_type = 'ingestion.completed'
ORDER BY created_at DESC
LIMIT 20
"""

result = client.query(query_success).result()
for row in result:
    print(f"\n[{row.created_at}] {row.event_type} - {row.source_system}")
    if row.payload:
        print(f"  Payload: {row.payload}")

# Check raw_objects table
print("\n" + "="*80)
print("RAW_OBJECTS COUNT BY SOURCE")
print("="*80)

query_objects = """
SELECT
  source_system,
  object_type,
  COUNT(*) as count,
  MIN(first_seen) as earliest,
  MAX(last_seen) as latest
FROM `local-orchestration.events_dev.raw_objects`
GROUP BY source_system, object_type
ORDER BY count DESC
"""

result = client.query(query_objects).result()
if result.total_rows == 0:
    print("\n⚠️  NO DATA IN raw_objects TABLE!")
else:
    for row in result:
        print(f"\n{row.source_system} ({row.object_type})")
        print(f"  Count: {row.count}")
        print(f"  Range: {row.earliest} → {row.latest}")

print("\n" + "="*80 + "\n")
