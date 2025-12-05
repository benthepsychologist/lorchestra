#!/usr/bin/env python
"""Nuclear clean - truncate all BigQuery tables."""

from google.cloud import bigquery
import os

client = bigquery.Client()
dataset = os.environ.get('EVENTS_BQ_DATASET', 'events_dev')

print('\n' + '='*80)
print('NUCLEAR CLEAN - TRUNCATING ALL TABLES')
print('='*80)
print('\nNote: Using TRUNCATE TABLE (removes all data including streaming buffer)')

print('\nTruncating event_log...')
query = f'TRUNCATE TABLE `{dataset}.event_log`'
client.query(query).result()
print('✓ Truncated event_log')

print('\nTruncating raw_objects...')
query = f'TRUNCATE TABLE `{dataset}.raw_objects`'
client.query(query).result()
print('✓ Truncated raw_objects')

# Verify
query = f'SELECT COUNT(*) as count FROM `{dataset}.event_log`'
event_count = next(iter(client.query(query).result())).count
print(f'\nAfter: {event_count} events in event_log')

query = f'SELECT COUNT(*) as count FROM `{dataset}.raw_objects`'
object_count = next(iter(client.query(query).result())).count
print(f'After: {object_count} objects in raw_objects')

print('\n' + '='*80)
print('✓ CLEAN SLATE - READY FOR PRODUCTION!')
print('='*80)
print('\nYou can now run real ingestion jobs:')
print('  lorchestra run gmail_ingest_acct1 --since "2025-11-01"')
print('  lorchestra run gmail_ingest_acct2')
print('  lorchestra run gmail_ingest_acct3')
