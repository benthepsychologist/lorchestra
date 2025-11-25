# Schema Migration: Standardize Source System Columns

## Overview

This migration separates concerns in `raw_objects` and `event_log` following industry standards (Airbyte, Singer/Meltano):
- **source_system**: provider family (gmail, exchange, dataverse, google_forms)
- **connection_name**: configured account (gmail-acct1, exchange-ben-mensio)
- **object_type** / **target_object_type**: domain object (email, contact, session)

## Current Schema

### raw_objects (current)

| Column | Type | Mode | Notes |
|--------|------|------|-------|
| idem_key | STRING | REQUIRED | Currently: `{object_type}:{source_system}:{external_id}` |
| source_system | STRING | REQUIRED | **PROBLEM**: Conflates provider + connection (e.g., `google-forms--intake-01`) |
| object_type | STRING | REQUIRED | OK |
| external_id | STRING | NULLABLE | OK |
| payload | JSON | REQUIRED | OK |
| first_seen | TIMESTAMP | REQUIRED | OK |
| last_seen | TIMESTAMP | REQUIRED | OK |

### event_log (current)

| Column | Type | Mode | Notes |
|--------|------|------|-------|
| event_id | STRING | REQUIRED | OK |
| event_type | STRING | REQUIRED | OK |
| source_system | STRING | REQUIRED | **PROBLEM**: Same conflation as raw_objects |
| object_type | STRING | NULLABLE | **PROBLEM**: Confusing - events aren't objects |
| idem_key | STRING | NULLABLE | **PROBLEM**: Events don't have idem_keys |
| correlation_id | STRING | NULLABLE | OK |
| subject_id | STRING | NULLABLE | Unused - DROP |
| created_at | TIMESTAMP | REQUIRED | OK |
| status | STRING | REQUIRED | OK |
| error_message | STRING | NULLABLE | OK |
| payload | JSON | NULLABLE | OK |
| trace_id | STRING | NULLABLE | OK |

## Target Schema

### raw_objects (target)

| Column | Type | Mode | Notes |
|--------|------|------|-------|
| idem_key | STRING | REQUIRED | New pattern: `{source_system}:{connection_name}:{object_type}:{external_id}` |
| source_system | STRING | REQUIRED | Provider only: `gmail`, `exchange`, `dataverse`, `google_forms` |
| connection_name | STRING | REQUIRED | **NEW**: Account: `gmail-acct1`, `exchange-ben-mensio` |
| object_type | STRING | REQUIRED | Domain object: `email`, `contact`, `session`, `form_response` |
| schema_ref | STRING | NULLABLE | **NEW**: Iglu URI: `iglu:com.mensio.raw/raw_gmail_email/jsonschema/1-0-0` |
| external_id | STRING | NULLABLE | Upstream ID (unchanged) |
| payload | JSON | REQUIRED | Full raw record (unchanged) |
| first_seen | TIMESTAMP | REQUIRED | (unchanged) |
| last_seen | TIMESTAMP | REQUIRED | (unchanged) |

### event_log (target)

| Column | Type | Mode | Notes |
|--------|------|------|-------|
| event_id | STRING | REQUIRED | (unchanged) |
| event_type | STRING | REQUIRED | Verb: `job.started`, `ingest.completed`, `upsert.completed` |
| source_system | STRING | REQUIRED | Provider or `lorchestra` for system events |
| connection_name | STRING | NULLABLE | **NEW**: Account (NULL for system events) |
| target_object_type | STRING | NULLABLE | **RENAMED**: Noun: `email`, `session` (NULL for job events) |
| event_schema_ref | STRING | NULLABLE | **NEW**: Iglu URI for event schema |
| correlation_id | STRING | NULLABLE | (unchanged) |
| trace_id | STRING | NULLABLE | (unchanged) |
| created_at | TIMESTAMP | REQUIRED | (unchanged) |
| status | STRING | REQUIRED | (unchanged) |
| error_message | STRING | NULLABLE | (unchanged) |
| payload | JSON | NULLABLE | (unchanged) |

**Dropped columns**: `object_type` (renamed), `idem_key` (events don't have idem_keys), `subject_id` (unused)

## Connection Name Mapping

| Provider | Identity | connection_name | object_type |
|----------|----------|-----------------|-------------|
| gmail | gmail:acct1 | gmail-acct1 | email |
| gmail | gmail:acct2 | gmail-acct2 | email |
| gmail | gmail:acct3 | gmail-acct3 | email |
| exchange | exchange:ben-mensio | exchange-ben-mensio | email |
| exchange | exchange:booking-mensio | exchange-booking-mensio | email |
| exchange | exchange:info-mensio | exchange-info-mensio | email |
| exchange | exchange:ben-efs | exchange-ben-efs | email |
| dataverse | dataverse:clinic | dataverse-clinic | contact |
| dataverse | dataverse:clinic | dataverse-clinic | session |
| dataverse | dataverse:clinic | dataverse-clinic | report |
| google_forms | google_forms:ipip120_01 | google-forms-ipip120 | form_response |
| google_forms | google_forms:intake_01 | google-forms-intake-01 | form_response |
| google_forms | google_forms:intake_02 | google-forms-intake-02 | form_response |
| google_forms | google_forms:followup | google-forms-followup | form_response |

## idem_key Pattern

**Format**: `{source_system}:{connection_name}:{object_type}:{external_id}`

**Examples**:
- `gmail:gmail-acct1:email:18c5a7b2e3f4d5c6`
- `exchange:exchange-ben-mensio:email:AAMkAGQ...`
- `dataverse:dataverse-clinic:session:739330df-5757-f011-bec2-6045bd619595`
- `google_forms:google-forms-intake-01:form_response:ACYDBNi84NuJUO2O13A`

**Rule**: Opaque string, never parsed. All filtering uses explicit columns.

## Event Types

| event_type | source_system | connection_name | target_object_type | When |
|------------|---------------|-----------------|-------------------|------|
| `job.started` | `lorchestra` | NULL | NULL | Job begins |
| `job.completed` | `lorchestra` | NULL | NULL | Job ends |
| `ingest.started` | provider | connection | object_type | Ingestion begins |
| `ingest.completed` | provider | connection | object_type | Ingestion ends |
| `ingest.failed` | provider | connection | object_type | Ingestion error |
| `upsert.completed` | provider | connection | object_type | Batch upsert ends |

## Migration Strategy

**Approach**: Truncate and re-ingest (~200MB total)

1. Archive existing tables (optional): `raw_objects_legacy`, `event_log_legacy`
2. Drop and recreate tables with new schema
3. Re-run all ingestion jobs with updated code

This is safer than SQL surgery on idem_keys.
