---
version: "0.1"
tier: C
title: Contact Events Projection
owner: benthepsychologist
goal: Implement contact_events - unified operational event ledger for all client-touch events
labels: [projection, events, operational-ledger]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-09T19:37:43.907616+00:00
updated: 2025-12-09T19:37:43.907616+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/contact-events"
---

# Contact Events Projection

## Objective

> Create `proj_contact_events` - a unified, row-per-event operational ledger that synthesizes all client-touch events across channels into a single queryable surface for automation, diagnostics, and engagement logic.

This table answers one question efficiently:

**"What has actually happened with this client across all systems, in time order?"**

### Naming Convention

| Context | Name | Notes |
|---------|------|-------|
| Logical name | `contact_events` | Used in documentation and conversation |
| BigQuery | `proj_contact_events` | Follows `proj_` prefix convention for projections |
| SQLite | `contact_events` | Synced mirror, no prefix |

## What This Is

- ✅ A projection
- ✅ Derived only from other canonicals
- ✅ Replaceable
- ✅ Overwrite-idempotent
- ✅ Append-capable later

## What This Is NOT

- ❌ A canonical source
- ❌ A state table
- ❌ A CRM substitute

## Acceptance Criteria

- [ ] `proj_contact_events` BQ view/table created
- [ ] Row-per-event grain from all source systems
- [ ] Sync job copies to SQLite `contact_events` table
- [ ] Daily pipeline includes rebuild
- [ ] CI green (lint + unit)
- [ ] **Validation check:** At least one client's full activity history can be reconstructed correctly from `proj_contact_events` (forms + sessions) and matches raw canonicals end-to-end

## Context

### Relationship to contacts

| Table | Purpose |
|-------|---------|
| `contacts` | Identity + governance + assignment |
| `contact_events` | Temporal reality |

**They must never collapse into one another.**

### Grain

**One row = one client-relevant event**

Not one per form type, not one per client per day—true atomic events.

## Schema

### Identity & Join Contract

| Column | Purpose |
|--------|---------|
| `contact_id` | Primary join key (Dataverse/internal ID) - **authoritative when present** |
| `contact_email` | Secondary fallback join key - for backfill joins, foreign-system reconciliation, disaster recovery |

### Event Classification (Minimal, Non-Dumb)

| Column | Purpose | Examples |
|--------|---------|----------|
| `event_type` | Coarse channel bucket | `form`, `session`, `email`, `message`, `system` |
| `event_name` | Human-meaningful subtype | `PHQ-9`, `Weekly Check-in`, `Session Completed`, `Form Reminder Sent` |
| `event_source` | Domain-level source (human/business concept) | `google_form`, `dataverse`, `respond_io`, `outlook`, `life_cli` |

These three together define the semantic identity of the event.

**event_source vs source_system distinction:**
- `event_source` = domain-level, human-meaningful origin. "Where did this come from as a concept?" Used in queries and business logic.
- `source_system` = pipeline-level origin. "Which canonical table produced this row?" Used for idempotency and debugging.

### Time Model

| Column | Purpose |
|--------|---------|
| `event_timestamp` | When the event actually occurred (UTC) |
| `ingested_at` | (Optional, later) When pipeline observed it |

All timestamps normalized to UTC. No exceptions.

### Provenance & Idempotency

| Column | Purpose | Examples |
|--------|---------|----------|
| `source_system` | Pipeline-level origin table | `measurement_events`, `canonical.clinical_sessions`, `canonical.email_events` |
| `source_record_id` | Stable unique identifier from the source | `measurement_event_id`, `session_id`, `email_event_id` |
| `event_fingerprint` | (Optional) Hash for deep dedupe later | |

**source_record_id contract:**
- MUST be a stable, unique identifier in the canonical source that represents one logical event
- MUST NOT change across pipeline reruns for the same logical event
- Typically maps to: `measurement_event_id`, `session_id`, `idem_key`, or similar

**Idem rule:**
```
(source_system, source_record_id) is the idempotency key.
Any upsert operation will overwrite the existing row with matching (source_system, source_record_id).
```

### Payload Strategy

| Column | Purpose |
|--------|---------|
| `payload` | JSON blob, raw passthrough of interesting fields |

Used for: debugging, one-off analytics, forensics, temporary logic without schema evolution.

**Rule:** Automation logic must never depend on payload semantics long-term. If it becomes important → it graduates to a real column.

### Complete Column Set

```
contact_id
contact_email

event_type
event_name
event_source

event_timestamp

source_system
source_record_id

payload
```

## Write Semantics

### NOW (Phase 1)

- Full rebuild
- `CREATE OR REPLACE TABLE`
- UNION from:
  - `measurement_events` (forms)
  - `clinical_sessions` (sessions)
  - `emails` (when available)
  - `messages` (if applicable)

Idempotency achieved structurally by overwrite.

### SOON (Phase 2)

- Per-source append/upsert
- Idem key: `(source_system, source_record_id)`
- Overwrite on conflict, not skip

Aligns with: **"idem by overwrite, not skip"**

## What This Table Is Optimized For

- "Who needs a form right now?"
- "Who has gone quiet?"
- "How active is this client across channels?"
- "Did we actually send that reminder?"
- "What was the last human-relevant interaction?"

**It is the event substrate for automation.**

## What This Table Is NOT Responsible For

- Scoring
- Compliance state
- Engagement classification
- Escalation thresholds
- CRM lifecycle flags

All of that belongs in:
- Queries against this table
- Or optional `contact_activity_state` later (aggregation layer)

## Plan

### Step 1: Define Source Mappings

Map each source canonical to `contact_events` schema:

**measurement_events → contact_events:**
```
contact_id      ← (lookup from contacts by subject_id email)
contact_email   ← subject_id
event_type      ← 'form'
event_name      ← measure_id (e.g., 'phq9', 'gad7')
event_source    ← 'google_form' (or binding_id source)
event_timestamp ← timestamp
source_system   ← 'measurement_events'
source_record_id ← measurement_event_id
payload         ← JSON with measure_version, binding_id, form_id, etc.
```

**clinical_sessions → contact_events:**
```
contact_id      ← contact_id
contact_email   ← (lookup from contacts)
event_type      ← 'session'
event_name      ← status (e.g., 'Session Completed', 'Session Cancelled')
event_source    ← 'dataverse'
event_timestamp ← started_at if available, else ended_at, else created_at
source_system   ← 'canonical.clinical_sessions'
source_record_id ← session_id
payload         ← JSON with session_num, idem_key, etc.
```

**emails → contact_events:** (future)
```
contact_id      ← (lookup by email address)
contact_email   ← sender/recipient email
event_type      ← 'email'
event_name      ← direction + subject snippet
event_source    ← 'gmail' | 'outlook'
event_timestamp ← sent_at
source_system   ← 'canonical.email_events'
source_record_id ← email_event_id
payload         ← JSON with thread_id, labels, etc.
```

### Contact Resolution Rules

When resolving `contact_id` from an email address:

1. **Exact match required:** The email must match exactly one contact in `contacts` (case-insensitive).
2. **No match:** If `subject_id` / email does not match any contact, `contact_id` is NULL. The row is still inserted (orphan event). Logged as warning.
3. **Multiple matches:** If email matches multiple contacts, the mapping fails. Logged as error. Row is skipped until disambiguation is resolved upstream.
4. **contact_id in source:** If the source canonical already has `contact_id`, it MUST be used directly and MUST exist in `contacts`. Mismatch is an error.

### Step 2: Add SQL Projection

**Files to touch:**
- `lorchestra/sql/projections.py`

This will be a UNION ALL of mapped sources, starting with:
1. measurement_events (forms)
2. clinical_sessions

### Step 3: Create Job Definitions

1. `create_proj_contact_events.json` - create/replace the view/table
2. `sync_proj_contact_events.json` - sync to SQLite

### Step 4: Update Pipeline Scripts

- `scripts/create_projections.sh` - add creation job
- `scripts/daily_projection.sh` - add sync job

### Step 5: Usage Queries

**Who needs a form (no form in X days):**
```sql
-- SQLite
SELECT c.contact_id, c.email, c.first_name,
       MAX(e.event_timestamp) as last_form
FROM contacts c
LEFT JOIN contact_events e
  ON c.contact_id = e.contact_id
  AND e.event_type = 'form'
GROUP BY c.contact_id, c.email, c.first_name
HAVING last_form IS NULL
   OR last_form < DATETIME('now', '-14 days')
```

```sql
-- BigQuery
SELECT c.contact_id, c.email, c.first_name,
       MAX(e.event_timestamp) as last_form
FROM proj_clients c
LEFT JOIN proj_contact_events e
  ON c.client_id = e.contact_id
  AND e.event_type = 'form'
GROUP BY c.contact_id, c.email, c.first_name
HAVING last_form IS NULL
   OR last_form < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
```

**Client activity timeline:**
```sql
-- SQLite / BigQuery (dialect-neutral)
SELECT event_timestamp, event_type, event_name, event_source
FROM contact_events
WHERE contact_id = ?
ORDER BY event_timestamp DESC
```

**Channel activity summary:**
```sql
-- SQLite
SELECT
  contact_id,
  SUM(CASE WHEN event_type = 'form' THEN 1 ELSE 0 END) as form_count,
  SUM(CASE WHEN event_type = 'session' THEN 1 ELSE 0 END) as session_count,
  SUM(CASE WHEN event_type = 'email' THEN 1 ELSE 0 END) as email_count,
  MAX(event_timestamp) as last_activity
FROM contact_events
GROUP BY contact_id
```

```sql
-- BigQuery
SELECT
  contact_id,
  COUNTIF(event_type = 'form') as form_count,
  COUNTIF(event_type = 'session') as session_count,
  COUNTIF(event_type = 'email') as email_count,
  MAX(event_timestamp) as last_activity
FROM proj_contact_events
GROUP BY contact_id
```

## Testing

- Unit test for each source mapping transformation
- Verify UNION produces correct grain (no duplicates)
- Verify idem_key → source_record_id mapping is unique
- Sync to SQLite produces expected schema

## Models & Tools

**Tools:** bash, pytest, ruff, bq

## Repository

**Branch:** `feat/contact-events`

**Merge Strategy:** squash