---
version: "0.2"
tier: C
title: Contact Events Projection
owner: benthepsychologist
goal: Implement contact_events - unified operational event ledger for all client-touch events
labels: [projection, events, operational-ledger]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-09T19:37:43.907616+00:00
updated: 2025-12-12T14:00:00.000000+00:00
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

- [x] `proj_contact_events` BQ view created
- [x] measurement_events (forms) source included
- [x] clinical_sessions source included
- [ ] emails source included (gmail + exchange)
- [ ] invoices source included (stripe)
- [ ] payments source included (stripe)
- [ ] refunds source included (stripe)
- [ ] Therapy filter uses client_type_code, not label
- [x] Sync job copies to SQLite `contact_events` table
- [x] Daily pipeline includes rebuild
- [ ] CI green (lint + unit)
- [ ] **Validation check:** At least one client's full activity history can be reconstructed correctly from `proj_contact_events` and matches raw canonicals end-to-end

## Data Inventory

Current data available in `canonical_objects` for projection:

| Source | Object Type | Schema | Count | Status |
|--------|-------------|--------|-------|--------|
| measurement_events | form | (separate table) | ~235 | ✅ In projection |
| dataverse | session | clinical_session/2-0-0 | 832 | ✅ In projection |
| gmail | email | email_jmap_lite/1-0-0 | 6,085 | ⬚ Pending |
| exchange | email | email_jmap_lite/1-0-0 | 4,372 | ⬚ Pending |
| stripe | invoice | invoice/1-0-0 | 549 | ⬚ Pending |
| stripe | payment_intent | payment/1-0-0 | 6,712 | ⬚ Pending |
| stripe | refund | refund/1-0-0 | 55 | ⬚ Pending |

**Not included (identity, not events):**
- dataverse/contact (642) - identity table, used for lookups via `proj_clients`
- stripe/customer (296) - identity table, used for lookups only

## Identity Surface: proj_clients

Contact resolution uses `proj_clients` (already exists in `projections.py`):

```sql
-- proj_clients provides identity for lookups
SELECT client_id, email, first_name, last_name, client_type, client_type_code
FROM proj_clients
```

**Important:** `proj_clients` currently filters on `client_type_label = 'Therapy'`. This MUST be changed to use `client_type_code`.

### Local SQLite Mirror

`proj_clients` MUST sync to local SQLite as `clients` table:

```json
{
  "job_id": "sync_proj_clients",
  "job_type": "sync_sqlite",
  "source": { "projection": "proj_clients" },
  "sink": {
    "sqlite_path": "~/clinical-vault/local.db",
    "table": "clients"
  }
}
```

This ensures local scripts (like `send_forms.sh`) can query clients without hitting BigQuery.

### Therapy Client Filtering

**Problem:** Labels are mutable/localized. `client_type_label = 'Therapy'` will break.

**Solution:** Filter by `client_type_code` which is stable:

```
Current client_type_code values:
  '1'     = Therapy (111 contacts)
  '2'     = Coaching (2)
  '3'     = Consulting (3)
  '1,3,6' = Multi-type including Therapy (1)
  NULL    = No type (525)
```

**Filter rule (deterministic, no substring):**
```sql
-- Handles single value and comma-separated multi-value
WHERE '1' IN UNNEST(SPLIT(JSON_VALUE(payload, '$.client_type_code'), ','))
```

This correctly matches `'1'` and `'1,3,6'` without false positives from substring matching.

**SQLite equivalent (for local queries):**
```sql
WHERE (',' || client_type_code || ',') LIKE '%,1,%'
   OR client_type_code = '1'
```

## Context

### Relationship to contacts

| Table | Purpose |
|-------|---------|
| `proj_clients` | Identity + governance (used for lookups) |
| `proj_contact_events` | Temporal reality (the activity ledger) |

**They must never collapse into one another.**

### Grain

**One row = one client-relevant event**

Not one per form type, not one per client per day—true atomic events.

**Critical:** Forms source from `measurement_events` (submission-grain), NOT from `observations`. Observations are results, not events.

## Schema

### Identity & Join Contract

| Column | Purpose |
|--------|---------|
| `contact_id` | Primary join key (Dataverse/internal ID) - **authoritative when present** |
| `contact_email` | Secondary fallback join key - for backfill joins, foreign-system reconciliation, disaster recovery |

### Event Classification

| Column | Purpose | Examples |
|--------|---------|----------|
| `event_type` | Coarse channel bucket | `form`, `session`, `email`, `invoice`, `payment`, `refund` |
| `event_name` | Human-meaningful subtype | `intake_01`, `followup`, `Scheduled`, `open`, `succeeded` |
| `event_source` | Domain-level source | `google_forms`, `dataverse`, `gmail`, `exchange`, `stripe` |

These three together define the semantic identity of the event.

**event_source vs source_system distinction:**
- `event_source` = domain-level, human-meaningful origin. Used in queries and business logic.
- `source_system` = pipeline-level origin. Used for idempotency and debugging.

### Time Model

| Column | Purpose |
|--------|---------|
| `event_timestamp` | When the event actually occurred (UTC) |

All timestamps normalized to UTC. No exceptions.

### Provenance & Idempotency

| Column | Purpose |
|--------|---------|
| `source_system` | Stable enum identifying the pipeline source |
| `source_record_id` | Stable unique identifier from that source |

**source_system enum (stable forever):**
```
measurement_events    -- form submissions
dataverse_sessions    -- clinical sessions
email_gmail          -- gmail emails
email_exchange       -- exchange/outlook emails
stripe_invoice       -- stripe invoices
stripe_payment       -- stripe payments
stripe_refund        -- stripe refunds
```

These values are part of the idem key and MUST NOT change.

**Idem rule:**
```
(source_system, source_record_id) is the idempotency key.
```

### Optional Columns (add if needed)

| Column | Purpose | When to add |
|--------|---------|-------------|
| `event_id` | `SHA256(source_system + ':' + source_record_id)` | If you need stable synthetic IDs |
| `event_direction` | `inbound` / `outbound` (for emails) | If direction queries become common |

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

### NOW (Phase 1) - View + Sync

- `proj_contact_events` is a **VIEW** (not a table)
- `CREATE OR REPLACE VIEW` in `projections.py`
- `create_proj_contact_events` job runs daily (idempotent view rebuild)
- `sync_proj_contact_events` job runs daily (full replace to SQLite)

UNION from all available sources:
- `measurement_events` (forms) ✅ implemented
- `dataverse_sessions` (sessions) ✅ implemented
- `email_gmail` ⬚ pending
- `email_exchange` ⬚ pending
- `stripe_invoice` ⬚ pending
- `stripe_payment` ⬚ pending
- `stripe_refund` ⬚ pending

Idempotency achieved structurally by view rebuild + full replace sync.

### LATER (Phase 2) - Materialized Table

When performance requires it, add materialization:

```json
{
  "job_id": "materialize_contact_events",
  "job_type": "materialize_projection",
  "source": {
    "view": "proj_contact_events"
  },
  "sink": {
    "table": "mat_contact_events"
  }
}
```

Implementation: `CREATE OR REPLACE TABLE mat_contact_events AS SELECT * FROM proj_contact_events`

This is a **future stub** - not wired yet. The view approach is sufficient for current scale (~20k events).

## What This Table Is Optimized For

- "Who needs a form right now?"
- "Who has gone quiet?"
- "How active is this client across channels?"
- "Did we actually send that reminder?"
- "What was the last human-relevant interaction?"

**It is the event substrate for automation.**

## What This Table Is NOT Responsible For

- Scoring (that's `observations`)
- Compliance state
- Engagement classification
- Escalation thresholds
- CRM lifecycle flags

All of that belongs in queries against this table or optional aggregation layers.

## Source Mappings

### measurement_events → contact_events (forms)

**Source:** `measurement_events` table (submission-grain, NOT observations)

```
contact_id       ← proj_clients.client_id (join on LOWER(subject_id) = LOWER(email))
contact_email    ← subject_id
event_type       ← 'form'
event_name       ← event_subtype (binding_id: intake_01, followup, etc.)
event_source     ← 'google_forms'
event_timestamp  ← occurred_at
source_system    ← 'measurement_events'
source_record_id ← measurement_event_id
payload          ← JSON: {binding_id, binding_version, form_id, canonical_object_id}
```

**Critical:** `event_name` is `binding_id` (e.g., "followup"), NOT `measure_id` (e.g., "phq9"). One submission = one event. Measures go in payload if needed.

### dataverse_sessions → contact_events (sessions)

**Source:** `canonical_objects` where schema = `clinical_session/2-0-0`

```
contact_id       ← session.contact_id (direct, authoritative)
contact_email    ← proj_clients.email (lookup by contact_id)
event_type       ← 'session'
event_name       ← status (Scheduled, Completed, Cancelled, etc.)
event_source     ← 'dataverse'
event_timestamp  ← COALESCE(started_at, ended_at)
source_system    ← 'dataverse_sessions'
source_record_id ← session_id  -- THE idempotency key (stable business identifier)
payload          ← JSON: {session_num}
```

**Note:** `session_id` is the stable business identifier from Dataverse. Use it directly as `source_record_id`, not `idem_key` (which is a pipeline artifact).

### email_gmail / email_exchange → contact_events (emails)

**Source:** `canonical_objects` where schema = `email_jmap_lite/1-0-0`

```
contact_id       ← proj_clients.client_id (join on contact_email)
contact_email    ← (see direction logic)
event_type       ← 'email'
event_name       ← direction + ': ' + SUBSTR(subject, 1, 50)  -- e.g., "outbound: Weekly check-in"
event_source     ← source_system ('gmail' or 'exchange')
event_timestamp  ← TIMESTAMP(sentAt)
source_system    ← 'email_gmail' or 'email_exchange'
source_record_id ← JSON_VALUE(payload, '$.id')
payload          ← JSON: {subject, threadId}
```

**Direction encoded in event_name** (not payload) for queryability:
- `event_name LIKE 'outbound:%'` → emails sent to client
- `event_name LIKE 'inbound:%'` → emails received from client

**Direction logic:**

My email addresses (canonical set):
```sql
-- Hardcoded for now, move to config later
('ben@mensiomentalhealth.com', 'ben@benthepsychologist.com', 'benthepsychologist@gmail.com')
```

```sql
CASE
  WHEN LOWER(JSON_VALUE(payload, '$.from[0].email')) IN (MY_EMAILS) THEN 'outbound'
  ELSE 'inbound'
END AS direction
```

**Contact email resolution:**
- outbound → `to[0].email` (first recipient)
- inbound → `from[0].email` (sender)
- If contact_email doesn't match any `proj_clients.email` → `contact_id = NULL`, keep as orphan event

### stripe_invoice → contact_events (invoices)

**Source:** `canonical_objects` where schema = `invoice/1-0-0`

```
contact_id       ← proj_clients.client_id (join on LOWER(customer_email))
contact_email    ← customer_email
event_type       ← 'invoice'
event_name       ← status (open, paid, void, uncollectible)
event_source     ← 'stripe'
event_timestamp  ← TIMESTAMP(event_time)
source_system    ← 'stripe_invoice'
source_record_id ← invoice_id
payload          ← JSON: {invoice_number, amount_due, currency, description}
```

### stripe_payment → contact_events (payments)

**Source:** `canonical_objects` where schema = `payment/1-0-0`

```
contact_id       ← (lookup via customer_id → stripe_customer → email → proj_clients)
contact_email    ← (from customer lookup, may be NULL)
event_type       ← 'payment'
event_name       ← status (succeeded, failed, canceled)
event_source     ← 'stripe'
event_timestamp  ← TIMESTAMP(event_time)
source_system    ← 'stripe_payment'
source_record_id ← payment_id
payload          ← JSON: {amount, currency, payment_method_type}
```

### stripe_refund → contact_events (refunds)

**Source:** `canonical_objects` where schema = `refund/1-0-0`

```
contact_id       ← (lookup via associated payment/invoice)
contact_email    ← (from associated record)
event_type       ← 'refund'
event_name       ← status (succeeded, failed, pending)
event_source     ← 'stripe'
event_timestamp  ← TIMESTAMP(event_time)
source_system    ← 'stripe_refund'
source_record_id ← refund_id
payload          ← JSON: {amount, currency, reason}
```

## Contact Resolution Rules

1. **Direct contact_id:** If source has `contact_id` (e.g., sessions), use it directly.
2. **Email lookup:** Join to `proj_clients` on email (case-insensitive).
3. **No match:** `contact_id = NULL`. Event is still inserted (orphan). Queryable for diagnostics.
4. **Ambiguous:** If logic can't determine contact, set `contact_id = NULL`. Don't guess.

## Implementation Plan

### Step 1: Fix proj_clients therapy filter

Change from:
```sql
WHERE ... AND JSON_VALUE(payload, '$.client_type_label') = 'Therapy'
```

To:
```sql
WHERE ... AND '1' IN UNNEST(SPLIT(JSON_VALUE(payload, '$.client_type_code'), ','))
```

### Step 2: Update proj_contact_events SQL

In `lorchestra/sql/projections.py`, update `PROJ_CONTACT_EVENTS`:

1. Fix therapy filter (use `client_type_code`)
2. Fix `source_system` values (use stable enums)
3. Add email source (UNION ALL)
4. Add stripe sources (UNION ALL)

### Step 3: Test view creation

```bash
lorchestra run view_proj_contact_events --dry-run
lorchestra run view_proj_contact_events
```

### Step 4: Verify sync

```bash
lorchestra run sync_proj_contact_events
sqlite3 ~/clinical-vault/local.db "SELECT event_type, COUNT(*) FROM contact_events GROUP BY 1"
```

## Usage Queries

**Who needs a form (no form in X days):**
```sql
-- SQLite
SELECT c.client_id, c.email, c.first_name,
       MAX(e.event_timestamp) as last_form
FROM clients c
LEFT JOIN contact_events e
  ON c.client_id = e.contact_id
  AND e.event_type = 'form'
WHERE ((',' || c.client_type_code || ',') LIKE '%,1,%' OR c.client_type_code = '1')
GROUP BY c.client_id, c.email, c.first_name
HAVING last_form IS NULL
   OR last_form < DATETIME('now', '-14 days')
```

**Who was emailed recently (for idempotent form reminders):**
```sql
-- SQLite - clients NOT emailed in last 2 weeks
SELECT c.client_id, c.first_name, c.last_name, c.email
FROM clients c
WHERE ((',' || c.client_type_code || ',') LIKE '%,1,%' OR c.client_type_code = '1')
  AND c.email IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM contact_events e
    WHERE LOWER(e.contact_email) = LOWER(c.email)
      AND e.event_type = 'email'
      AND e.event_name LIKE 'outbound:%'
      AND e.event_timestamp > DATETIME('now', '-14 days')
  )
```

**Client activity timeline:**
```sql
SELECT event_timestamp, event_type, event_name, event_source
FROM contact_events
WHERE contact_id = ?
ORDER BY event_timestamp DESC
```

## Testing

- Unit test for each source mapping transformation
- Verify UNION produces correct grain (no duplicates)
- Verify source_system values match enum
- Sync to SQLite produces expected schema
- Spot-check: pick one client, verify their timeline matches raw sources

## Repository

**Branch:** `feat/contact-events`
**Merge Strategy:** squash