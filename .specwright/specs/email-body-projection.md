---
version: "0.1"
tier: C
title: Email Body in Contact Events Projection
owner: benthepsychologist
goal: Add email body text to contact_events projection for idempotent notification checks
labels: [projection, email, idempotency]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-15T21:00:00.000000+00:00
updated: 2025-12-15T21:00:00.000000+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/email-body-projection"
---

# Email Body in Contact Events Projection

## Objective

> Update the `proj_contact_events` projection to include email body text from canonical objects, enabling idempotent notification checks ("has this client been contacted about X in the last N days?").

## Acceptance Criteria

- [ ] `PROJ_CONTACT_EVENTS` SQL includes `body_text` (derived from canonical `body.text`) in the payload JSON for email sources
- [ ] SQLite `contact_events` table schema includes body text (via payload JSON)
- [ ] Body text is truncated to reasonable size (first 4000 chars) to avoid SQLite bloat
- [ ] Gmail and Exchange email sources both include body
- [ ] Existing functionality unchanged (event_type, event_name, timestamps all preserved)
- [ ] CI green (ruff + pytest)

## Context

### Background

The current `proj_contact_events` projection extracts emails but only includes:
- `subject` - email subject line
- `thread_id` - for threading

The canonical `email_jmap_lite` schema contains full email body:
- `body.text` - plain text body
- `body.html` - HTML body

For notification idempotency, we need to check if a client has already been sent a specific type of message (e.g., questionnaire reminder) within a time window. Subject line alone is insufficient - we need body text to match content patterns.

### Why body.text not body.html

- Text is smaller (no markup bloat)
- Pattern matching is simpler
- HTML can be reconstructed from text if needed
- SQLite storage is cheaper

### Size constraint

Email bodies can be large (10KB+). To prevent SQLite bloat:
- Truncate to first 4000 characters
- This is enough for pattern matching ("Please complete your questionnaire")
- Full body remains in BigQuery canonical_objects if needed

## Non-Goals

- **Not** adding body to file projections (markdown files) - would bloat clinical-vault
- **Not** adding HTML body - text is sufficient
- **Not** changing canonical schema - body already exists
- **Not** building notification dedup logic - that's a separate spec

## Governance

This change implements the `projection-pipeline` pattern and adheres to:
- `projection-safety@0.1.0` - read-only, idempotent
- `incremental-sync@0.1.0` - watermark-based sync
- `phi-handling@0.1.0` - email body is PHI, stays local

## Plan

### Step 1: Update PROJ_CONTACT_EVENTS SQL

**File:** `lorchestra/lorchestra/sql/projections.py`

**Change:** Modify the Gmail and Exchange email source SELECTs to include truncated body text in the payload JSON.

For Gmail emails (Source 3: `email_gmail`), extract body text from the canonical payload in the inner SELECT:
```sql
JSON_VALUE(payload, '$.body.text') AS body_text,
```

Then include it in the outer payload JSON (truncated to 4000 chars):
```sql
TO_JSON_STRING(STRUCT(
  e.subject,
  e.thread_id,
  SUBSTR(COALESCE(e.body_text, ''), 1, 4000) AS body_text
)) AS payload
```

Repeat the same change for Exchange emails (Source 4: `email_exchange`).

**Constraints:**
- Only modify email source SELECTs (Gmail and Exchange)
- Do not modify form_responses or dataverse_sessions sources
- Preserve existing column order and structure

### Step 2: Recreate BQ Views

**Command:**
```bash
lorchestra run view_proj_contact_events
```

This will execute the updated SQL to recreate the `proj_contact_events` view in BigQuery.

**Note:** `create_projection` creates views in the configured `dataset_canonical`.

### Step 3: Sync to SQLite

**Command:**
```bash
lorchestra run sync_proj_contact_events
```

This will sync the updated projection to local SQLite. The payload column is JSON text, so no schema migration is needed - the new `body_text` field is simply part of the JSON.

**Note:** `sync_sqlite` reads from `dataset_canonical` unless the job definition sets `source.dataset` (e.g., `"derived"`).

### Step 4: Verify body_text in BQ and SQLite

**Verification commands:**
```bash
# Check BQ view has body_text in payload (uses config placeholders)
lorchestra sql "SELECT source_record_id, JSON_VALUE(payload, '$.body_text') AS body_text FROM \`${PROJECT}.${DATASET_CANONICAL}.proj_contact_events\` WHERE event_type = 'email' LIMIT 5"

# Check SQLite has body_text in payload
sqlite3 ~/clinical-vault/local.db "SELECT source_record_id, json_extract(payload, '$.body_text') AS body_text FROM contact_events WHERE event_type = 'email' LIMIT 5"
```

## Allowed Paths

```yaml
allowed_paths:
  - lorchestra/lorchestra/sql/projections.py
```

## Forbidden Paths

```yaml
forbidden_paths:
  - lorchestra/processors/  # No processor changes needed
  - lorchestra/stack_clients/  # No client changes needed

  # Note: this spec is Markdown; do not blanket-forbid "*.md".
  # The intent is to avoid changes to markdown *file projections* (clinical-vault outputs), not specs/docs.
```

## Rollback

If issues arise:
1. Revert `projections.py` change
2. Re-run `lorchestra run view_proj_contact_events`
3. Re-run `lorchestra run sync_proj_contact_events`

The change is fully reversible with no data loss.
