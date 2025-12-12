---
version: "0.1"
tier: C
title: Canonical Data Cleanup
owner: benthepsychologist
goal: Remove stale/duplicate records from canonical_objects to fix data quality issues
labels: [data-quality, cleanup, canonical]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-12T16:31:35.667745+00:00
updated: 2025-12-12T16:31:35.667745+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "chore/canonical-cleanup"
---

# Canonical Data Cleanup

## Objective

> Remove stale/duplicate records from canonical_objects to fix data quality issues identified during proj_contact_events implementation.

## Background

During the contact_events projection implementation, several data quality issues were discovered:

### Issue 1: Duplicate Stripe Connections
- `stripe-prod` and `stripe-mensio` are pulling from the **same Stripe account**
- 2,653 payments appear in both connections (nearly complete overlap)
- This doubles the canonical record count unnecessarily

### Issue 2: Legacy idem_key Suffixes
- Old canonization jobs created records with incorrect suffixes:
  - `#payment_intent` instead of `#payment` (5,312 stale records)
  - `#email_jmap_lite` instead of `#email` (unknown count)
- New canonization creates correct suffixes, but old records remain

### Issue 3: Duplicate Contact Records (informational)
- 1 email has 2 contact_ids in Dataverse source data
- This is a source system issue, not a cleanup task here

## Acceptance Criteria

- [ ] Delete stale `#payment_intent` suffix records from canonical_objects
- [ ] Delete stale `#email_jmap_lite` suffix records from canonical_objects
- [ ] Delete stale `#email` suffix records (legacy) from canonical_objects
- [ ] Remove or deprecate `stripe-mensio` ingest jobs
- [ ] Verify no duplicate source_record_ids in proj_contact_events after cleanup
- [ ] CI green (lint + unit)

## Data Inventory

### Stale Records to Delete

```sql
-- Payment records with wrong suffix (created 2025-12-03)
SELECT COUNT(*) FROM canonical_objects
WHERE canonical_schema = 'iglu:org.canonical/payment/jsonschema/1-0-0'
  AND idem_key LIKE '%#payment_intent'
-- Result: 5,312 records

-- Email records with wrong suffix
SELECT COUNT(*) FROM canonical_objects
WHERE canonical_schema = 'iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0'
  AND (idem_key LIKE '%#email_jmap_lite' OR idem_key LIKE '%#email')
-- Check actual count before deletion
```

### Duplicate Connection Records

```sql
-- stripe-mensio records (subset of stripe-prod)
SELECT object_type, COUNT(*) FROM raw_objects
WHERE connection_name = 'stripe-mensio'
GROUP BY object_type
-- customer: 148, invoice: 273, payment_intent: 2653, refund: 27
```

## Plan

### Step 1: Audit Current State

**Prompt:** Query and document exact record counts for each cleanup category.

**Commands:**
```bash
source .env
bq query --use_legacy_sql=false "
SELECT
  CASE
    WHEN idem_key LIKE '%#payment_intent' THEN 'payment#payment_intent'
    WHEN idem_key LIKE '%#payment' THEN 'payment#payment'
    ELSE 'other'
  END as category,
  COUNT(*) as cnt
FROM \`\${GCP_PROJECT}.\${EVENTS_BQ_DATASET}.canonical_objects\`
WHERE canonical_schema = 'iglu:org.canonical/payment/jsonschema/1-0-0'
GROUP BY 1
"
```

**Outputs:**
- Record counts before cleanup

### Step 2: Delete Stale Payment Records

**Prompt:** Delete canonical_objects records with `#payment_intent` suffix.

**Commands:**
```bash
source .env
bq query --use_legacy_sql=false "
DELETE FROM \`\${GCP_PROJECT}.\${EVENTS_BQ_DATASET}.canonical_objects\`
WHERE canonical_schema = 'iglu:org.canonical/payment/jsonschema/1-0-0'
  AND idem_key LIKE '%#payment_intent'
"
```

### Step 3: Delete Stale Email Records

**Prompt:** Delete canonical_objects records with legacy email suffixes.

**Commands:**
```bash
source .env
bq query --use_legacy_sql=false "
DELETE FROM \`\${GCP_PROJECT}.\${EVENTS_BQ_DATASET}.canonical_objects\`
WHERE canonical_schema = 'iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0'
  AND (idem_key LIKE '%#email_jmap_lite' OR idem_key LIKE '%#email')
"
```

### Step 4: Remove stripe-mensio Connection

**Prompt:** Delete raw_objects and canonical_objects for stripe-mensio connection, then remove/deprecate ingest jobs.

**Commands:**
```bash
source .env
# Delete canonical records
bq query --use_legacy_sql=false "
DELETE FROM \`\${GCP_PROJECT}.\${EVENTS_BQ_DATASET}.canonical_objects\`
WHERE connection_name = 'stripe-mensio'
"
# Delete raw records
bq query --use_legacy_sql=false "
DELETE FROM \`\${GCP_PROJECT}.\${EVENTS_BQ_DATASET}.raw_objects\`
WHERE connection_name = 'stripe-mensio'
"
```

**Files to modify:**
- Remove or rename stripe-mensio job definitions (if any exist)

### Step 5: Verify and Rebuild

**Prompt:** Rebuild proj_contact_events view and verify no duplicates remain.

**Commands:**
```bash
source .env
lorchestra run view_proj_contact_events
lorchestra run sync_proj_contact_events

# Verify no duplicates
bq query --use_legacy_sql=false "
SELECT source_system, source_record_id, COUNT(*) as cnt
FROM \`\${GCP_PROJECT}.\${EVENTS_BQ_DATASET}.proj_contact_events\`
GROUP BY 1, 2
HAVING COUNT(*) > 1
"
```

### Step 6: Remove Dedup Logic from Projection (optional)

**Prompt:** Once source data is clean, simplify the projection SQL by removing ROW_NUMBER() dedup logic.

**Files to modify:**
- `lorchestra/sql/projections.py` - simplify PROJ_CONTACT_EVENTS

## Notes

- The projection currently handles duplicates via ROW_NUMBER() dedup
- After cleanup, this logic can be simplified but is not strictly required
- Keep dedup logic as defensive measure against future data issues

## Repository

**Branch:** `chore/canonical-cleanup`
**Merge Strategy:** squash
