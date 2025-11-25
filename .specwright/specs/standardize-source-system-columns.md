---
version: "0.1"
tier: B
title: Standardize Source System Columns
owner: benthepsychologist
goal: Add connection_name column to raw_objects and normalize source_system to provider-only values
labels: [schema, migration, refactor]
orchestrator_contract: "standard"
repo:
  working_branch: "feat/standardize-source-columns"
---

# Standardize Source System Columns

## Objective

> Separate source_system into three distinct columns following industry standards (Airbyte, Singer/Meltano): `source_system` (provider), `connection_name` (account), `object_type` (stream)

### Background

Currently, `source_system` in `raw_objects` conflates two concepts:
- The upstream product/API family (gmail, exchange, dataverse, google_forms)
- The specific configured connection/account (acct1, ben-mensio, clinic)

Examples of current values:
- `tap-gmail--acct1`
- `exchange--ben-mensio`
- `dataverse--clinic-contacts`
- `google-forms--intake-01`

This makes queries awkward:
- To find all Gmail: `WHERE source_system LIKE 'tap-gmail%'` or `LIKE 'gmail%'`
- To find all emails: `WHERE object_type = 'email'` works, but joining across providers is clunky

Industry standard (Airbyte, Singer/Meltano) uses three identifiers:
1. **source_system**: upstream product/API family (`gmail`, `exchange`, `dataverse`, `stripe`)
2. **connection_name**: specific connection/account (`gmail-acct1`, `exchange-ben-mensio`)
3. **object_type**: type/stream of object (`email`, `contact`, `form_response`)

### Acceptance Criteria

- `raw_objects` has new `connection_name` STRING column
- Existing data migrated: `connection_name = current source_system`, `source_system = normalized provider`
- All ingestion jobs updated to set all three columns
- `event_log` updated with same pattern (if it has source_system)
- Queries work:
  - `WHERE source_system = 'gmail' AND object_type = 'email'` (all Gmail emails)
  - `WHERE connection_name = 'gmail-acct1'` (specific account)
  - `WHERE object_type = 'email' AND source_system IN ('gmail', 'exchange')` (all emails)
- Test tables (`test_raw_objects`, `test_event_log`) also updated

### Constraints

- Must be backwards compatible during migration
- No data loss
- Minimal downtime (single ALTER + UPDATE)

## Plan

### Step 1: Schema Migration Design [G0: Design Approval]

**Prompt:**

Design the BigQuery schema migration:
1. Document current schema for `raw_objects` and `event_log`
2. Design new schema with `connection_name` column
3. Write migration SQL:
   - ALTER TABLE to add `connection_name` column
   - UPDATE to populate `connection_name` from current `source_system`
   - UPDATE to normalize `source_system` to provider-only values
4. Document mapping rules:
   - `tap-gmail--acct1` → `source_system='gmail'`, `connection_name='gmail-acct1'`
   - `exchange--ben-mensio` → `source_system='exchange'`, `connection_name='exchange-ben-mensio'`
   - `dataverse--clinic-contacts` → `source_system='dataverse'`, `connection_name='dataverse-clinic-contacts'`
   - `google-forms--intake-01` → `source_system='google_forms'`, `connection_name='google-forms-intake-01'`

**Outputs:**
- `artifacts/migration/schema-design.md`
- `artifacts/migration/migration.sql`

---

### Step 2: Update Ingestion Jobs [G1: Code Review]

**Prompt:**

Update all ingestion job modules to use the new three-column pattern:

1. `lorchestra/jobs/ingest_gmail.py`:
   - Change `source_system` from `"tap-gmail--acct1"` to `"gmail"`
   - Add `connection_name="gmail-acct1"` parameter to `upsert_objects()`

2. `lorchestra/jobs/ingest_exchange.py`:
   - Change `source_system` from `"exchange--ben-mensio"` to `"exchange"`
   - Add `connection_name="exchange-ben-mensio"` etc.

3. `lorchestra/jobs/ingest_dataverse.py`:
   - Change `source_system` from `"dataverse--clinic-contacts"` to `"dataverse"`
   - Add `connection_name="dataverse-clinic-contacts"` etc.

4. `lorchestra/jobs/ingest_google_forms.py`:
   - Change `source_system` from `"google-forms--ipip120"` to `"google_forms"`
   - Add `connection_name="google-forms-ipip120"` etc.

5. Update `_get_last_sync_timestamp()` queries in each module to use both `source_system` AND `connection_name`

**Outputs:**
- `lorchestra/jobs/ingest_gmail.py` (updated)
- `lorchestra/jobs/ingest_exchange.py` (updated)
- `lorchestra/jobs/ingest_dataverse.py` (updated)
- `lorchestra/jobs/ingest_google_forms.py` (updated)

---

### Step 3: Update event_client.py [G1: Code Review]

**Prompt:**

Update the event_client module:

1. Add `connection_name` parameter to `upsert_objects()` function signature
2. Add `connection_name` to the temp table schema and MERGE query
3. Add `connection_name` parameter to `log_event()` function signature
4. Update both functions to require `connection_name` (not optional)
5. Update docstrings and examples

**Outputs:**
- `lorchestra/stack_clients/event_client.py` (updated)

---

### Step 4: Run BigQuery Migration [G2: Pre-Release]

**Prompt:**

Execute the schema migration on BigQuery:

1. Run migration on test tables first (`test_raw_objects`, `test_event_log`)
2. Verify data integrity on test tables
3. Run migration on production tables (`raw_objects`, `event_log`)
4. Verify data integrity on production tables
5. Document row counts before/after

**Commands:**

```bash
# Run migration SQL via bq command or Python
python scripts/run_migration.py --target test
python scripts/run_migration.py --target prod
```

**Outputs:**
- Migration execution log
- Before/after row counts

---

### Step 5: Validation & Testing [G3: Pre-Release]

**Prompt:**

Validate the migration and test ingestion:

1. Run `--dry-run` for each ingestion job type
2. Run `--test-table` for one job of each type
3. Verify new records have correct three-column values
4. Verify queries work as expected:
   - `SELECT * FROM raw_objects WHERE source_system = 'gmail'`
   - `SELECT * FROM raw_objects WHERE connection_name = 'gmail-acct1'`
   - `SELECT * FROM raw_objects WHERE object_type = 'email' AND source_system IN ('gmail', 'exchange')`

**Commands:**

```bash
lorchestra run gmail_ingest_acct1 --dry-run
lorchestra run exchange_ingest_ben_mensio --dry-run
lorchestra run dataverse_ingest_contacts --dry-run
lorchestra run google_forms_ingest_intake_01 --dry-run

lorchestra run gmail_ingest_acct1 --test-table
```

**Outputs:**
- Test results log
- Query validation results

---

### Step 6: Update Documentation [G3: Pre-Release]

**Prompt:**

Update documentation to reflect new schema:

1. Update `ARCHITECTURE.md` - raw_objects schema section
2. Update any BQ setup docs in `setup/`
3. Add migration notes

**Outputs:**
- `ARCHITECTURE.md` (updated)
- `setup/bigquery-setup.md` (updated if exists)

## Orchestrator

**State Machine:** Standard (pending → running → awaiting_human → succeeded/failed)

**Tools:** bash, bq, python, git

## Repository

**Branch:** `feat/standardize-source-columns`

**Merge Strategy:** squash
