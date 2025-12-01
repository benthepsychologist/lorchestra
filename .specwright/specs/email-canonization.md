---
version: "0.1"
tier: C
title: Email Canonization Pipeline
owner: benthepsychologist
goal: Validate and canonize raw Gmail and Exchange emails to JMAP Lite format via lorchestra jobs with validation stamps and new canonical_objects table
labels: [canonization, email, jmap, bigquery, validation]
project_slug: lorchestra
spec_version: 1.2.0
created: 2025-11-26T00:00:00+00:00
updated: 2025-11-26T02:00:00+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/email-canonization"
status: draft
---

# Email Canonization Pipeline

## Objective

> Validate raw Gmail and Exchange email payloads against source schemas, then transform to canonical
> JMAP Lite format. Validation and canonization are separate jobs. All schemas (source AND canonical)
> are validated by canonizer.

## Responsibility Model

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              RESPONSIBILITY SPLIT                            │
├─────────────────────────────────────────────────────────────────────────────┤
│  InJest                                                                      │
│  ├── Talks to external APIs (Gmail, Exchange, Stripe, Dataverse, etc.)      │
│  ├── Yields structured JSON objects                                          │
│  ├── Knows: auth, adapters, pagination                                       │
│  └── Does NOT know: BQ, schemas, transforms, validation                      │
│                                                                              │
│  (FUTURE: InJest will call canonizer.validate() on extraction,              │
│   rejecting invalid payloads before they reach BQ. Not in this spec.)       │
├─────────────────────────────────────────────────────────────────────────────┤
│  Canonizer                                                                   │
│  ├── Pure library + registry                                                 │
│  ├── TWO CORE FUNCTIONS:                                                     │
│  │   ├── validate(payload, schema) → validates ANY schema (source OR canon) │
│  │   └── canonicalize(payload, transform) → transforms source → canonical   │
│  │       └── (internally validates input against source schema first)       │
│  │       └── (internally validates output against canonical schema after)   │
│  ├── Knows: schemas (source + canonical), transforms, iglu refs             │
│  └── Does NOT know: BQ, jobs, events, orchestration                          │
│                                                                              │
│  Schema Registry:                                                            │
│  ├── Source schemas: Gmail API v1, Microsoft Graph v1.0, Stripe API, etc.   │
│  └── Canonical schemas: JMAP Lite Email, Canonical Contact, etc.            │
├─────────────────────────────────────────────────────────────────────────────┤
│  Lorchestra                                                                  │
│  ├── Orchestrator                                                            │
│  ├── Knows:                                                                  │
│  │   ├── How to pull rows from raw_objects                                  │
│  │   ├── How to call canonizer.validate() for source validation             │
│  │   ├── How to call canonizer.canonicalize() for transformation            │
│  │   ├── How to write to raw_objects (validation stamps)                    │
│  │   ├── How to write to canonical_objects                                  │
│  │   └── How to emit events via event_client                                │
│  └── Jobs:                                                                   │
│      ├── INGESTION: ingest_gmail_acct1, ingest_exchange_ben_mensio, ...    │
│      ├── VALIDATION: validate_gmail_source, validate_exchange_source        │
│      ├── CANONIZATION: canonize_gmail_jmap, canonize_exchange_jmap          │
│      └── (future) validate_stripe_source, canonize_stripe_*, etc.           │
└─────────────────────────────────────────────────────────────────────────────┘

Data Flow (Current - This Spec):
┌──────────┐    ┌─────────────┐    ┌──────────────────┐    ┌───────────────────┐
│  InJest  │───►│ raw_objects │───►│ validate_*_source│───►│ canonize_*_jmap   │
│          │    │ (unstamped) │    │ (stamps rows)    │    │ (transforms)      │
└──────────┘    └─────────────┘    └──────────────────┘    └───────────────────┘
                                           │                        │
                                           ▼                        ▼
                                   raw_objects.validation_stamp    canonical_objects

Data Flow (Future - After This Spec):
┌──────────┐    ┌────────────────────┐    ┌─────────────┐    ┌───────────────────┐
│  InJest  │───►│ canonizer.validate │───►│ raw_objects │───►│ canonize_*_jmap   │
│          │    │ (inline validation)│    │ (pre-stamped│    │ (transforms)      │
└──────────┘    └────────────────────┘    └─────────────┘    └───────────────────┘
                        │                                            │
                        ▼                                            ▼
                  reject invalid                              canonical_objects
```

## Acceptance Criteria

- [ ] `canonical_objects` BigQuery table created with proper schema
- [ ] `raw_objects` updated with `schema_source`, `schema_iglu`, `validation_stamp` columns
- [ ] Canonizer configured with explicit `CANONIZER_REGISTRY_ROOT` env var
- [ ] `lorchestra run validate_gmail_source` validates Gmail emails and stamps them
- [ ] `lorchestra run validate_exchange_source` validates Exchange emails and stamps them
- [ ] `lorchestra run canonize_gmail_jmap` transforms validated Gmail emails to JMAP Lite
- [ ] `lorchestra run canonize_exchange_jmap` transforms validated Exchange emails to JMAP Lite
- [ ] `canonize_*` jobs FAIL if source payload doesn't pass validation (canonizer enforces this)
- [ ] Validation failures emit `validation.failed` events with error details
- [ ] Transform failures emit `canonize.failed` events with error details
- [ ] `--dry-run` mode works (no BQ writes)
- [ ] `--test-table` mode works (writes to test tables)
- [ ] All 7,654 existing emails validated and canonized

## Context

### Current State

**Raw Objects in BigQuery** (`events_dev.raw_objects`):

| source_system | connection_name | object_type | count |
|---------------|-----------------|-------------|-------|
| gmail | gmail-acct3 | email | 3,504 |
| exchange | exchange-ben-mensio | email | 3,285 |
| gmail | gmail-acct1 | email | 673 |
| gmail | gmail-acct2 | email | 129 |
| exchange | exchange-booking-mensio | email | 36 |
| exchange | exchange-info-mensio | email | 27 |

**Total Emails to Validate & Canonize:** 7,654

### Schema Sources (Provider Versioning)

**Gmail API:**
- Provider: Google
- API Version: `v1`
- Schema versioning: **None** - Gmail evolves incrementally via field additions, no explicit schema version tag
- Schema source string: `gmail/v1/users.messages`
- Our Iglu mapping: `iglu:com.google/gmail_email/jsonschema/1-0-0`
- Docs: https://developers.google.com/gmail/api/reference/rest/v1/users.messages

**Microsoft Graph API (NOT EWS):**
- Provider: Microsoft
- API Version: `v1.0`
- Schema versioning: **None** - Graph is REST-based, no XSD schema like EWS
- Schema source string: `microsoft.graph/v1.0/message`
- Our Iglu mapping: `iglu:com.microsoft/exchange_email/jsonschema/1-0-0`
- Docs: https://learn.microsoft.com/en-us/graph/api/resources/message

**Note on EWS vs Graph:** Exchange/EWS has formal XSD schema versioning via `<RequestServerVersion>`.
However, we use Microsoft Graph API (REST), which does NOT have the same schema discipline.
InJest pulls from `/users/{id}/messages` via Graph, not EWS SOAP endpoints.

**Our Iglu Schemas:**
Since neither provider formally versions their JSON response schemas, our Iglu versions (`1-0-0`)
track when WE captured/defined the expected shape. If the provider changes their response format,
we bump our Iglu schema version and handle migration.

### Canonizer Functions

**Validation (core function - separate from transform):**
```python
canonizer.validate(payload, schema_iglu) → raises ValidationError or returns True
```

**Canonization (transform with built-in validation):**
```python
canonizer.canonicalize(payload, transform_id) → dict
# Internally:
#   1. Validates payload against source schema (MUST pass or raises)
#   2. Transforms via JSONata
#   3. Validates output against canonical schema
#   4. Returns canonical dict
```

### Canonizer Transforms Available

```
transforms/email/gmail_to_jmap_lite/1.0.0/spec.meta.yaml
transforms/email/exchange_to_jmap_lite/1.0.0/spec.meta.yaml
```

Both transform to: `iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0`

### Design Decisions

1. **Validation and canonization are separate jobs**:
   - `validate_gmail_source` - validates and stamps raw_objects
   - `canonize_gmail_jmap` - transforms validated rows to canonical

2. **Canonizer validates ALL schemas**: Source schemas AND canonical schemas. Validation is a core function, not just part of transformation.

3. **Canonize MUST validate first**: `canonizer.canonicalize()` internally validates input against source schema. If validation fails, transform fails. This is enforced by canonizer, not by the job.

4. **Schema source strings use provider's versioning**: `gmail/v1/users.messages`, not our invented names.

5. **Strict validation**: Fail entire payload if any field is invalid. No partial stamping.

6. **Simple re-canonization (Strategy A)**: When transforms change, truncate and re-run. No version history at 7k records.

7. **Boring idem_keys**: `canonical_objects.idem_key = raw_idem_key`. Let columns carry meaning.

8. **Explicit paths**: Canonizer uses `CANONIZER_REGISTRY_ROOT` env var, not CWD magic.

## Table Schemas

### raw_objects (Updated Columns)

Add three new columns to track schema compliance:

```sql
ALTER TABLE `events_dev.raw_objects`
ADD COLUMN IF NOT EXISTS schema_source STRING,      -- Provider's schema version (e.g., "gmail/v1/users.messages")
ADD COLUMN IF NOT EXISTS schema_iglu STRING,        -- Our Iglu URI (e.g., "iglu:com.google/gmail_email/jsonschema/1-0-0")
ADD COLUMN IF NOT EXISTS validation_stamp STRING;   -- Validation proof (e.g., "canonizer:1-0-0:2025-11-26T00:00:00Z")
```

**Validation Stamp Format:**
```
canonizer:<iglu-schema-version>:<iso-timestamp>
```

Example: `canonizer:1-0-0:2025-11-26T12:34:56Z`

This stamp proves:
- The payload was validated by canonizer
- Against schema version `1-0-0` (from the Iglu URI)
- At the specified timestamp

### canonical_objects (New Table)

```sql
CREATE TABLE IF NOT EXISTS `events_dev.canonical_objects` (
  -- Identity (boring key = raw key)
  idem_key STRING NOT NULL,                    -- Same as raw_objects.idem_key

  -- Source Linkage
  source_system STRING NOT NULL,               -- Original provider (gmail, exchange)
  connection_name STRING NOT NULL,             -- Original connection (gmail-acct1)
  object_type STRING NOT NULL,                 -- Canonical type (email)

  -- Schema Info
  canonical_schema STRING NOT NULL,            -- Target schema: iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0
  canonical_format STRING NOT NULL,            -- Format identifier: jmap_lite_email
  transform_ref STRING,                        -- Transform iglu (optional, future-proof)

  -- Validation
  validation_stamp STRING NOT NULL,            -- Post-transform validation: canonizer:1-0-0:2025-11-26T00:00:00Z

  -- Payload
  payload JSON NOT NULL,                       -- Canonical JMAP payload

  -- Tracing
  correlation_id STRING,                       -- Job correlation ID
  trace_id STRING,                             -- Cross-system trace ID

  -- Timestamps
  created_at TIMESTAMP NOT NULL,               -- When canonized
  source_first_seen TIMESTAMP,                 -- When raw object first ingested
  source_last_seen TIMESTAMP                   -- When raw object last updated
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, object_type;
```

## Jobs

### validate_gmail_source

**Purpose:** Validate all Gmail email payloads against source schema and stamp them.

**Entry Point:** `lorchestra.jobs.validate_gmail:job_validate_gmail_source`

**Usage:**
```bash
# Validate all Gmail emails (where validation_stamp IS NULL)
lorchestra run validate_gmail_source

# Validate with date filter (based on last_seen)
lorchestra run validate_gmail_source --since 2025-11-01

# Force re-validate all (ignore existing stamps)
lorchestra run validate_gmail_source --force

# Dry-run (no BQ writes)
lorchestra run validate_gmail_source --dry-run
```

**Process:**
1. Query `raw_objects` where `source_system='gmail'` AND `object_type='email'`
   - Default: WHERE `validation_stamp IS NULL`
   - With `--force`: all rows
   - With `--since`: filter by `last_seen`
2. For each row:
   - Call `canonizer.validate(payload, schema_iglu="iglu:com.google/gmail_email/jsonschema/1-0-0")`
   - If valid: add to batch for stamping
   - If invalid: emit `validation.failed` event, skip row
3. Batch UPDATE `raw_objects` with `schema_source`, `schema_iglu`, `validation_stamp`
4. Emit `validation.completed` event with counts

**Config:**
```python
GMAIL_VALIDATION_CONFIG = {
    "source_system": "gmail",
    "object_type": "email",
    "schema_source": "gmail/v1/users.messages",
    "schema_iglu": "iglu:com.google/gmail_email/jsonschema/1-0-0",
}
```

### validate_exchange_source

**Purpose:** Validate all Exchange email payloads against source schema and stamp them.

**Entry Point:** `lorchestra.jobs.validate_exchange:job_validate_exchange_source`

**Usage:**
```bash
lorchestra run validate_exchange_source
lorchestra run validate_exchange_source --since 2025-11-01
lorchestra run validate_exchange_source --force
lorchestra run validate_exchange_source --dry-run
```

**Config:**
```python
EXCHANGE_VALIDATION_CONFIG = {
    "source_system": "exchange",
    "object_type": "email",
    "schema_source": "microsoft.graph/v1.0/message",
    "schema_iglu": "iglu:com.microsoft/exchange_email/jsonschema/1-0-0",
}
```

### canonize_gmail_jmap

**Purpose:** Transform validated Gmail emails to JMAP Lite canonical format.

**Entry Point:** `lorchestra.jobs.canonize_gmail:job_canonize_gmail_jmap`

**Usage:**
```bash
# Canonize all validated Gmail emails
lorchestra run canonize_gmail_jmap

# With date filter (based on last_seen)
lorchestra run canonize_gmail_jmap --since 2025-11-01

# Dry-run (no BQ writes)
lorchestra run canonize_gmail_jmap --dry-run

# Test table mode
lorchestra run canonize_gmail_jmap --test-table
```

**Process:**
1. Query `raw_objects` where:
   - `source_system='gmail'` AND `object_type='email'`
   - `validation_stamp IS NOT NULL` (ONLY validated rows)
   - Optional: filter by `--since` / `--until` on `last_seen`
2. For each row:
   - Call `canonizer.canonicalize(payload, transform_id="email/gmail_to_jmap_lite@1.0.0")`
   - canonizer internally validates input (should pass since already stamped)
   - canonizer transforms via JSONata
   - canonizer validates output against canonical schema
   - If any step fails: emit `canonize.failed` event, skip row
   - If success: build canonical row with provenance
3. Batch INSERT to `canonical_objects`
4. Emit `canonize.completed` event with counts

**Config:**
```python
GMAIL_CANONIZE_CONFIG = {
    "source_system": "gmail",
    "object_type": "email",
    "schema_iglu": "iglu:com.google/gmail_email/jsonschema/1-0-0",
    "transform_id": "email/gmail_to_jmap_lite@1.0.0",
    "canonical_schema": "iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0",
    "canonical_format": "jmap_lite_email",
}
```

**Important:** If a row has `validation_stamp IS NULL`, it will NOT be canonized. Run `validate_gmail_source` first.

### canonize_exchange_jmap

**Purpose:** Transform validated Exchange emails to JMAP Lite canonical format.

**Entry Point:** `lorchestra.jobs.canonize_exchange:job_canonize_exchange_jmap`

**Usage:**
```bash
lorchestra run canonize_exchange_jmap
lorchestra run canonize_exchange_jmap --since 2025-11-01
lorchestra run canonize_exchange_jmap --dry-run
```

**Config:**
```python
EXCHANGE_CANONIZE_CONFIG = {
    "source_system": "exchange",
    "object_type": "email",
    "schema_iglu": "iglu:com.microsoft/exchange_email/jsonschema/1-0-0",
    "transform_id": "email/exchange_to_jmap_lite@1.0.0",
    "canonical_schema": "iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0",
    "canonical_format": "jmap_lite_email",
}
```

## Implementation Plan

### Step 1: BigQuery Schema Updates [G0: Foundation]

**Tasks:**
1. Add `schema_source`, `schema_iglu`, `validation_stamp` columns to `raw_objects`
2. Create `canonical_objects` table with full schema
3. Create `test_canonical_objects` table (for --test-table mode)

**SQL:**
```sql
-- Update raw_objects
ALTER TABLE `local-orchestration.events_dev.raw_objects`
ADD COLUMN IF NOT EXISTS schema_source STRING,
ADD COLUMN IF NOT EXISTS schema_iglu STRING,
ADD COLUMN IF NOT EXISTS validation_stamp STRING;

-- Create canonical_objects
CREATE TABLE IF NOT EXISTS `local-orchestration.events_dev.canonical_objects` (
  idem_key STRING NOT NULL,
  source_system STRING NOT NULL,
  connection_name STRING NOT NULL,
  object_type STRING NOT NULL,
  canonical_schema STRING NOT NULL,
  canonical_format STRING NOT NULL,
  transform_ref STRING,
  validation_stamp STRING NOT NULL,
  payload JSON NOT NULL,
  correlation_id STRING,
  trace_id STRING,
  created_at TIMESTAMP NOT NULL,
  source_first_seen TIMESTAMP,
  source_last_seen TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, object_type;
```

**Outputs:**
- Updated `raw_objects` schema
- New `canonical_objects` table

---

### Step 2: Canonizer Configuration [G0: Foundation]

**Tasks:**
1. Add `CANONIZER_REGISTRY_ROOT` to `.env`
2. Create canonizer integration module in lorchestra
3. Verify source schemas exist in canonizer registry
4. Verify `canonizer.validate()` works as standalone function

**Files:**
- `.env` - add `CANONIZER_REGISTRY_ROOT=/workspace/canonizer`
- `lorchestra/canonizer_client.py` (new) - wrapper for canonizer calls

**Canonizer Client:**
```python
"""Canonizer integration for lorchestra jobs."""
import os
from pathlib import Path
from datetime import datetime, timezone

def get_canonizer_root() -> Path:
    """Get canonizer registry root from env."""
    root = os.environ.get("CANONIZER_REGISTRY_ROOT")
    if not root:
        raise RuntimeError("CANONIZER_REGISTRY_ROOT not set")
    return Path(root)

def validate_payload(payload: dict, schema_iglu: str) -> tuple[bool, list[str]]:
    """
    Validate payload against schema (source OR canonical).

    Returns (is_valid, errors).
    """
    import sys
    root = get_canonizer_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from canonizer.core.validator import SchemaValidator

    validator = SchemaValidator(schemas_dir=root / "schemas")
    try:
        validator.validate(payload, schema_iglu)
        return True, []
    except Exception as e:
        return False, [str(e)]

def canonicalize_payload(payload: dict, transform_id: str) -> dict:
    """
    Transform payload to canonical format.

    Canonizer internally validates input against source schema,
    transforms, then validates output against canonical schema.
    Raises on any failure.
    """
    import sys
    root = get_canonizer_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from canonizer import canonicalize

    original_cwd = os.getcwd()
    try:
        os.chdir(root)
        return canonicalize(payload, transform_id=transform_id)
    finally:
        os.chdir(original_cwd)

def make_validation_stamp(schema_iglu: str) -> str:
    """
    Create validation stamp string.

    Format: canonizer:<schema-version>:<iso-timestamp>
    Example: canonizer:1-0-0:2025-11-26T12:34:56Z
    """
    # Extract version from iglu URI: iglu:com.google/gmail_email/jsonschema/1-0-0 → 1-0-0
    version = schema_iglu.split("/")[-1]
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"canonizer:{version}:{timestamp}"
```

**Outputs:**
- Configured `CANONIZER_REGISTRY_ROOT`
- Working canonizer integration with separate validate and canonicalize functions

---

### Step 3: Schema Verification [G1: Implementation]

**Tasks:**
1. Verify Gmail source schema matches actual payloads in BQ
2. Verify Exchange source schema matches actual payloads in BQ
3. Sample-test validation on a few raw objects from each source

**Validation Test:**
```python
# Sample validation test
from lorchestra.canonizer_client import validate_payload

# Get sample Gmail payload from BQ
sample = bq_client.query("""
    SELECT payload FROM events_dev.raw_objects
    WHERE source_system = 'gmail' AND object_type = 'email'
    LIMIT 5
""").result()

for row in sample:
    is_valid, errors = validate_payload(row.payload, "iglu:com.google/gmail_email/jsonschema/1-0-0")
    print(f"Valid: {is_valid}, Errors: {errors}")
```

**Outputs:**
- Confirmed schemas match actual data
- Any schema fixes applied to canonizer

---

### Step 4: Gmail Validation Job [G1: Implementation]

**Tasks:**
1. Create `lorchestra/jobs/validate_gmail.py`
2. Implement `job_validate_gmail_source()` function
3. Register entry point in `pyproject.toml`
4. Test with `--dry-run`
5. Run on all Gmail emails

**Files:**
- `lorchestra/jobs/validate_gmail.py` (new)
- `pyproject.toml` (add entry point)

**Outputs:**
- Working `lorchestra run validate_gmail_source` command
- All 4,306 Gmail emails validated and stamped

---

### Step 5: Exchange Validation Job [G1: Implementation]

**Tasks:**
1. Create `lorchestra/jobs/validate_exchange.py`
2. Implement `job_validate_exchange_source()` function
3. Register entry point in `pyproject.toml`
4. Test with `--dry-run`
5. Run on all Exchange emails

**Files:**
- `lorchestra/jobs/validate_exchange.py` (new)
- `pyproject.toml` (add entry point)

**Outputs:**
- Working `lorchestra run validate_exchange_source` command
- All 3,348 Exchange emails validated and stamped

---

### Step 6: Gmail Canonization Job [G1: Implementation]

**Tasks:**
1. Create `lorchestra/jobs/canonize_gmail.py`
2. Implement `job_canonize_gmail_jmap()` function
3. Register entry point in `pyproject.toml`
4. Test with `--dry-run`

**Files:**
- `lorchestra/jobs/canonize_gmail.py` (new)
- `pyproject.toml` (add entry point)

**Outputs:**
- Working `lorchestra run canonize_gmail_jmap` command

---

### Step 7: Exchange Canonization Job [G1: Implementation]

**Tasks:**
1. Create `lorchestra/jobs/canonize_exchange.py`
2. Implement `job_canonize_exchange_jmap()` function
3. Register entry point in `pyproject.toml`
4. Test with `--dry-run`

**Files:**
- `lorchestra/jobs/canonize_exchange.py` (new)
- `pyproject.toml` (add entry point)

**Outputs:**
- Working `lorchestra run canonize_exchange_jmap` command

---

### Step 8: Full Pipeline Test [G2: Validation]

**Tasks:**
1. Verify all emails are validated (check validation_stamp NOT NULL)
2. Run Gmail canonization
3. Run Exchange canonization
4. Verify canonical_objects contains expected data
5. Check event_log for any failures

**Commands:**
```bash
# Verify validation complete
lorchestra bq query "SELECT source_system, COUNT(*) as total, COUNTIF(validation_stamp IS NOT NULL) as validated FROM events_dev.raw_objects WHERE object_type='email' GROUP BY 1"

# Canonize all emails
lorchestra run canonize_gmail_jmap
lorchestra run canonize_exchange_jmap

# Verify results
lorchestra bq query "SELECT source_system, COUNT(*) FROM events_dev.canonical_objects GROUP BY 1"

# Check for failures
lorchestra bq query "SELECT event_type, COUNT(*) FROM events_dev.event_log WHERE event_type LIKE '%failed%' GROUP BY 1"
```

**Outputs:**
- All 7,654 emails canonized
- Event telemetry showing success

---

### Step 9: BQ Query Helper (Optional) [G3: Polish]

**Tasks:**
1. Add `lorchestra bq query` command as thin DBA helper
2. Simple SQL execution with table output

**Files:**
- `lorchestra/cli.py` (add bq group)

**Outputs:**
- Working `lorchestra bq query` command

## Telemetry Events

### validation.started
```json
{
  "event_type": "validation.started",
  "source_system": "lorchestra",
  "correlation_id": "validate-gmail-20251126120000",
  "payload": {
    "source_filter": "gmail",
    "object_type": "email",
    "schema_iglu": "iglu:com.google/gmail_email/jsonschema/1-0-0",
    "target_count": 4306
  }
}
```

### validation.failed
```json
{
  "event_type": "validation.failed",
  "source_system": "lorchestra",
  "correlation_id": "validate-gmail-20251126120000",
  "status": "failed",
  "error_message": "Schema validation failed",
  "payload": {
    "raw_idem_key": "gmail:gmail-acct1:email:18c5f2e8a9b4d7f3",
    "schema_iglu": "iglu:com.google/gmail_email/jsonschema/1-0-0",
    "errors": ["'payload.headers' is a required property"]
  }
}
```

### validation.completed
```json
{
  "event_type": "validation.completed",
  "source_system": "lorchestra",
  "correlation_id": "validate-gmail-20251126120000",
  "status": "ok",
  "payload": {
    "source_filter": "gmail",
    "object_type": "email",
    "schema_iglu": "iglu:com.google/gmail_email/jsonschema/1-0-0",
    "total_processed": 4306,
    "valid_count": 4300,
    "invalid_count": 6,
    "duration_seconds": 45.2
  }
}
```

### canonize.started
```json
{
  "event_type": "canonize.started",
  "source_system": "lorchestra",
  "correlation_id": "canonize-gmail-20251126130000",
  "payload": {
    "source_filter": "gmail",
    "object_type": "email",
    "transform_id": "email/gmail_to_jmap_lite@1.0.0",
    "target_count": 4300
  }
}
```

### canonize.failed
```json
{
  "event_type": "canonize.failed",
  "source_system": "lorchestra",
  "correlation_id": "canonize-gmail-20251126130000",
  "status": "failed",
  "error_message": "Transform execution failed",
  "payload": {
    "raw_idem_key": "gmail:gmail-acct1:email:18c5f2e8a9b4d7f3",
    "transform_id": "email/gmail_to_jmap_lite@1.0.0",
    "error": "JSONata error: Cannot read property 'headers' of undefined"
  }
}
```

### canonize.completed
```json
{
  "event_type": "canonize.completed",
  "source_system": "lorchestra",
  "correlation_id": "canonize-gmail-20251126130000",
  "status": "ok",
  "payload": {
    "source_filter": "gmail",
    "object_type": "email",
    "transform_id": "email/gmail_to_jmap_lite@1.0.0",
    "total_processed": 4300,
    "canonical_count": 4298,
    "failed_count": 2,
    "duration_seconds": 180.5
  }
}
```

## Error Handling

### Validation Failures

When a payload fails schema validation:
1. Emit `validation.failed` event with `idem_key`, `schema_iglu`, and error list
2. Do NOT update `validation_stamp` (remains NULL)
3. Continue processing other rows
4. Include failure count in `validation.completed` event

### Canonization Failures

When canonization fails (transform error OR output validation error):
1. Emit `canonize.failed` event with `idem_key`, `transform_id`, and exception
2. Do NOT insert to `canonical_objects`
3. Continue processing other rows
4. Include failure count in `canonize.completed` event

**Note:** If source validation inside `canonizer.canonicalize()` fails, that's a bug - the row should have been stamped by the validation job first. This would indicate either:
- Schema drift (source API changed)
- Data corruption
- Validation job didn't run

### Re-canonization

When transforms are updated:
1. Truncate `canonical_objects` for the specific `(source_system, object_type)`
2. Re-run the canonization job
3. No version history needed at current scale (7k records)

```sql
-- Before re-running canonize_gmail_jmap after transform update
DELETE FROM `events_dev.canonical_objects`
WHERE source_system = 'gmail' AND object_type = 'email';
```

## Environment Variables

```bash
# Add to .env
CANONIZER_REGISTRY_ROOT=/workspace/canonizer
```

## Entry Points (pyproject.toml)

```toml
[project.entry-points."lorchestra.jobs"]
# ... existing ingestion jobs ...

# Validation jobs
validate_gmail_source = "lorchestra.jobs.validate_gmail:job_validate_gmail_source"
validate_exchange_source = "lorchestra.jobs.validate_exchange:job_validate_exchange_source"

# Canonization jobs
canonize_gmail_jmap = "lorchestra.jobs.canonize_gmail:job_canonize_gmail_jmap"
canonize_exchange_jmap = "lorchestra.jobs.canonize_exchange:job_canonize_exchange_jmap"
```

## Models & Tools

**Tools:** bash, python, lorchestra, canonizer

**Models:** (defaults)

**Dependencies:**
- canonizer (local: `/workspace/canonizer`)
- google-cloud-bigquery

## Future Work (Out of Scope)

- **Inline validation in ingestion jobs**: After this spec, InJest will call `canonizer.validate()` during extraction, rejecting invalid payloads before they reach BQ. This is NOT part of this spec.
- Stripe validation/canonization (separate spec)
- Dataverse validation/canonization (separate spec)
- Google Forms validation/canonization (separate spec)
- Transform versioning / history (when scale demands it)
- Async/streaming canonization (when performance demands it)

## Repository

**Branch:** `feat/email-canonization`

**Commits:** (TBD)
