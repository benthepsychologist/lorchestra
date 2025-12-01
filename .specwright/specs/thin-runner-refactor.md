---
version: "0.1"
tier: B
title: Thin Runner Refactor
owner: benthepsychologist
goal: Replace per-job Python files with a single generic runner + JSON job specs + typed processors
labels: [architecture, refactor, jobs]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-01T18:43:06.569615+00:00
updated: 2025-12-01T18:43:06.569615+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/thin-runner-refactor"
---

# Thin Runner Refactor

## Objective

> Replace the current "one .py file per job" pattern with a **single generic job runner** that:
> 1. Loads JSON job specs
> 2. Instantiates shared clients (event_client, storage_client, bq_client)
> 3. Dispatches to typed **processors** (ingest, canonize, final_form)
> 4. Keeps IO and event emission in the job layer, not in the libraries

## Acceptance Criteria

- [ ] Single `job_runner.py` dispatches all jobs by `job_type`
- [ ] Three processor classes: `IngestProcessor`, `CanonizeProcessor`, `FinalFormProcessor`
- [ ] All jobs defined as JSON specs in `jobs/specs/*.json`, not Python files
- [ ] Libraries (`injest`, `canonizer`, `final-form`) are pure transforms—no IO, no events
- [ ] Validation jobs are `canonize` jobs with `mode: "validate_only"` (no fourth processor)
- [ ] Existing 21 jobs work via JSON specs
- [ ] CLI resolution: `lorchestra run <job_id>` → `jobs/specs/<job_id>.json`
- [ ] CI green (lint + unit tests)
- [ ] 80% test coverage on new processor code (behavior-focused: happy path, source errors, transform errors, event emission)

## Context

### Background

**Current state (the kludge):** We have 10 separate Python files under `lorchestra/jobs/`:

```
ingest_gmail.py      (306 lines, 3 account functions)
ingest_exchange.py   (346 lines, 4 account functions)
ingest_dataverse.py  (290 lines, 3 entity functions)
ingest_google_forms.py (322 lines, 4 form functions)
ingest_stripe.py     (320 lines, 4 object functions)
validate_gmail.py    (315 lines)
validate_exchange.py (315 lines)
canonize_gmail.py    (249 lines)
canonize_exchange.py (249 lines)
```

Each file duplicates:
- BigQuery client setup
- Event emission patterns (log_event, upsert_objects)
- Error handling and telemetry
- Date parsing and state management

This creates **~2,800 lines of boilerplate** that's hard to maintain and test.

**Target state:**

```
lorchestra/
├── job_runner.py              # Single entry point (~100 lines)
├── processors/
│   ├── __init__.py
│   ├── base.py                # Processor protocol
│   ├── ingest.py              # IngestProcessor (~150 lines)
│   ├── canonize.py            # CanonizeProcessor (~150 lines)
│   └── final_form.py          # FinalFormProcessor (~150 lines)
├── jobs/
│   ├── specs/                 # JSON job definitions
│   │   ├── gmail_ingest_acct1.json
│   │   ├── gmail_ingest_acct2.json
│   │   ├── stripe_ingest_customers.json
│   │   ├── canonize_gmail_jmap.json
│   │   └── ... (21 total)
│   └── __init__.py            # Job discovery from JSON
└── stack_clients/
    ├── event_client.py        # (existing)
    └── storage_client.py      # (new, for GCS/local FS)
```

### Key Architecture Rules

1. **Single generic job runner**
   - Loads JSON job spec
   - Instantiates shared clients once
   - Dispatches to processor by `job_spec["job_type"]`

2. **Job types wrap libraries (IO + events live in the job, not the library)**

   | Job Type | Library | Responsibility |
   |----------|---------|----------------|
   | `ingest` | `injest` | Call injest → write raw_objects → emit events |
   | `canonize` | `canonizer` | Read raw → validate → transform → write canonical_objects → emit events |
   | `final_form` | `final-form` | Read canonical → call final-form → write finalized → emit events |

3. **Validation is canonizer's job, not a separate concern**

   ```
   ingest → raw_objects (whatever shape, we accept it)
              ↓
   canonize → [validate input] → [transform] → [validate output] → canonical_objects
              ↓
   final_form → [validate canonical input via canonizer] → [process] → measurement_events
   ```

   - **Ingest never validates.** If garbage comes in, canonize deals with it.
   - **Canonize owns all schema validation** (input AND output).
   - **FinalForm may call canonizer** for canonical validation if needed, but never validates upstream.
   - **"Validation jobs"** are just `canonize` with `mode: "validate_only"` (stamps `validation_status`, no transform).

4. **Libraries are pure-ish transforms**
   - Receive Python objects/JSON + config
   - Return transformed data
   - Do NOT handle storage or events

5. **Processors implement common interface**
   ```python
   class Processor(Protocol):
       def run(
           self,
           job_spec: dict,
           storage_client: StorageClient,
           event_client: EventClient,
           bq_client: bigquery.Client | None = None
       ) -> None:
           ...
   ```

6. **Jobs are JSON, not Python**

   Minimal v1 schema—keep it small, use `options` for free-form config:
   ```json
   {
     "job_id": "gmail_ingest_acct1",
     "job_type": "ingest",
     "source": { ... },
     "sink": { ... },
     "transform": { ... },
     "events": { ... },
     "options": { ... }
   }
   ```

   - `source` / `sink` / `transform` are typed by `job_type` in processor code
   - `options` is free-form, parsed by processors as needed (batch_size, dry_run, etc.)
   - Don't over-specify v1—tighten only when real bugs/needs show up

7. **CLI resolution**

   `lorchestra run <job_name>`:
   - Resolves `<job_name>` → `jobs/specs/<job_name>.json`
   - Loads and validates the spec
   - Dispatches to the appropriate processor

### Constraints

- No edits to `injest` library internals (it stays pure)
- No edits to `canonizer` library internals (it stays pure)
- CLI interface unchanged: `lorchestra run gmail_ingest_acct1` must work
- No changes to BigQuery schema (event_log, raw_objects, canonical_objects)

## Plan

### Step 1: Design Job Spec Schema [G0: Design Approval]

**Prompt:**

Design the JSON schema for job specs. Cover all three job types (ingest, canonize, final_form) with examples for:
- Gmail ingest (existing)
- Stripe ingest (existing)
- Gmail canonization (existing)
- PHQ-9 final-form (future)

Include:
- Required vs optional fields
- How to specify date ranges, limits, filters
- How to reference transforms/schemas
- Event configuration

**Outputs:**

- `artifacts/design/job-spec-schema.json` (JSON Schema)
- `artifacts/design/job-spec-examples.md` (annotated examples)

---

### Step 2: Implement Processor Protocol [G1: Code Review]

**Prompt:**

Create the processor base class and protocol:

1. Define `Processor` protocol in `processors/base.py`
2. Define `StorageClient` interface (read/write JSON objects)
3. Define processor registry for dispatch by job_type

Keep it minimal—just the interface, no implementation yet.

**Commands:**

```bash
ruff check lorchestra/processors/
pytest tests/test_processors.py -v
```

**Outputs:**

- `lorchestra/processors/__init__.py`
- `lorchestra/processors/base.py`
- `tests/test_processors.py` (protocol tests)

---

### Step 3: Implement IngestProcessor [G1: Code Review]

**Prompt:**

Implement `IngestProcessor` that:

1. Reads stream config from job_spec (stream name, identity, date range)
2. Calls `injest.get_stream()` to pull data (injest does NO IO)
3. Writes records to raw_objects via `storage_client.upsert_objects()`
4. Emits `ingest.completed` event via `event_client.log_event()`

Migrate the common logic from `ingest_gmail.py`:
- `_get_last_sync_timestamp()` → processor method
- `_parse_date_to_datetime()` → processor method
- Batch upsert pattern → processor method

**Commands:**

```bash
ruff check lorchestra/processors/ingest.py
pytest tests/test_ingest_processor.py -v
```

**Outputs:**

- `lorchestra/processors/ingest.py`
- `tests/test_ingest_processor.py`

---

### Step 4: Implement CanonizeProcessor [G1: Code Review]

**Prompt:**

Implement `CanonizeProcessor` that supports two modes:

**Mode: `validate_only`** (for validation jobs)
1. Reads source config from job_spec (source_system, object_type, schema_in)
2. Queries raw_objects for records not yet validated
3. Calls `canonizer.validate()` to check schema conformance (canonizer does NO IO)
4. Updates `validation_status` field on raw_objects via `storage_client`
5. Emits `validate.completed` event

**Mode: `full`** (default, for canonization jobs)
1. Reads source config from job_spec (source_system, object_type, transform_ref)
2. Queries raw_objects for validated records (`validation_status = 'pass'`) not yet canonized
3. Calls `canonizer.validate()` on input, then `canonizer.transform()` (canonizer does NO IO)
4. Optionally validates output against `schema_out`
5. Writes canonical records via `storage_client`
6. Emits `canonize.completed` event

Migrate logic from `canonize_gmail.py` and `validate_gmail.py`:
- Query pattern for unprocessed records
- Batch insert to canonical_objects
- Transform caching
- Validation stamp updates

**Commands:**

```bash
ruff check lorchestra/processors/canonize.py
pytest tests/test_canonize_processor.py -v
```

**Outputs:**

- `lorchestra/processors/canonize.py`
- `tests/test_canonize_processor.py`

---

### Step 5: Implement FinalFormProcessor [G1: Code Review]

**Prompt:**

Implement `FinalFormProcessor` that:

1. Reads source config from job_spec (canonical_schema, instrument_id, binding_id)
2. Queries canonical_objects for records matching the instrument
3. Calls `final_form.process()` to produce measurement_event + observations
4. Writes finalized objects via `storage_client` and/or `bq_client`
5. Emits `finalization.completed` event

This is a new processor—design for PHQ-9, GAD-7, and similar clinical instruments.

**Commands:**

```bash
ruff check lorchestra/processors/final_form.py
pytest tests/test_final_form_processor.py -v
```

**Outputs:**

- `lorchestra/processors/final_form.py`
- `tests/test_final_form_processor.py`

---

### Step 6: Implement Job Runner [G1: Code Review]

**Prompt:**

Implement `job_runner.py` that:

1. Loads job spec from `jobs/specs/{job_id}.json`
2. Instantiates shared clients:
   - `event_client` (existing)
   - `storage_client` (new)
   - `bq_client` (existing pattern)
3. Dispatches to processor by `job_spec["job_type"]`
4. Wraps execution with:
   - `job.started` event
   - `job.completed` or `job.failed` event
   - Duration tracking
   - Error handling

**Commands:**

```bash
ruff check lorchestra/job_runner.py
pytest tests/test_job_runner.py -v
```

**Outputs:**

- `lorchestra/job_runner.py`
- `tests/test_job_runner.py`

---

### Step 7: Create JSON Job Specs [G1: Code Review]

**Prompt:**

Create JSON job specs for all 21 existing jobs:

**Ingest jobs (18):**
- `gmail_ingest_acct1.json`, `gmail_ingest_acct2.json`, `gmail_ingest_acct3.json`
- `exchange_ingest_ben_mensio.json`, `exchange_ingest_booking_mensio.json`, `exchange_ingest_info_mensio.json`, `exchange_ingest_ben_efs.json`
- `dataverse_ingest_contacts.json`, `dataverse_ingest_sessions.json`, `dataverse_ingest_reports.json`
- `google_forms_ingest_ipip120.json`, `google_forms_ingest_intake_01.json`, `google_forms_ingest_intake_02.json`, `google_forms_ingest_followup.json`
- `stripe_ingest_customers.json`, `stripe_ingest_invoices.json`, `stripe_ingest_payment_intents.json`, `stripe_ingest_refunds.json`

**Canonize jobs with `mode: "validate_only"` (2):**
- `validate_gmail_source.json` → `job_type: "canonize"`, `transform.mode: "validate_only"`
- `validate_exchange_source.json` → `job_type: "canonize"`, `transform.mode: "validate_only"`

**Canonize jobs with `mode: "full"` (2):**
- `canonize_gmail_jmap.json`
- `canonize_exchange_jmap.json`

**Commands:**

```bash
# Validate all specs against schema
python -c "import json; [json.load(open(f)) for f in glob.glob('lorchestra/jobs/specs/*.json')]"
```

**Outputs:**

- `lorchestra/jobs/specs/*.json` (22 files)

---

### Step 8: Update CLI for JSON Dispatch [G1: Code Review]

**Prompt:**

Update `cli.py` to:

1. Discover jobs from `jobs/specs/*.json` instead of entrypoints
2. Call `job_runner.run(job_id, **kwargs)` instead of `execute_job()`
3. CLI unchanged: `lorchestra run gmail_ingest_acct1` works
4. Add `lorchestra jobs validate` command to check specs against schema

Remove the entrypoints from `pyproject.toml` after migration.

**Commands:**

```bash
lorchestra jobs list
lorchestra run gmail_ingest_acct1 --dry-run
ruff check lorchestra/cli.py
pytest tests/test_cli.py -v
```

**Outputs:**

- `lorchestra/cli.py` (updated)
- `pyproject.toml` (entrypoints removed)
- `tests/test_cli.py` (updated)

---

### Step 9: Delete Legacy Job Files [G2: Pre-Release]

**Prompt:**

After confirming all tests pass with the new architecture:

1. Delete the old job files:
   - `lorchestra/jobs/ingest_gmail.py`
   - `lorchestra/jobs/ingest_exchange.py`
   - `lorchestra/jobs/ingest_dataverse.py`
   - `lorchestra/jobs/ingest_google_forms.py`
   - `lorchestra/jobs/ingest_stripe.py`
   - `lorchestra/jobs/validate_gmail.py`
   - `lorchestra/jobs/validate_exchange.py`
   - `lorchestra/jobs/canonize_gmail.py`
   - `lorchestra/jobs/canonize_exchange.py`

2. Update `lorchestra/jobs/__init__.py` for new discovery pattern

3. Run full test suite to confirm nothing breaks

**Commands:**

```bash
pytest tests/ -v --cov=lorchestra --cov-report=term-missing
ruff check .
```

**Outputs:**

- `artifacts/migration/deleted-files.md` (list of removed files)
- Coverage report showing 80%+ on processors

---

### Step 10: Integration Testing [G3: Pre-Release]

**Prompt:**

Run end-to-end tests:

1. `lorchestra run gmail_ingest_acct1 --dry-run` works
2. `lorchestra run canonize_gmail_jmap --dry-run` works
3. All 21 jobs can be loaded and validated
4. Event emission patterns match old behavior

**Commands:**

```bash
# Dry-run all jobs
for spec in lorchestra/jobs/specs/*.json; do
  lorchestra run $(basename $spec .json) --dry-run
done

# Full test suite
pytest tests/ -v
```

**Outputs:**

- `artifacts/test/integration-results.md`

---

### Step 11: Documentation & Cleanup [G4: Post-Implementation]

**Prompt:**

Update documentation:

1. Update `docs/jobs.md` for new JSON-based job creation
2. Update `ARCHITECTURE.md` with processor pattern
3. Add `docs/job-spec-reference.md` with schema documentation
4. Update `README.md` usage examples

**Outputs:**

- `docs/jobs.md` (updated)
- `docs/job-spec-reference.md` (new)
- `ARCHITECTURE.md` (updated)
- `artifacts/governance/decision-log.md`

## Models & Tools

**Tools:** bash, pytest, ruff, python

**Models:** Claude (implementation), GPT-5 (review)

## Repository

**Branch:** `feat/thin-runner-refactor`

**Merge Strategy:** squash

## Appendix: Job Spec Schema (Minimal v1)

Keep the schema small and brutally clear. Don't over-specify—tighten only when real bugs/needs show up.

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "LorchestraJobSpec",
  "type": "object",
  "required": ["job_id", "job_type"],
  "properties": {
    "job_id": {
      "type": "string",
      "description": "Unique job identifier, matches filename without .json"
    },
    "job_type": {
      "type": "string",
      "enum": ["ingest", "canonize", "final_form"],
      "description": "Determines which processor handles this job"
    },
    "source": {
      "type": "object",
      "description": "Source configuration—structure varies by job_type, interpreted by processor"
    },
    "sink": {
      "type": "object",
      "description": "Sink configuration—structure varies by job_type, interpreted by processor"
    },
    "transform": {
      "type": "object",
      "description": "Transform configuration—only for canonize/final_form, interpreted by processor"
    },
    "events": {
      "type": "object",
      "description": "Event names for lifecycle (optional, has defaults)"
    },
    "options": {
      "type": "object",
      "description": "Free-form options parsed by processors (batch_size, dry_run, etc.)"
    }
  }
}
```

**Key design decisions:**
- `source` / `sink` / `transform` are **typed by `job_type` in processor code**, not in schema
- `options` is **free-form** for now, parsed by processors as needed
- Don't try to unify transform fields across job types—keep them job-type-specific

## Appendix: Example Job Specs

### Ingest Job (Gmail)

```json
{
  "job_id": "gmail_ingest_acct1",
  "job_type": "ingest",
  "source": {
    "stream": "gmail.messages",
    "identity": "gmail:acct1"
  },
  "sink": {
    "source_system": "gmail",
    "connection_name": "gmail-acct1",
    "object_type": "email"
  },
  "options": {
    "auto_since": true
  }
}
```

### Ingest Job (Stripe)

```json
{
  "job_id": "stripe_ingest_customers",
  "job_type": "ingest",
  "source": {
    "stream": "stripe.customers",
    "identity": "stripe:default"
  },
  "sink": {
    "source_system": "stripe",
    "connection_name": "stripe-prod",
    "object_type": "customer"
  }
}
```

### Canonize Job: Validate Only (Gmail)

Validation jobs are `canonize` with `mode: "validate_only"`. No transform, just stamp `validation_status`.

```json
{
  "job_id": "validate_gmail_source",
  "job_type": "canonize",
  "source": {
    "source_system": "gmail",
    "object_type": "email"
  },
  "transform": {
    "mode": "validate_only",
    "schema_in": "iglu:raw/email_gmail/jsonschema/1-0-0"
  },
  "sink": {
    "update_field": "validation_status"
  }
}
```

### Canonize Job: Full Transform (Gmail → JMAP)

```json
{
  "job_id": "canonize_gmail_jmap",
  "job_type": "canonize",
  "source": {
    "source_system": "gmail",
    "object_type": "email",
    "filter": {
      "validation_status": "pass"
    }
  },
  "transform": {
    "mode": "full",
    "schema_in": "iglu:raw/email_gmail/jsonschema/1-0-0",
    "schema_out": "iglu:canonical/email_jmap/jsonschema/1-0-0",
    "transform_ref": "gmail_to_jmap_lite@1.0.0"
  },
  "sink": {
    "table": "canonical_objects"
  }
}
```

### Final-Form Job (PHQ-9)

Transform config is job-type-specific—don't try to unify with canonize transform fields.

```json
{
  "job_id": "final_form_phq9",
  "job_type": "final_form",
  "source": {
    "table": "canonical_objects",
    "filter": {
      "canonical_schema": "iglu:canonical/questionnaire_response/jsonschema/1-0-0",
      "instrument_id": "phq-9"
    }
  },
  "transform": {
    "instrument_id": "phq-9",
    "instrument_version": "1.0.0",
    "binding_id": "intake_v1",
    "finalform_spec": "phq9_finalform@1.0.0"
  },
  "sink": {
    "measurement_table": "measurement_events",
    "observation_table": "observations"
  }
}
```