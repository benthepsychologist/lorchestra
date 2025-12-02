---
version: "0.1"
tier: C
title: Canonization Pipeline
owner: benthepsychologist
goal: Add verification stamping and canonization jobs for all ingested data sources
labels: [canonizer, bigquery, pipeline]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-02T11:41:20.349949+00:00
updated: 2025-12-02T11:41:20.349949+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/canonization-pipeline"
---

# Canonization Pipeline

## Objective

> Add verification stamping and canonization jobs for all ingested data sources (Gmail, Exchange, Google Forms, Dataverse, Stripe). This spec covers the lorchestra side: job definitions, script updates, and testing. Schema and transform work is done in the canonizer repo.

## Acceptance Criteria

- [ ] All ingest sources have corresponding validate+stamp jobs
- [ ] daily_ingest.sh runs verification stamping after ingestion
- [ ] Google Forms validation and canonization jobs working
- [ ] Dataverse validation and canonization jobs working (requires canonizer spec complete)
- [ ] Stripe validation and canonization jobs working (requires canonizer spec complete)
- [ ] daily_canonize.sh script created
- [ ] Tests pass for all new jobs
- [ ] CI green (lint + unit)

## Context

### Background

Daily ingestion is working. We ingest raw data from 5 sources:
- **Gmail** (3 accounts) - validation/canonization jobs exist
- **Exchange** (4 mailboxes) - validation/canonization jobs exist
- **Google Forms** (4 forms) - transform exists in canonizer, needs jobs here
- **Dataverse** (contacts, sessions, reports) - needs canonizer work first, then jobs here
- **Stripe** (customers, invoices, payment_intents, refunds) - needs canonizer work first, then jobs here

### Architecture

```
Raw Ingest → Verify & Stamp → Canonize
     ↓              ↓              ↓
raw_objects    validation     canonical_objects
  table         status           table
```

### Dependencies

- `/workspace/canonizer` - transformation library (separate repo, separate spec)
- Existing ingest jobs in `jobs/definitions/`
- CanonizeProcessor in `processors/canonize.py`

### Blocked By

- **Dataverse jobs (Steps 8-10)**: Require canonizer spec Phase 1 (Dataverse schemas/transforms)
- **Stripe jobs (Steps 13-15)**: Require canonizer spec Phase 2 (Stripe schemas/transforms)

### Constraints

- This spec only covers lorchestra job definitions and scripts
- Schema/transform work is done in canonizer repo via its own spec

## Plan

### Phase 1: Verification Stamping Infrastructure

### Step 1: Verify Email Stamping Jobs Work [G0: Plan Approval]

**Prompt:**

Verify existing validate jobs work correctly:
- `validate_gmail_source.json`
- `validate_exchange_source.json`

Test that they stamp raw records with validation_status using CanonizeProcessor.

**Commands:**

```bash
lorchestra run validate_gmail_source --dry-run
lorchestra run validate_exchange_source --dry-run
```

**Outputs:**

- `artifacts/phase1/email-validation-verified.md`

### Step 2: Create Google Forms Validate Job

**Prompt:**

Create `validate_google_forms_source.json` job definition.
Source schema: `iglu:com.google/forms_response/jsonschema/1-0-0`
Mode: validate_only

**Outputs:**

- `lorchestra/jobs/definitions/validate_google_forms_source.json`

### Step 3: Add Verification to Daily Ingest

**Prompt:**

Modify `scripts/daily_ingest.sh` to run verification stamping after ingestion.
Add a verification section that runs validate jobs for sources that have them:
- validate_gmail_source
- validate_exchange_source
- validate_google_forms_source

Structure the script with clear sections for ingest vs verify.

**Outputs:**

- `scripts/daily_ingest.sh`

---

### Phase 2: Google Forms Canonization

### Step 4: Create Google Forms Canonize Job

**Prompt:**

Create `canonize_google_forms.json` job definition.
Transform: `forms/google_forms_to_canonical@1.0.0` (exists in canonizer)

The job should:
- Source from google_forms / form_response with validation_status: pass
- Transform using existing forms transform
- Output to canonical form_response schema

**Outputs:**

- `lorchestra/jobs/definitions/canonize_google_forms.json`

### Step 5: Test Google Forms Pipeline

**Prompt:**

Test the full pipeline: ingest → validate → canonize for Google Forms.
Verify records flow through correctly.

**Commands:**

```bash
lorchestra run validate_google_forms_source
lorchestra run canonize_google_forms --dry-run
```

**Outputs:**

- `artifacts/phase2/google-forms-pipeline-test.md`

---

### Phase 3: Dataverse Jobs (Blocked by Canonizer Spec)

### Step 6: Create Dataverse Validate Jobs

**Prompt:**

Create validation jobs for Dataverse entities:
- `validate_dataverse_contacts.json`
- `validate_dataverse_sessions.json`
- `validate_dataverse_reports.json`

Use source schemas from canonizer:
- `iglu:com.microsoft/dataverse_contact/jsonschema/1-0-0`
- `iglu:com.microsoft/dataverse_session/jsonschema/1-0-0`
- `iglu:com.microsoft/dataverse_report/jsonschema/1-0-0`

**Outputs:**

- `lorchestra/jobs/definitions/validate_dataverse_contacts.json`
- `lorchestra/jobs/definitions/validate_dataverse_sessions.json`
- `lorchestra/jobs/definitions/validate_dataverse_reports.json`

### Step 7: Create Dataverse Canonize Jobs

**Prompt:**

Create canonization jobs for Dataverse entities:
- `canonize_dataverse_contacts.json`
- `canonize_dataverse_sessions.json`
- `canonize_dataverse_reports.json`

Use transforms from canonizer:
- `contact/dataverse_to_canonical@1.0.0`
- `clinical_session/dataverse_to_canonical@1.0.0`
- `report/dataverse_to_canonical@1.0.0`

**Outputs:**

- `lorchestra/jobs/definitions/canonize_dataverse_contacts.json`
- `lorchestra/jobs/definitions/canonize_dataverse_sessions.json`
- `lorchestra/jobs/definitions/canonize_dataverse_reports.json`

### Step 8: Update Daily Ingest for Dataverse

**Prompt:**

Add Dataverse verification to daily_ingest.sh verification section.

**Outputs:**

- `scripts/daily_ingest.sh`

---

### Phase 4: Stripe Jobs (Blocked by Canonizer Spec)

### Step 9: Create Stripe Validate Jobs

**Prompt:**

Create validation jobs:
- `validate_stripe_customers.json`
- `validate_stripe_invoices.json`
- `validate_stripe_payment_intents.json`
- `validate_stripe_refunds.json`

Use source schemas from canonizer.

**Outputs:**

- `lorchestra/jobs/definitions/validate_stripe_customers.json`
- `lorchestra/jobs/definitions/validate_stripe_invoices.json`
- `lorchestra/jobs/definitions/validate_stripe_payment_intents.json`
- `lorchestra/jobs/definitions/validate_stripe_refunds.json`

### Step 10: Create Stripe Canonize Jobs

**Prompt:**

Create canonization jobs:
- `canonize_stripe_customers.json`
- `canonize_stripe_invoices.json`
- `canonize_stripe_payment_intents.json`
- `canonize_stripe_refunds.json`

Use transforms from canonizer.

**Outputs:**

- `lorchestra/jobs/definitions/canonize_stripe_customers.json`
- `lorchestra/jobs/definitions/canonize_stripe_invoices.json`
- `lorchestra/jobs/definitions/canonize_stripe_payment_intents.json`
- `lorchestra/jobs/definitions/canonize_stripe_refunds.json`

### Step 11: Update Daily Ingest for Stripe

**Prompt:**

Add Stripe verification to daily_ingest.sh verification section.

**Outputs:**

- `scripts/daily_ingest.sh`

---

### Phase 5: Integration & Validation

### Step 12: Create Daily Canonize Script

**Prompt:**

Create `scripts/daily_canonize.sh` that runs all canonization jobs:
- canonize_gmail_jmap
- canonize_exchange_jmap
- canonize_google_forms
- canonize_dataverse_contacts
- canonize_dataverse_sessions
- canonize_dataverse_reports
- canonize_stripe_customers
- canonize_stripe_invoices
- canonize_stripe_payment_intents
- canonize_stripe_refunds

Mirror the structure of daily_ingest.sh with failure tracking.

**Outputs:**

- `scripts/daily_canonize.sh`

### Step 13: End-to-End Testing [G1: Code Readiness]

**Prompt:**

Test the complete pipeline for all sources:
1. Run daily_ingest.sh (ingest + verify)
2. Run daily_canonize.sh
3. Verify canonical output in BigQuery

**Commands:**

```bash
./scripts/daily_ingest.sh
./scripts/daily_canonize.sh
```

**Outputs:**

- `artifacts/phase5/e2e-test-results.md`

### Step 14: Final Validation [G2: Pre-Release]

**Prompt:**

Run full test suite and linting.

**Commands:**

```bash
ruff check .
pytest -q --tb=short
```

**Outputs:**

- `artifacts/phase5/test-pass-confirmation.md`

## Models & Tools

**Tools:** bash, pytest, ruff, lorchestra CLI

**Models:** Claude (implementation)

## Repository

**Branch:** `feat/canonization-pipeline`

**Merge Strategy:** squash
