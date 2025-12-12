---
version: "0.1"
tier: C
title: Add Pipeline Composite Jobs
owner: benthepsychologist
goal: Define composite jobs that group atomic pipeline steps into single orchestration units
labels: [feature, pipeline, jobs]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-11T00:00:00+00:00
updated: 2025-12-11T00:00:00+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/pipeline-composites"
---

# Add Pipeline Composite Jobs

## Objective

> Define composite jobs that group atomic pipeline steps into single orchestration units callable by external tools.

### Background

The daily data pipeline currently runs via bash scripts (`life-cockpit/scripts/`) that call `lorchestra run <job>` repeatedly with manual failure tracking.

This spec adds composite jobs that encapsulate each pipeline phase. External tools (like `life-cli`) can then invoke ONE job per phase instead of knowing internal job structure.

**Current state** (bash scripts know all jobs):
```bash
# daily_ingest.sh knows 17+ individual jobs
lorchestra run ingest_gmail_acct1
lorchestra run ingest_gmail_acct2
lorchestra run ingest_exchange_ben_mensio
# ... etc
```

**Target state** (single composite job):
```bash
lorchestra run pipeline.ingest
# Lorchestra handles the 17+ internal jobs
```

### Acceptance Criteria

**Functional:**
- [ ] `pipeline.ingest` runs all ingestion + validation jobs
- [ ] `pipeline.canonize` runs all canonization jobs
- [ ] `pipeline.formation` runs measurement events + observations jobs
- [ ] `pipeline.project` runs BQ sync + file projection jobs
- [ ] `pipeline.views` runs all view creation jobs
- [ ] `pipeline.daily_all` runs full pipeline in sequence

**Result Contract:**
- [ ] All composite jobs return structured result (see schema below)
- [ ] Phase composites: `success = (failed == 0)` but always run all children
- [ ] `pipeline.daily_all`: stops on first failed phase, returns phase that failed

**Migration:**
- [ ] Bash scripts in `life-cockpit/scripts/` marked deprecated with comment pointing to composites
- [ ] Job lists maintained in ONE place only (lorchestra composite definitions)
- [ ] After composites are validated, bash scripts are frozen legacy shims - they will NOT receive new jobs

### Constraints

- Composite jobs must be defined in lorchestra job YAML format
- Internal job IDs remain unchanged (no breaking changes)
- Failure in one atomic job should not stop its phase composite (`ingest`, `canonize`, `formation`, `project`, `views`) - graceful degradation
- `pipeline.daily_all` MAY stop on a failed phase (it operates on phases, not atomic jobs)

---

## Result Schema

### Base Schema (Phase Composites)

All phase composites (`ingest`, `canonize`, `formation`, `project`, `views`) MUST return this structure:

```json
{
  "job_id": "pipeline.ingest",
  "success": false,
  "total": 23,
  "succeeded": 21,
  "failed": 2,
  "failures": [
    {"job_id": "ingest_gmail_acct3", "error": "Connection timeout"},
    {"job_id": "validate_stripe_refunds", "error": "Invalid data format"}
  ],
  "duration_ms": 45321
}
```

### Extended Schema (pipeline.daily_all)

`pipeline.daily_all` MUST return the base schema fields PLUS additional phase tracking:

```json
{
  "job_id": "pipeline.daily_all",
  "success": false,
  "total": 4,
  "succeeded": 2,
  "failed": 1,
  "failures": [
    {"job_id": "pipeline.formation", "error": "Phase failed with 2 job errors"}
  ],
  "duration_ms": 120000,
  "phases_completed": ["pipeline.ingest", "pipeline.canonize"],
  "failed_phase": "pipeline.formation",
  "phase_results": {
    "pipeline.ingest": { "success": true, "total": 23, ... },
    "pipeline.canonize": { "success": true, "total": 13, ... },
    "pipeline.formation": { "success": false, "total": 6, ... }
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `phases_completed` | list[str] | Phases that ran (including failed phase) |
| `failed_phase` | str\|null | Phase that caused early stop, or null if all succeeded |
| `phase_results` | dict[str, BaseSchema] | Full result for each phase that ran |

**Success definition:**
| Composite | Success Condition |
|-----------|-------------------|
| `pipeline.ingest` | `failed == 0` (all children succeeded) |
| `pipeline.canonize` | `failed == 0` |
| `pipeline.formation` | `failed == 0` |
| `pipeline.project` | `failed == 0` |
| `pipeline.views` | `failed == 0` |
| `pipeline.daily_all` | All phase composites succeeded |

**Behavior on failure:**
| Composite | On Child Failure |
|-----------|------------------|
| Phase composites | Continue, run all children, report failures at end |
| `pipeline.daily_all` | Stop on first failed phase, do not proceed to next |

---

## Execution Model

### Parallelization

Each composite has internal parallelization opportunities:

**`pipeline.ingest`:**
```
Stage 1 (parallel): ingest_gmail_*, ingest_exchange_*, ingest_dataverse_*,
                    ingest_stripe_*, ingest_google_forms_*
                    ↓ (barrier - all must complete)
Stage 2 (parallel): validate_*
```

**`pipeline.canonize`:**
```
All canonize_* jobs MAY run in parallel (no dependencies between them)
```

**`pipeline.formation`:**
```
Stage 1 (parallel): proj_me_* (measurement events)
                    ↓ (barrier)
Stage 2 (parallel): proj_obs_* (observations depend on ME)
```

**`pipeline.project`:**
```
Stage 1 (parallel): sync_* (BQ → SQLite)
                    ↓ (barrier)
Stage 2 (parallel): file_proj_* (SQLite → Markdown)
```

**`pipeline.views`:**
```
All view_* jobs MAY run in parallel
```

**`pipeline.daily_all`:**
```
Strictly sequential: ingest → canonize → formation → project
```

Implementation note: Parallelization is OPTIONAL for v1. Sequential execution is acceptable. But the spec documents the dependency structure for future optimization.

---

## Plan

### Step 1: Define pipeline.ingest Composite [G0: Code Review]

**Prompt:**

Create composite job `pipeline.ingest` that runs all jobs from `daily_ingest.sh`:

**Stage 1 - Ingestion (parallelizable):**
- Gmail: `ingest_gmail_acct1`, `ingest_gmail_acct2`, `ingest_gmail_acct3`
- Exchange: `ingest_exchange_ben_mensio`, `ingest_exchange_booking_mensio`, `ingest_exchange_info_mensio`
- Dataverse: `ingest_dataverse_contacts`, `ingest_dataverse_sessions`, `ingest_dataverse_reports`
- Stripe: `ingest_stripe_customers`, `ingest_stripe_invoices`, `ingest_stripe_payment_intents`, `ingest_stripe_refunds`
- Google Forms: `ingest_google_forms_intake_01`, `ingest_google_forms_intake_02`, `ingest_google_forms_followup`, `ingest_google_forms_ipip120`

**Stage 2 - Validation (after all ingestion completes):**
- `validate_gmail`, `validate_exchange`, `validate_google_forms`
- `validate_dataverse_contacts`, `validate_dataverse_sessions`, `validate_dataverse_reports`
- `validate_stripe_customers`, `validate_stripe_invoices`, `validate_stripe_payment_intents`, `validate_stripe_refunds`

Must return structured result per schema.

**Outputs:**
- Job definition for `pipeline.ingest`

---

### Step 2: Define pipeline.canonize Composite [G0: Code Review]

**Prompt:**

Create composite job `pipeline.canonize` that runs all jobs from `daily_canonize.sh`:

**All parallelizable:**
- Email: `canonize_gmail_jmap`, `canonize_exchange_jmap`
- Dataverse: `canonize_dataverse_contacts`, `canonize_dataverse_sessions_clinical`, `canonize_dataverse_sessions_transcripts`, `canonize_dataverse_sessions_notes`, `canonize_dataverse_sessions_summaries`, `canonize_dataverse_reports`
- Stripe: `canonize_stripe_customers`, `canonize_stripe_invoices`, `canonize_stripe_payments`, `canonize_stripe_refunds`
- Forms: `canonize_google_forms`

Must return structured result per schema.

**Outputs:**
- Job definition for `pipeline.canonize`

---

### Step 3: Define pipeline.formation Composite [G0: Code Review]

**Prompt:**

Create composite job `pipeline.formation` that runs all jobs from `daily_formation.sh`:

**Stage 1 - Measurement Events (parallelizable):**
- `proj_me_intake_01`, `proj_me_intake_02`, `proj_me_followup`

**Stage 2 - Observations (after ME completes, parallelizable):**
- `proj_obs_intake_01`, `proj_obs_intake_02`, `proj_obs_followup`

Must return structured result per schema.

**Outputs:**
- Job definition for `pipeline.formation`

---

### Step 4: Define pipeline.project Composite [G0: Code Review]

**Prompt:**

Create composite job `pipeline.project` that runs all jobs from `daily_local_projection.sh`:

**Stage 1 - BQ → SQLite Sync (parallelizable):**
- `sync_proj_clients`, `sync_proj_sessions`, `sync_proj_transcripts`
- `sync_proj_clinical_documents`, `sync_proj_form_responses`, `sync_proj_contact_events`
- `sync_measurement_events`, `sync_observations`

**Stage 2 - SQLite → Markdown Files (after sync completes, parallelizable):**
- `file_proj_clients`, `file_proj_transcripts`
- `file_proj_session_notes`, `file_proj_session_summaries`, `file_proj_reports`

Must return structured result per schema.

**Outputs:**
- Job definition for `pipeline.project`

---

### Step 5: Define pipeline.views Composite [G0: Code Review]

**Prompt:**

Create composite job `pipeline.views` that runs all jobs from `create_views.sh`:

**All parallelizable:**
- `view_proj_clients`, `view_proj_sessions`, `view_proj_transcripts`
- `view_proj_clinical_documents`, `view_proj_form_responses`, `view_proj_contact_events`

Must return structured result per schema.

**Outputs:**
- Job definition for `pipeline.views`

---

### Step 6: Define pipeline.daily_all Composite [G0: Code Review]

**Prompt:**

Create composite job `pipeline.daily_all` that runs the full daily pipeline:

**Strictly sequential, stop on failure:**
1. `pipeline.ingest` → if failed, stop and return
2. `pipeline.canonize` → if failed, stop and return
3. `pipeline.formation` → if failed, stop and return
4. `pipeline.project` → if failed, stop and return

(Note: `pipeline.views` is NOT included - it's a one-time setup, not daily)

Must return extended result schema (see Result Schema section above).

**Outputs:**
- Job definition for `pipeline.daily_all`

---

### Step 7: Deprecate Bash Scripts [G1: Code Review]

**Prompt:**

Add deprecation notice to each script in `life-cockpit/scripts/`:

```bash
#!/bin/bash
# DEPRECATED: This script is replaced by lorchestra composite jobs.
# Use: lorchestra run pipeline.ingest
# Or:  life pipeline ingest (after life-cli integration)
#
# This script will be removed in a future release.
# See: /workspace/lorchestra/.specwright/specs/pipeline-composite-jobs.md
```

Do NOT update the job lists in these scripts. The composite jobs are now the source of truth.

**Outputs:**
- `life-cockpit/scripts/daily_ingest.sh` (deprecation comment added)
- `life-cockpit/scripts/daily_canonize.sh` (deprecation comment added)
- `life-cockpit/scripts/daily_formation.sh` (deprecation comment added)
- `life-cockpit/scripts/daily_local_projection.sh` (deprecation comment added)
- `life-cockpit/scripts/create_views.sh` (deprecation comment added)

---

### Step 8: Add Snapshot Tests [G1: Pre-Release]

**Prompt:**

Create tests that verify composite job child lists match expectations:

```python
def test_pipeline_ingest_child_jobs():
    """Ensure pipeline.ingest includes all expected child jobs."""
    expected = {
        "ingest_gmail_acct1", "ingest_gmail_acct2", "ingest_gmail_acct3",
        "ingest_exchange_ben_mensio", "ingest_exchange_booking_mensio", "ingest_exchange_info_mensio",
        "ingest_dataverse_contacts", "ingest_dataverse_sessions", "ingest_dataverse_reports",
        "ingest_stripe_customers", "ingest_stripe_invoices", "ingest_stripe_payment_intents", "ingest_stripe_refunds",
        "ingest_google_forms_intake_01", "ingest_google_forms_intake_02", "ingest_google_forms_followup", "ingest_google_forms_ipip120",
        "validate_gmail", "validate_exchange", "validate_google_forms",
        "validate_dataverse_contacts", "validate_dataverse_sessions", "validate_dataverse_reports",
        "validate_stripe_customers", "validate_stripe_invoices", "validate_stripe_payment_intents", "validate_stripe_refunds",
    }
    actual = get_composite_child_jobs("pipeline.ingest")
    assert actual == expected, f"Missing: {expected - actual}, Extra: {actual - expected}"


def get_composite_child_jobs(composite_id: str) -> set[str]:
    """Test helper: Parse pipeline.yaml and return child job IDs for a composite.

    This is a test utility, not a production API. It parses the YAML definition
    directly to extract the list of child jobs.
    """
    # Implementation: load YAML, extract job list from composite definition
    ...
```

Similar tests for all composites.

This prevents accidentally dropping jobs when refactoring.

**Commands:**

```bash
pytest tests/test_pipeline_composites.py -v
```

**Outputs:**
- `tests/test_pipeline_composites.py` (new)

---

### Step 9: Integration Test [G2: Pre-Release]

**Prompt:**

Test each composite job in dry-run mode:

```bash
lorchestra run pipeline.ingest --dry-run
lorchestra run pipeline.canonize --dry-run
lorchestra run pipeline.formation --dry-run
lorchestra run pipeline.project --dry-run
lorchestra run pipeline.views --dry-run
lorchestra run pipeline.daily_all --dry-run
```

Verify:
- All internal jobs are listed
- Order is correct (stages respected)
- Result schema is returned
- No missing jobs compared to bash scripts

**Outputs:**
- Test results showing all jobs discovered

---

## Files to Touch

| File | Action |
|------|--------|
| `lorchestra/jobs/pipeline.yaml` | CREATE |
| `tests/test_pipeline_composites.py` | CREATE |
| `life-cockpit/scripts/daily_ingest.sh` | UPDATE (deprecation notice) |
| `life-cockpit/scripts/daily_canonize.sh` | UPDATE (deprecation notice) |
| `life-cockpit/scripts/daily_formation.sh` | UPDATE (deprecation notice) |
| `life-cockpit/scripts/daily_local_projection.sh` | UPDATE (deprecation notice) |
| `life-cockpit/scripts/create_views.sh` | UPDATE (deprecation notice) |

---

## Repository

**Branch:** `feat/pipeline-composites`

**Merge Strategy:** squash
