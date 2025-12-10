---
version: "0.1"
tier: C
title: final-form Processing V2 - Fix Data Model
owner: benthepsychologist
goal: Fix measurement_events and observations tables to use correct FHIR-aligned grain (1 row per submission, not 1 row per measure)
labels: [formation, final-form, schema-fix, data-model]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-09T20:00:00.000000+00:00
updated: 2025-12-09T20:00:00.000000+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "fix/measurement-schema"
---

# final-form Processing V2 - Fix Data Model

## Objective

Fix the data model for `measurement_events` and `observations` tables to use FHIR-aligned semantics:

**Current (WRONG):**
- `measurement_events` = 1 row per **scored measure** (PHQ-9, GAD-7) → ~2,208 rows for 232 submissions
- `observations` = 1 row per **item/scale** within a measure → ~31,000 rows

**Target (CORRECT):**
- `measurement_events` = 1 row per **form submission** (the measurement event) → 232 rows
- `observations` = 1 row per **scored construct** (PHQ-9 total, GAD-7 total) with `components` JSON array → ~1,108 rows

This is a clean fix, not a migration. We drop the tables, update the processor, and rerun.

## Acceptance Criteria

- [ ] `measurement_events` table has 1 row per form submission
- [ ] `observations` table has 1 row per scored measure with `components` JSON array
- [ ] All existing form data repopulated correctly
- [ ] `proj_contact_events` updated to use new schema
- [ ] CI green (lint + unit tests)
- [ ] Counts verified: ~232 measurement_events, ~1,108 observations (unique submission × measure)

## Context

### FHIR Alignment

| Concept | FHIR Equivalent | Our Table | Grain |
|---------|-----------------|-----------|-------|
| Measurement Event | DiagnosticReport | `measurement_events` | 1 per submission |
| Observation | Observation | `observations` | 1 per scored construct |
| Component | Observation.component | `observations.components` | JSON array |

### Why This Matters

The current model treats "PHQ-9 score" as an "event" when it's actually an "observation within an event". This breaks:
- Event timelines (232 submissions show as 2,208 events)
- FHIR interoperability
- Future extensibility for labs/devices

### Current Data State

```
measurement_events: 2,208 rows (WRONG - these are observations)
  - 232 unique form_submission_ids
  - ~9.5 measures per submission average
  - Has duplicates: 1,100 duplicate (submission, measure) pairs

observations: 30,998 rows (WRONG - these are components)
  - Item-level data (phq9_item1, phq9_total, etc.)
```

### What final-form Actually Produces

final-form's `Pipeline.process()` returns a `ProcessingResult` with:
- `events: list[MeasurementEvent]` - **one per measure** (PHQ-9, GAD-7, etc.)
- Each `MeasurementEvent` has:
  - `measure_id`, `measure_version`, `subject_id`, `timestamp`
  - `source`: form_id, form_submission_id, binding_id, binding_version
  - `observations: list[Observation]` - items and scales (phq9_item1, phq9_total)

So final-form's "MeasurementEvent" is actually what we should call an "Observation", and final-form's "Observation" is a "Component".

---

## New Schema

### measurement_events (1 row per submission)

| Column | Type | Description |
|--------|------|-------------|
| `idem_key` | STRING | PK: `{canonical_idem_key}` - deterministic, same as measurement_event_id |
| `measurement_event_id` | STRING | Same as idem_key. Deterministic from canonical_idem_key. |
| `subject_id` | STRING | Client email (from respondent) |
| `subject_contact_id` | STRING | Canonical contact_id. **NULL for v2** - will be populated by future projection. |
| `event_type` | STRING | `'form'` (future: `'lab'`, `'device'`) |
| `event_subtype` | STRING | Binding ID: `'intake_01'`, `'followup'`, etc. |
| `occurred_at` | TIMESTAMP | When form was submitted |
| `received_at` | TIMESTAMP | When we processed it |
| `source_system` | STRING | `'google_forms'` |
| `source_entity` | STRING | `'form_response'` |
| `source_id` | STRING | `form_submission_id` from Google |
| `canonical_object_id` | STRING | `idem_key` from canonical_objects |
| `form_id` | STRING | Google form ID |
| `binding_id` | STRING | Form binding used |
| `binding_version` | STRING | Binding version |
| `metadata` | JSON | Additional context |
| `correlation_id` | STRING | Run ID for tracing |
| `processed_at` | TIMESTAMP | Formation timestamp |
| `created_at` | TIMESTAMP | Row creation |

**Idem key:** `{canonical_idem_key}` (one per submission)

### observations (1 row per scored construct)

| Column | Type | Description |
|--------|------|-------------|
| `idem_key` | STRING | PK: `{measurement_event_idem_key}:{measure_code}` |
| `observation_id` | STRING | Stable UUID for this observation |
| `measurement_event_id` | STRING | FK to measurement_events.measurement_event_id |
| `subject_id` | STRING | Denormalized for convenience |
| `measure_code` | STRING | `'phq9'`, `'gad7'`, `'fscrs'`, etc. |
| `measure_version` | STRING | `'1.0.0'` |
| `value_numeric` | FLOAT64 | Total score |
| `value_text` | STRING | For qualitative results (nullable) |
| `unit` | STRING | `'score'` (future: `'mmol/L'`) |
| `severity_code` | STRING | `'none'`, `'mild'`, `'moderate'`, `'severe'` |
| `severity_label` | STRING | `'Moderate depression'` |
| `components` | JSON | Array of item/subscale details |
| `metadata` | JSON | Additional context |
| `correlation_id` | STRING | Run ID for tracing |
| `processed_at` | TIMESTAMP | Formation timestamp |
| `created_at` | TIMESTAMP | Row creation |

**Idem key:** `{measurement_event_idem_key}:{measure_code}` (one per measure per submission)

### components JSON structure

```json
[
  {
    "code": "phq9_item1",
    "kind": "item",
    "value": 2,
    "value_type": "integer",
    "label": null,
    "raw_answer": "More than half the days",
    "position": 1,
    "missing": false
  },
  {
    "code": "phq9_total",
    "kind": "scale",
    "value": 14,
    "value_type": "integer",
    "label": "Moderate",
    "raw_answer": null,
    "position": null,
    "missing": false
  }
]
```

---

## Plan

### Step 1: Drop Existing Tables

Drop the incorrectly-shaped tables in BigQuery:

```bash
bq rm -f local-orchestration:events_dev.measurement_events
bq rm -f local-orchestration:events_dev.observations
```

### Step 2: Update FinalFormProcessor

**File:** `lorchestra/processors/final_form.py`

**Key changes:**

1. **Create ONE measurement_event per canonical record (submission):**
   ```python
   def _submission_to_measurement_event(self, record, payload, form_response, result, correlation_id):
       """Create one measurement_event row for the submission."""
       # Use first event to get binding info (all events share same submission)
       first_event = result.events[0] if result.events else None
       return {
           "idem_key": record["idem_key"],  # canonical idem_key = 1 per submission
           "measurement_event_id": self._generate_measurement_event_id(record),
           "subject_id": form_response.get("subject_id"),
           "event_type": "form",
           "event_subtype": first_event.source.binding_id if first_event else None,
           "occurred_at": form_response.get("timestamp"),
           "source_system": "google_forms",
           "source_entity": "form_response",
           "source_id": form_response.get("form_submission_id"),
           "canonical_object_id": record["idem_key"],
           "form_id": form_response.get("form_id"),
           "binding_id": first_event.source.binding_id if first_event else None,
           "binding_version": first_event.source.binding_version if first_event else None,
           ...
       }
   ```

2. **Create ONE observation per final-form MeasurementEvent:**
   ```python
   def _measure_to_observation(self, event, measurement_event_idem_key, measurement_event_id, correlation_id):
       """Create one observation row per scored measure, with components array."""
       # Extract severity from scale observations
       severity_label = self._extract_severity(event)

       # Build components array from event.observations
       components = [
           {
               "code": obs.code,
               "kind": obs.kind,
               "value": obs.value,
               "value_type": obs.value_type,
               "label": obs.label,
               "raw_answer": obs.raw_answer,
               "position": obs.position,
               "missing": obs.missing,
           }
           for obs in event.observations
       ]

       # Get total score from _total scale observation
       total_score = self._extract_total_score(event)

       return {
           "idem_key": f"{measurement_event_idem_key}:{event.measure_id}",
           "observation_id": event.measurement_event_id,  # Reuse final-form's UUID
           "measurement_event_id": measurement_event_id,
           "subject_id": event.subject_id,
           "measure_code": event.measure_id,
           "measure_version": event.measure_version,
           "value_numeric": total_score,
           "severity_label": severity_label,
           "components": json.dumps(components),
           ...
       }
   ```

3. **Update main processing loop:**
   ```python
   for record in records:
       result = pipeline.process(form_response)

       # ONE measurement_event per submission
       me_row = self._submission_to_measurement_event(record, payload, form_response, result, correlation_id)
       measurement_rows.append(me_row)

       # ONE observation per scored construct
       for event in result.events:
           obs_row = self._measure_to_observation(event, me_row["idem_key"], me_row["measurement_event_id"], correlation_id)
           observation_rows.append(obs_row)
   ```

### Step 3: Update job_runner Upsert Methods

**File:** `lorchestra/job_runner.py`

Update `upsert_measurements()` and `upsert_observations()` for new column sets.

**upsert_measurements changes:**
- Add: `event_type`, `event_subtype`, `subject_contact_id`, `occurred_at`, `received_at`, `source_system`, `source_entity`, `source_id`, `canonical_object_id`, `metadata`
- Remove: `canonical_idem_key`, `measure_id`, `measure_version`, `timestamp`

**upsert_observations changes:**
- Add: `measurement_event_id`, `measure_code`, `value_numeric`, `value_text`, `unit`, `severity_code`, `severity_label`, `components`, `metadata`
- Remove: `measurement_idem_key`, `code`, `kind`, `value`, `value_type`, `label`, `raw_answer`, `position`, `missing`

### Step 4: Update Incremental Query

**File:** `lorchestra/job_runner.py`

Update `query_canonical_for_formation()`:
- Join on `measurement_events.canonical_object_id = canonical_objects.idem_key`
- Simpler than current compound key matching

### Step 5: Update Tests

**File:** `tests/test_final_form_processor.py`

Update tests for new output shape:
- Mock storage client expectations for new column sets
- Verify 1 measurement_event per submission
- Verify observations have components JSON

### Step 6: Rerun Final-Form Jobs

```bash
lorchestra run form_intake_01
lorchestra run form_intake_02
lorchestra run form_followup
```

### Step 7: Update proj_contact_events

**File:** `lorchestra/sql/projections.py`

Update `PROJ_CONTACT_EVENTS` to use new `measurement_events`:
- Source: `measurement_events` (now 1 row per submission)
- `event_type = 'form'`
- `event_name = event_subtype` (e.g., 'intake_01', 'followup')
- `event_timestamp = occurred_at`
- `source_system = 'measurement_events'`
- `source_record_id = measurement_event_id`

### Step 8: Verify Counts

```sql
-- Should be ~232 (unique form submissions)
SELECT COUNT(*) FROM measurement_events;

-- Should be ~1,108 (unique submission × measure pairs)
SELECT COUNT(*) FROM observations;

-- Verify no duplicates
SELECT idem_key, COUNT(*) FROM measurement_events GROUP BY idem_key HAVING COUNT(*) > 1;
SELECT idem_key, COUNT(*) FROM observations GROUP BY idem_key HAVING COUNT(*) > 1;
```

---

## Files to Modify

| File | Change |
|------|--------|
| `lorchestra/processors/final_form.py` | Restructure to emit 1 event per submission, 1 observation per measure |
| `lorchestra/job_runner.py` | Update `upsert_measurements()` and `upsert_observations()` for new schemas |
| `lorchestra/sql/projections.py` | Update `PROJ_CONTACT_EVENTS` to use new measurement_events |
| `tests/test_final_form_processor.py` | Update tests for new output shape |

---

## Rollback Plan

If something goes wrong:
1. Tables are derived from canonical_objects - no data loss
2. Restore old processor code from git
3. Drop tables and rerun with old code

---

## Models & Tools

**Tools:** bash, pytest, ruff, bq

## Repository

**Branch:** `fix/measurement-schema`

**Merge Strategy:** squash
