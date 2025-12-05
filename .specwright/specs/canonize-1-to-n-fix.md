---
version: "0.1"
tier: C
title: Fix 1:N Raw→Canonical Object Mapping
owner: benthepsychologist
goal: Fix canonization to support one raw object producing multiple canonical objects
labels: [bugfix, canonization]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-04T19:04:37.947096+00:00
updated: 2025-12-04T19:04:37.947096+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/canonize-1-to-n-fix"
---

# Fix 1:N Raw→Canonical Object Mapping

## Objective

> Fix the canonization system to support 1:N mappings where one raw object (e.g., session) can produce multiple canonical objects (clinical_session, session_transcript, etc.) without idem_key collisions.

## Acceptance Criteria

- [ ] CI green (lint + unit)
- [ ] 2-0-0 transforms are loadable via `can registry sync`
- [ ] Canonical idem_keys use format `raw_idem_key#canonical_object_type`
- [ ] Re-canonization correctly produces separate objects per transform

## Context

### Background

The canonization system assumes 1:1 mapping between raw and canonical objects. This is incorrect - one session raw object should produce:
- `clinical_session` - session metadata
- `session_transcript` - transcript content (if present)
- Future: `clinical_document` (note), `clinical_document` (summary)

Currently all these share the same idem_key, causing later canonizations to overwrite earlier ones. When running canonize_dataverse_transcripts after canonize_dataverse_sessions, the 132 session_transcripts overwrote 132 clinical_sessions.

Additionally, the 2-0-0 transforms are not in the lock.json file, causing "Failed to load transform" errors.

### Constraints

- Must not break existing 1:1 canonizations (contacts, reports)
- Must support re-canonization when raw data updates
- Transcript processing needs limit to prevent timeouts

## Plan

### Step 1: Update lock.json with 2-0-0 entries

**Files:**
- `.canonizer/lock.json`

Add entries for:
- `contact/dataverse_to_canonical@2-0-0`
- `clinical_session/dataverse_to_canonical@2-0-0`
- `clinical_document/dataverse_to_canonical@2-0-0`
- `session_transcript/dataverse_to_canonical@2-0-0`
- Corresponding 2-0-0 schemas

### Step 2: Run can registry sync

```bash
cd /workspace/lorchestra
source .venv/bin/activate
can registry sync
```

### Step 3: Update query_objects_for_canonization JOIN

**File:** `lorchestra/job_runner.py` (lines 105-186)

Change join condition to match new idem_key format:
```python
canonical_object_type = canonical_schema.split("/")[1]
join_condition = f"CONCAT(r.idem_key, '#{canonical_object_type}') = c.idem_key"
```

### Step 4: Add limit to transcripts job

**File:** `lorchestra/jobs/definitions/canonize_dataverse_transcripts.json`

Add `"options": {"limit": 10}` to prevent timeouts.

### Step 5: Delete old canonical objects and re-run

```bash
# Delete old format
DELETE FROM canonical_objects WHERE idem_key NOT LIKE '%#%'

# Re-run all canonization jobs
lorchestra run canonize_dataverse_contacts
lorchestra run canonize_dataverse_sessions
lorchestra run canonize_dataverse_reports
lorchestra run canonize_dataverse_transcripts
```

### Step 6: Verify counts

Expected:
- contact: 626+
- clinical_session: 811
- session_transcript: ~132
- clinical_document: 34

## Models & Tools

**Tools:** bash, can, lorchestra

**Models:** claude-sonnet

## Repository

**Branch:** `feat/canonize-1-to-n-fix`

**Merge Strategy:** squash
