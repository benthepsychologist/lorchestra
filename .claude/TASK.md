---
id: e011-04-pm-sheets-projection
title: "PM Sheets Projection — BQ Views and Schema Sheets"
tier: B
owner: benthepsychologist
goal: "BQ view SQL: view_pm_projects (project_id, name, status, owner, start_date, target_end_date, ...)"
branch: feat/pm-projection
status: in-progress
repo:
  name: lorchestra
  url: /workspace/lorchestra
created: 2026-02-11T21:24:26Z
updated: 2026-02-11T21:30:00Z
---

# e011-04-pm-sheets-projection: PM Sheets Projection — BQ Views and Schema Sheets

**Epic:** e011-pm-system
**Branch:** `feat/pm-projection`
**Tier:** B

## Schema Reference

**Depends on**: e011-01-workman-pm-domain, e011-02-lorchestra-pm-jobs

**Schema reference**: See [../resources.md](../resources.md) §5 (WAL Event Shapes) and §6 (BQ View SQL) for the complete BigQuery view definitions. Section §2 (PM Op Request Schemas) defines the field structure for all PM entities that these views project.

## Objective

Create BigQuery views and projection tables that reconstruct current PM entity state from WAL events and establish Schema Sheets projection for all PM entities (projects, work items, deliverables, ops streams, artifacts) plus AKM foundation entities (atoms, links) with **ownership metadata**. This enables PM data consumption in Google Sheets through lorchestra's standard Storacle Plan interface while preserving the AKM ownership model needed for permission gating.

The projection layer serves as the read path for PM data after workman (write path) and lorchestra PM jobs (orchestration) are operational. Atoms and links are stored in persistent tables (atom.objects, atom.links) with owner fields that control who can CRUD each entity. Schema Sheets follow the three-column convention (row_id, row_version, row_state) and support the full PM domain model including the AKM graph foundation with ownership gates.

## Problem

1. PM data written via workman WAL events has no read path for analysis and review
2. Google Sheets consumption requires structured projection from event stream to current state
3. PM entities need consistent Schema Sheet layout for user consumption and future sync-back
4. AKM graph entities (atoms, links) need persistent tables with ownership metadata to support permission gating
5. Workman-owned atoms/links must be distinguished from knowledge-owned atoms/links for CRUD enforcement
6. Multiple PM entity types require individual job configs and a unified pipeline job for batch processing
7. Stale link handling requires snapshot versioning to track cascading changes to atoms

## Current Capabilities

### kernel.surfaces

```yaml
- surface: cli
  entrypoints:
    - "lorchestra run <job_id>"
    - "lorchestra list --type ingest"
    - "lorchestra query recent-jobs --limit 20"
- surface: python_api
  entrypoints:
    - "from lorchestra.job_runner import run_job"
```

### modules

```yaml
- name: public_surface
  provides: ['LorchestraConfig', 'load_config()', 'get_lorchestra_home()']
- name: cli
  provides: ['lorchestra run', 'lorchestra init', 'lorchestra jobs list', 'lorchestra jobs show', 'lorchestra stats canonical', 'lorchestra stats raw', 'lorchestra stats jobs', 'lorchestra query', 'lorchestra sql']
- name: job_runner
  provides: ['run_job(job_id, **options)', 'BigQueryStorageClient', 'load_job_spec(job_id)']
- name: callable
  provides: ['CallableDispatch', 'CallableResult']
- name: handlers
  provides: ['BaseHandler', 'ComputeHandler', 'OrchestrationHandler']
- name: sql
  provides: ['get_projection_sql(projection_name)', 'PROJECTIONS dict']
- name: job_definitions
  provides: ['YAML v2.0 job specs organized by type (pm, pipeline, etc.)']
```

### layout

```yaml
- path: lorchestra/sql/
  role: "Projection SQL definitions, get_projection_sql()"
- path: lorchestra/jobs/definitions/
  role: "YAML v2.0 job specs organized by type"
```

## Proposed build_delta

```yaml
target: "projects/lorchestra/lorchestra.build.yaml"
summary: "Add PM entity BQ views (projects, work items, deliverables, opsstreams, artifacts) and pm.sheets job for Sheets consumption. Views fold WAL events into current state with hierarchy resolution."

adds:
  layout:
    - path: "lorchestra/sql/pm_projections.py"
      role: "PM entity projection SQL definitions: view_pm_projects, view_pm_work_items, view_pm_deliverables, view_pm_opsstreams, view_pm_artifacts"
    - path: "lorchestra/jobs/definitions/pm/pm.sheets.yaml"
      role: "PM sheets projection job: reads PM views, emits storacle.sheets.write plan for Google Sheets"
    - path: "lorchestra/jobs/definitions/pipeline/pm_views.yaml"
      role: "Pipeline job running pm.sheets projection"
  modules: []
  kernel_surfaces: []
modifies:
  modules:
    - name: sql
      change: "Import PM_PROJECTIONS from pm_projections.py into main PROJECTIONS dict"
    - name: job_definitions
      provides_add:
        - "pm/pm.sheets.yaml"
        - "pipeline/pm_views.yaml"
removes: {}
```

## Acceptance Criteria

### PM Entity Views (Fold WAL to Current State)

- [ ] BQ view: view_pm_projects — projects with direct fields (name, description, owner, status, target_end_date) + row_id, row_version, row_state
- [ ] BQ view: view_pm_work_items — work items with direct fields (title, kind, state, priority, severity, assignees, labels, due_at) + project_id, deliverable_id, opsstream_id, parent_id + effective_project_id (resolved via deliverable hierarchy per ADR-003)
- [ ] BQ view: view_pm_deliverables — deliverables with direct fields (name, description, status, acceptance_criteria, due_date) + project_id + row_id, row_version, row_state
- [ ] BQ view: view_pm_opsstreams — ops streams with direct fields (name, type, owner, status, description) + row_id, row_version, row_state
- [ ] BQ view: view_pm_artifacts — artifacts with direct fields (title, kind, status, content, content_ref, delivered_via, tags) + container FKs (work_item_id, deliverable_id, project_id, opsstream_id, contact_ref) + row_id, row_version, row_state
- [ ] All views fold WAL events using ARRAY_AGG with IGNORE NULLS and latest-wins semantics (per resources.md §6)
- [ ] All views query from `wal_events` table (storacle WAL convention: aggregate_id, aggregate_type, event_type, payload, event_id, timestamp)
- [ ] All views include row_id (= aggregate_id), row_version (= latest event_id), row_state (= 'synced')

### Sheets Integration

- [ ] PM job: lorchestra/jobs/definitions/pm/pm.sheets.yaml queries all BQ views and emits storacle.sheets.write plan
- [ ] Pipeline job: lorchestra/jobs/definitions/pipeline/pm_views.yaml runs pm.sheets projection
- [ ] Schema Sheets have fixed column layout matching BQ view output
- [ ] lorchestra run pm_views produces PM data in Google Sheets via storacle

## Constraints

- All writes go through storacle: WAL via wal.append, Sheets via sheets.write
- Schema Sheet columns follow row_id, row_version, row_state convention
- Strategy is replace (full refresh) for v1 — not incremental
- BQ views fold WAL events into current state (latest event per aggregate_id)
- Column set should be consistent with pm.fields.yaml (field registry is normative for what appears)
- Effective container fields resolved at read time via JOINs per ADR-003 (no cascade writes to WAL)
- Views query `wal_events` table (storacle WAL)

---

---

## Architectural Decision: AKM Deferred to e013

**ADR-005: AKM Persistent Tables Deferred**

**Date**: 2026-02-17
**Status**: Accepted
**Context**: e011-04 scope clarification

### Decision

e011-04 focuses on **PM entity projections only** (view_pm_projects, view_pm_work_items, etc.). Persistent AKM tables (`atom.objects`, `atom.links`) and the projection job (`pm.projection.yaml`) are **deferred to e013 (AKM Surfaces)**.

### Rationale

1. **Scope clarity**: e011 is about operational PM (create/update/view). AKM is a future knowledge layer.
2. **Risk reduction**: Snapshot versioning and ownership gating are complex; they should be designed when we have a clearer use case.
3. **Faster shipping**: PM projections to Sheets don't require persistent atom tables—they work directly from wal_events views.
4. **Clean separation**: e011 delivers PM views → Sheets. e013 adds AKM as a first-class surfaces layer with atoms, links, curation.

### What's In Scope (e011-04)

- PM entity views (fold wal_events into current state)
- Effective field resolution via JOINs (ADR-003 hierarchy)
- pm.sheets job (query views, write to Sheets)

### What's Deferred (e013+)

- Persistent atom.objects table with ownership gating
- Persistent atom.links table with snapshot versioning
- pm.projection.yaml job (domain_events → atom tables)
- AKM-specific query surfaces and curation tooling

---

## PM Entity Views: Design & Implementation

### Objective
Create clean BigQuery views for PM entities that fold `wal_events` into current state. Each view uses ARRAY_AGG to take the latest non-null value per field. Views include **effective fields** that resolve container hierarchy at read time per ADR-003 (no cascade writes).

### Data Flow
```
workman compiles intent
    ↓
lorchestra pm.exec writes to wal_events (storacle WAL)
    ↓
wal_events contains: aggregate_id, aggregate_type, event_type, payload (JSON), event_id, timestamp
    ↓
BQ views (view_pm_projects, view_pm_work_items, etc.) fold wal_events into current state
    ↓
pm.sheets job queries views and writes to Google Sheets via storacle.sheets.write
```

### Files to Create
- `lorchestra/sql/pm_projections.py` — PM entity view SQL definitions
- `lorchestra/jobs/definitions/pm/pm.sheets.yaml` — Job that queries views, writes to Sheets
- `lorchestra/jobs/definitions/pipeline/pm_views.yaml` — Pipeline wrapper for batch runs

### Files to Modify
- `lorchestra/sql/__init__.py` — Import PM_PROJECTIONS into PROJECTIONS registry

### Implementation Notes

**Field Folding Pattern** (from resources.md §6):
```sql
ARRAY_AGG(JSON_VALUE(payload, '$.field_name') IGNORE NULLS
  ORDER BY timestamp DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS field_name
```
This takes the latest non-null value for each field across all events for an aggregate.

**Hierarchy Resolution (ADR-003)**:
- Work items may not have `project_id` directly (only via deliverable)
- Views use LEFT JOINs to resolve effective container fields
- Example: `effective_project_id = COALESCE(wi.project_id, del.project_id)`

**For all views**:
- Query from `wal_events` table (aggregate_type-filtered)
- Include row_id (= aggregate_id), row_version (= latest event_id), row_state ('synced')
- Use JSON_VALUE for scalar fields, JSON_QUERY for arrays
- Follow naming convention: `view_pm_<entity_type>`

**For artifacts specifically**:
- Truncate content to 500 chars (Sheets cell limit)
- Include both content (WAL-native) and content_ref (delivered copy)

### Effective Field Resolution (ADR-003)

Per ADR-003, container hierarchy is resolved at **read time** via JOINs in the BQ views. Work items may not have `project_id` directly in the WAL—it may only exist in the associated deliverable. The views compute effective fields:

**Example: view_pm_work_items hierarchy resolution**
```sql
WITH work_items_folded AS (
  -- Fold work_item events to get direct fields (project_id, deliverable_id, etc.)
  -- ...
),
with_effective_fields AS (
  SELECT
    wi.*,
    -- If work_item has no project_id, get it from deliverable
    COALESCE(wi.project_id, del.project_id) AS effective_project_id,
    -- If work_item has no opsstream_id, get it from project
    COALESCE(wi.opsstream_id, proj.opsstream_id) AS effective_opsstream_id
  FROM work_items_folded wi
  LEFT JOIN view_pm_deliverables del ON wi.deliverable_id = del.deliverable_id
  LEFT JOIN view_pm_projects proj ON COALESCE(wi.project_id, del.project_id) = proj.project_id
)
SELECT * FROM with_effective_fields
```

This means:
- `project_id` = what's in the WAL event (may be NULL)
- `effective_project_id` = current truth from hierarchy (always resolved)
- Schema Sheets show `effective_*` fields (derived, not raw WAL fields)
- If a deliverable moves to a different project, work items' effective project updates immediately (no WAL cascades)

### Verification
- `python -c "from lorchestra.sql.pm_projections import PM_PROJECTIONS; print(len(PM_PROJECTIONS))"` → 5 projections
- `python -c "from lorchestra.sql import get_projection_sql; print('OK' if get_projection_sql('view_pm_projects') else 'FAIL')"` → OK
- `ruff check lorchestra/sql/` → clean

## PM Sheets Projection Job

### Objective
Create a YAML v2.0 job definition that queries all PM BQ views and emits a storacle.sheets.write plan. The job uses the call op to invoke BigQuery query and storacle operations via dispatch_callable, enabling single-command refresh of all PM data to Google Sheets.

### Files to Touch
- `lorchestra/jobs/definitions/pm/pm.sheets.yaml` (create) — PM sheets projection job querying all BQ views and emitting sheets.write plan

### Implementation Notes

Follow YAML v2.0 format with multi-step job:

```yaml
job_id: pm.sheets
version: '2.0'
description: "Project PM entity data to Google Sheets via storacle"

steps:
  - step_id: query.projects
    op: call
    params:
      callable: bq_reader
      query: "SELECT * FROM `{project}.{dataset}.view_pm_projects`"
      project: "@payload.project"
      dataset: "@payload.dataset"

  - step_id: query.work_items
    op: call
    params:
      callable: bq_reader
      query: "SELECT * FROM `{project}.{dataset}.view_pm_work_items`"
      project: "@payload.project"
      dataset: "@payload.dataset"

  - step_id: query.deliverables
    op: call
    params:
      callable: bq_reader
      query: "SELECT * FROM `{project}.{dataset}.view_pm_deliverables`"
      project: "@payload.project"
      dataset: "@payload.dataset"

  - step_id: query.opsstreams
    op: call
    params:
      callable: bq_reader
      query: "SELECT * FROM `{project}.{dataset}.view_pm_opsstreams`"
      project: "@payload.project"
      dataset: "@payload.dataset"

  - step_id: query.artifacts
    op: call
    params:
      callable: bq_reader
      query: "SELECT * FROM `{project}.{dataset}.view_pm_artifacts`"
      project: "@payload.project"
      dataset: "@payload.dataset"

  - step_id: build.plan
    op: plan.build
    params:
      items:
        projects: "@run.query.projects.items"
        work_items: "@run.query.work_items.items"
        deliverables: "@run.query.deliverables.items"
        opsstreams: "@run.query.opsstreams.items"
        artifacts: "@run.query.artifacts.items"
      method: sheets.write
      target_sheet_id: "@payload.sheet_id"
      strategy: replace

  - step_id: submit
    op: storacle.submit
    params:
      plan: "@run.build.plan.plan"
```

Each query step reads from the corresponding BQ view using the internal `bq_reader` callable. The build.plan step collects all results and builds a plan for sheets.write. The final submit step submits the plan to storacle, which handles the actual Sheets write operation.

### Verification
- `lorchestra jobs list` → shows pm.sheets job
- `lorchestra run pm.sheets --payload '{"sheet_id": "..."}' --dry-run` → shows plan without writing
- `lorchestra run pm.sheets --payload '{"sheet_id": "..."}'` → writes all PM data to Sheets
- `ruff check lorchestra/` → clean (no new Python code)

## PM Views Pipeline Job

### Objective
Create a pipeline job that runs the pm.sheets projection job. This provides a unified command for batch refreshing PM data in Sheets.

### Files to Touch
- `lorchestra/jobs/definitions/pipeline/pm_views.yaml` (create) — PM views pipeline job

### Implementation Notes

Follow YAML v2.0 format:

```yaml
job_id: pipeline.pm_views
version: '2.0'
description: "Pipeline: refresh all PM data in Google Sheets"

steps:
  - step_id: pm.sheets
    op: lorchestra.run
    job_id: pm.sheets
    payload: "@payload"
```

The pipeline simply wraps the pm.sheets job for consistency with other pipeline patterns.

### Verification
- `lorchestra jobs show pipeline.pm_views` → valid YAML v2.0 job
- `lorchestra run pm_views --payload '{"sheet_id": "..."}'` → executes pm.sheets projection
- Google Sheets contains fresh PM data after execution

## lorchestra.build.yaml Diff (Post-e011-04)

The lorchestra.build.yaml file will be updated to include PM entity views and sheets projection:

```yaml
# UPDATES to layout:
layout:
  - path: "sql/"
    module: sql
    role: "BigQuery projection SQL definitions"
  - path: "jobs/definitions/pm/"
    module: job_definitions
    role: "PM sheets projection job (reads views, writes to Sheets)"
  - path: "jobs/definitions/pipeline/"
    module: job_definitions
    role: "Pipeline jobs (pm_views) for batch operations"

# UPDATES to modules:
modules:
  - name: sql
    kind: module
    provides_add:
      - "pm_projections.py" — PM entity view definitions
      - get_pm_projection_sql() — SQL retrieval function

  - name: job_definitions
    kind: module
    provides_add:
      # NEW in e011-04 (sheets projection)
      - "pm/pm.sheets.yaml"
      - "pipeline/pm_views.yaml"
```

**Files to Create**:
- `lorchestra/sql/pm_projections.py` — PM entity view SQL (view_pm_projects, view_pm_work_items, view_pm_deliverables, view_pm_opsstreams, view_pm_artifacts)
- `lorchestra/jobs/definitions/pm/pm.sheets.yaml` — Sheets projection job (queries BQ views, writes to Sheets)
- `lorchestra/jobs/definitions/pipeline/pm_views.yaml` — PM views pipeline

**Files to Modify**:
- `lorchestra/sql/__init__.py` — Import PM_PROJECTIONS into PROJECTIONS registry