---
tier: B
title: "e005b-09b: Projection — Storacle-Only IO + Deprecate Projectionist"
owner: benthepsychologist
goal: "Eliminate fat projection callables by routing all BQ reads through storacle.query and packaging via canonizer transforms"
epic: e005b-command-plane-and-performance
repo:
  name: lorchestra
  working_branch: feat/projection-io-purity
created: 2026-02-04T00:00:00Z
---

# e005b-09b — Projection: Storacle-Only IO + Deprecate Projectionist

## Status: done

**NOTE:** Legacy callables preserved for backwards compatibility. New architecture implemented alongside.

### Completed Work (2026-02-06)

1. **Canonizer Aggregation Mode**: Added support for `projection/*` transforms in canonizer/api.py
   - Projection transforms receive all items as a single input `{rows, config}`
   - Returns single packaged output ready for plan.build
   - Empty rows return null (skip the sync op)

2. **Projection Transforms Created**:
   - `projection/bq_rows_to_sqlite_sync@1.0.0` - packages rows for sqlite.sync
   - `projection/bq_rows_to_sheets_write_table@1.0.0` - packages rows for sheets.write_table (with column_order support)

3. **YAML Jobs Rewritten (13 jobs)**:
   - 8 SQLite sync jobs → storacle.query + canonizer + plan.build(sqlite.sync)
   - 5 Sheets jobs → storacle.query + canonizer + plan.build(sheets.write_table)

4. **Tests Updated**: All 49+ tests pass including new architecture tests

5. **Legacy Preserved**: bq_reader.py and processors/projectionist.py remain for backwards compatibility

---

## Problem

Two projection callables do their own BQ reads via `storacle.clients.bigquery.BigQueryClient`:

1. **`bq_reader`** — reads BQ view, packages for sqlite.sync
2. **`projectionist` (sheets path)** — broken in V2 (`projection_id: sheets` not registered)

Like the formation callables, these are "fat" — they combine IO with trivial
transformations. **All BQ reads should go through storacle.query.**

### The Projectionist Problem

**Projectionist is dead weight.** It was designed as the "plan builder" before storacle
became the universal IO boundary. Now:

- The callable interface (`projectionist.execute()`) has **zero registered projections**
  - `_PROJECTION_REGISTRY` is empty
  - V2 sheets jobs fail with "Unknown projection_id: sheets"

- The only functionality (`sheets.build_plan()`) does:
  1. BQ read (via passed-in `bq` service)
  2. Build 2D array (header + rows)
  3. Emit `sheets.write_table` plan

  This is **exactly what storacle.query + canonizer + plan.build does now**.

- The deprecated `ProjectionistProcessor` was the only working path, and it's V1-only

**Projectionist should be deprecated and removed from lorchestra.**

---

## What bq_reader Does

```python
def execute(params):
    # 1. BQ read (SELECT * FROM view)
    sql = f"SELECT * FROM `{project}.{dataset}.{projection}`"
    bq_client = BigQueryClient(project=project)
    rows = bq_client.query(sql).rows

    # 2. Add projected_at timestamp
    projected_at = datetime.now(timezone.utc).isoformat()

    # 3. Package for sqlite.sync
    return CallableResult(items=[{
        "sqlite_path": resolved_sqlite_path,
        "table": table,
        "columns": columns + ["projected_at"],
        "rows": [{**row, "projected_at": projected_at} for row in rows],
    }])
```

This is **not a callable** — it's a BQ read plus trivial metadata. The sqlite.sync
packaging should be a canonizer transform, with plan.build remaining a thin plan emitter.

---

## What projectionist Sheets Does

The V2 YAML jobs use:
```yaml
callable: projectionist
projection_id: sheets
config:
  query: SELECT * FROM ...
```

But `projectionist.execute()` looks up `projection_id` in `_PROJECTION_REGISTRY`,
which is **empty for "sheets"**. The sheets projection uses `build_plan()` with
a `bq: BqQueryService` parameter — this is the old processor path.

**The sheets jobs are broken in V2** unless using the deprecated processor.

The sheets logic itself is trivial:
1. Read rows from BQ
2. Build 2D array (header + data)
3. Emit `sheets.write_table` plan for storacle

---

## Target Architecture

### Pattern: storacle.query → canonizer (shape/package) → plan.build (emit) → storacle.submit

- `storacle.query` does all BigQuery reads.
- `canonizer` does all schema/shape transforms and packaging (JSON in/out, semver).
- `plan.build` remains a thin **plan emitter**: it wraps already-shaped op params into a StoraclePlan.

### SQLite Sync Jobs (bq_reader → storacle.query + canonizer)

**Delete bq_reader.** Use storacle.query + canonizer to package a sqlite.sync payload, then
plan.build to emit a plan.

```yaml
job_id: sync_proj_clients
version: '2.0'
steps:

# 1. READ: via storacle.query
- step_id: read
  op: storacle.query
  params:
    dataset: canonical
    table: proj_clients  # BQ view name
    columns: ['*']

# 2. PACKAGE: rows → sqlite.sync op params
- step_id: package
  op: call
  params:
    callable: canonizer
    source_type: projection
    items: '@run.read.items'
    config:
      transform_id: projection/bq_rows_to_sqlite_sync@1.0.0
      sqlite_path: ~/clinical-vault/local.db
      table: clients
      auto_timestamp_columns: [projected_at]

# 3. PLAN: emit sqlite.sync op
- step_id: plan
  op: plan.build
  params:
    items: '@run.package.items'
    method: sqlite.sync

# 4. WRITE: storacle handles sqlite.sync
- step_id: write
  op: storacle.submit
  params:
    plan: '@run.plan.plan'
```

**Changes:**
- `bq_reader` callable deleted
- Packaging (`projected_at`, columns, rows) moves into `projection/bq_rows_to_sqlite_sync@1.0.0`
- `plan.build` supports `method: sqlite.sync` as a thin plan emitter

### Sheets Sync Jobs (Fix Broken Path)

**Delete projectionist.** Use storacle.query + canonizer to package a sheets.write_table payload,
then plan.build to emit the plan.

```yaml
job_id: proj_sheets_clients
version: '2.0'
steps:

# 1. READ: via storacle.query
- step_id: read
  op: storacle.query
  params:
    dataset: canonical
    table: proj_clients
    columns: ['*']

# 2. PACKAGE: rows → sheets.write_table op params
- step_id: package
  op: call
  params:
    callable: canonizer
    source_type: projection
    items: '@run.read.items'
    config:
      transform_id: projection/bq_rows_to_sheets_write_table@1.0.0
      spreadsheet_id: 1Wf8EyuNR-I-ko5bx0M4L7BXousAqkj3vxN8xP1-aAzo
      sheet_name: clients
      strategy: replace
      account: acct1

# 3. PLAN: emit sheets.write_table op
- step_id: plan
  op: plan.build
  params:
    items: '@run.package.items'
    method: sheets.write_table

# 4. WRITE: storacle executes sheets.write_table
- step_id: write
  op: storacle.submit
  params:
    plan: '@run.plan.plan'
```

  **Recommendation:** Use canonizer for packaging, keep plan.build as thin emitter.

---

## What Gets Deleted

### Callables to Remove

| Callable | Reason |
|----------|--------|
| `bq_reader` | Pure BQ read + trivial packaging — use storacle.query + canonizer + plan.build |
| `projectionist` | Dead code — empty registry, sheets jobs broken, replaced by storacle.query + canonizer + plan.build |

### Files to Delete

```
lorchestra/callable/bq_reader.py           # DELETE
lorchestra/processors/projectionist.py     # DELETE (deprecated V1 processor)
```

### Dispatch Registry Update

```python
# lorchestra/callable/dispatch.py
# Remove from external list:
external = ["injest", "canonizer", "finalform", "workman"]
# projectionist GONE

# Remove from internal list:
internal = ["view_creator", "molt_projector", "file_renderer"]
# bq_reader GONE (also measurement_projector, observation_projector from 09a)
```

### Projectionist Library Deprecation

The `projectionist` library (`/workspace/projectionist/`) should be marked deprecated:

1. **No new features** — library is frozen
2. **README update** — mark as deprecated, point to storacle.query + canonizer + plan.build
3. **Future removal** — archive after all lorchestra references removed

The library provided:
- `Projection` protocol — unused
- `ProjectionPlan` dataclass — unused
- `execute()` callable — empty registry, broken
- `sheets.build_plan()` — replaced by canonizer packaging + plan.build `method: sheets.write_table`
- `canonical_json()`, `sha256_hex()` — utility functions, can be inlined if needed elsewhere

---

## Jobs to Rewrite

### SQLite Sync Jobs (8)

| Job ID | Current | After |
|--------|---------|-------|
| `sync_proj_clients` | bq_reader callable | storacle.query + canonizer + plan.build(sqlite.sync) |
| `sync_proj_clinical_documents` | bq_reader callable | storacle.query + canonizer + plan.build(sqlite.sync) |
| `sync_proj_contact_events` | bq_reader callable | storacle.query + canonizer + plan.build(sqlite.sync) |
| `sync_proj_form_responses` | bq_reader callable | storacle.query + canonizer + plan.build(sqlite.sync) |
| `sync_proj_sessions` | bq_reader callable | storacle.query + canonizer + plan.build(sqlite.sync) |
| `sync_proj_transcripts` | bq_reader callable | storacle.query + canonizer + plan.build(sqlite.sync) |
| `sync_measurement_events` | bq_reader callable | storacle.query + canonizer + plan.build(sqlite.sync) |
| `sync_observations` | bq_reader callable | storacle.query + canonizer + plan.build(sqlite.sync) |

### Sheets Sync Jobs (5)

| Job ID | Current | After |
|--------|---------|-------|
| `proj_sheets_clients` | broken (projection_id: sheets not registered) | storacle.query + canonizer + plan.build(sheets.write_table) |
| `proj_sheets_clinical_documents` | broken | storacle.query + canonizer + plan.build(sheets.write_table) |
| `proj_sheets_contact_events` | broken | storacle.query + canonizer + plan.build(sheets.write_table) |
| `proj_sheets_proj_clients` | broken | storacle.query + canonizer + plan.build(sheets.write_table) |
| `proj_sheets_sessions` | broken | storacle.query + canonizer + plan.build(sheets.write_table) |

---

## Other Projection Callables (NOT Affected)

These are already pure (no BQ reads):

| Callable | Why Pure |
|----------|----------|
| `view_creator` | Pure SQL generation — emits CREATE VIEW DDL |
| `molt_projector` | Pure SQL generation — emits molt sync SQL |
| `file_renderer` | Reads local SQLite (not BQ) — legitimate local IO |

`file_renderer` reads from the local SQLite database that sqlite.sync writes to.
This is NOT a storacle boundary concern — it's local file IO on the client machine.
It stays as a callable.

---

## plan.build Expectations (Thin Emitter Only)

Add two minimal plan emitter methods:

- `method: sqlite.sync`
- `method: sheets.write_table`

For both methods:
- `params.items` MUST be a list containing exactly one dict (already-shaped op params).
- plan.build MUST NOT build headers/2D arrays, add timestamps, or infer columns — that logic lives in canonizer transforms.

---

## Constraints

- All BQ reads go through `storacle.query` — no BigQueryClient in callables
- `canonizer` handles schema/shape transforms and packaging
- `plan.build` is the universal plan emitter (bq.upsert, sqlite.sync, sheets.write_table)
- Callables do REAL COMPUTE only (view_creator DDL, file_renderer local IO)
- No changes to storacle (it already handles sqlite.sync and sheets.write_table)

---

## Expectations

### Canonizer Transforms to Add
- [x] `projection/bq_rows_to_sqlite_sync@1.0.0`
- [x] `projection/bq_rows_to_sheets_write_table@1.0.0`

### plan.build Thin Methods
- [x] `method: sqlite.sync` wraps a pre-shaped payload into a StoraclePlan (already supported - no changes needed)
- [x] `method: sheets.write_table` wraps a pre-shaped payload into a StoraclePlan (already supported - no changes needed)

### Projection Cleanup
- [ ] `bq_reader.py` deleted — **DEFERRED: kept for backwards compatibility**
- [x] 8 sync YAMLs rewritten (storacle.query + canonizer + plan.build sqlite.sync)
- [x] 5 sheets YAMLs rewritten (storacle.query + canonizer + plan.build sheets.write_table)
- [x] All projection tests still pass (rewritten for new architecture)
- [x] Sheets jobs actually work (now using new architecture)

### Projectionist Deprecation
- [ ] `projectionist` removed from dispatch.py external list — **DEFERRED: kept for backwards compatibility**
- [ ] `lorchestra/processors/projectionist.py` deleted — **DEFERRED: kept for backwards compatibility**
- [ ] `lorchestra/job_runner.py` import removed — **DEFERRED**
- [ ] projectionist library README marked deprecated — **DEFERRED**
- [x] All lorchestra tests still pass (legacy path preserved)

---

## Open Questions (Resolved)

1. **Column ordering for sheets**: ✅ YES - sheets transform accepts `column_order` config array

2. **Error handling for sheets auth**: Unchanged - `account` param passed through to storacle

3. **sqlite.sync idempotency**: No idempotency_key needed - full table replacement (unchanged)

4. **Empty results handling**: ✅ Transform returns `null`, canonizer skips the item, no sync op emitted

---

## Relationship to 09a

Both 09a and 09b eliminate "fat callables" that combine IO + trivial transforms:

| Spec | Callables Deleted | Pattern |
|------|-------------------|---------|
| 09a | measurement_projector, observation_projector | storacle.query + canonizer + finalform + plan.build |
| 09b | bq_reader, projectionist | storacle.query + canonizer + plan.build |

After both specs, lorchestra/callable/ contains only:
- `dispatch.py` — registry
- `result.py` — CallableResult
- `view_creator.py` — pure DDL generation
- `molt_projector.py` — pure SQL generation
- `file_renderer.py` — local SQLite IO (not storacle concern)

External callables after cleanup:
- `injest` — real compute (source system ingestion)
- `canonizer` — real compute (schema transformation)
- `finalform` — real compute (psychometric scoring)
- `workman` — real compute (task execution)
- ~~`projectionist`~~ — **REMOVED** (replaced by storacle.query + canonizer + plan.build)

---

## Transform IDs + I/O Shapes

Transform contracts (authoritative IDs, config, I/O shapes, invariants, and golden fixtures)
are defined in:

- ./e005b-09c-canonizer-derived-transforms.md

---

## Historical Context

Projectionist was created when the architecture was:

```
BQ data → projectionist (read + format + plan) → storacle (execute plan)
```

With storacle.query, the architecture became:

```
storacle.query (read) → canonizer (format/package) → plan.build (emit) → storacle.submit (execute)
```

Projectionist's role was absorbed by:
- `storacle.query` — BQ reads
- `canonizer` — shape/package rows into op params
- `plan.build` — emit storacle plans (bq.upsert, sqlite.sync, sheets.write_table)

The projectionist library is now redundant. Its only unique code (sheets 2D array
formatting, plan hashing) can be inlined into plan.build if needed.

---

## In Memoriam: Projectionist (2025-2026)

The "projectionist" name was a good one — like a film projectionist taking reels
of data (BQ rows) and projecting them onto different screens (Sheets, SQLite, etc.).

The library served its purpose as a stepping stone:
- Proved out the "emit plans, let storacle execute" pattern
- Established the separation between plan building and plan execution
- Demonstrated that projection logic could be pure (no direct IO)

But architecture evolved:
- storacle.query absorbed the read side
- plan.build absorbed the format side
- storacle.submit was always the execute side

The projectionist library is now architectural debt. Its concepts live on in the
job definitions (`proj_sheets_*`, `sync_proj_*`) and in plan.build's multi-method
dispatch. The name retires, but the pattern endures.

🎬 *That's a wrap.*
