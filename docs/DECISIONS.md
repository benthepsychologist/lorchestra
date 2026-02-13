# Architectural Decisions

Record of key architectural decisions for lorchestra.

---

## ADR-001: Query Builder Scope and SQL Passthrough

**Date:** 2026-02-10
**Status:** Accepted
**Context:** e006-04-peek (life peek commands)

### Decision

`query_builder.py` provides a declarative DSL for simple table reads. Complex queries use SQL passthrough.

### Scope of query_builder DSL

The DSL handles:
- **Simple SELECTs**: `dataset`, `table`, `columns`, `limit`
- **Filters**: Equality (`{column: value}`) and comparison (`[{column, op, value}]`)
- **Ordering**: Custom `order_by` clause
- **Incremental patterns**: `left_anti` and `not_exists` joins for "rows not yet processed"

The DSL does NOT handle:
- Joins (beyond incremental patterns)
- Subqueries
- Aggregations (GROUP BY, HAVING)
- CTEs
- Window functions

### SQL Passthrough

When a job needs query power beyond the DSL, use raw SQL in the job definition:

```yaml
op: storacle.query
params:
  sql: |
    SELECT t.* FROM tasks t
    JOIN clients c ON t.client_id = c.id
    WHERE c.id = @client_id
      AND t.deliverable = @deliverable
  params:
    client_id: '@payload.client_id'
    deliverable: '@payload.deliverable'
```

Storacle's `bq.query` RPC accepts SQL directly.

### Rationale

1. **90/10 rule**: Most jobs need simple table reads. The DSL covers this without writing SQL in YAML.
2. **Escape hatch**: Complex queries are rare but necessary. SQL passthrough avoids DSL bloat.
3. **Future portability**: If we migrate off BigQuery:
   - DSL queries are structured/liftable
   - Raw SQL in job defs can be grepped and migrated
4. **Separation of concerns**: Lorchestra owns query intent, storacle owns execution. Whether intent is DSL or SQL, storacle runs it.

### Consequences

- `query_builder.py` stays small and focused
- New filter operators (e.g., `LIKE`, `IN`) require explicit decision before adding
- Jobs needing joins/aggregations write SQL directly
- Storacle remains a "dumb SQL runner" for reads

---

## ADR-002: Anchor-Based Container Inheritance

**Date:** 2026-02-13
**Status:** Accepted
**Context:** e011-pm-system (PM hierarchy enforcement in workman compile_intent)

### Decision

Container hierarchy enforcement uses **anchor-based rules**, not field-matching.

The PM container hierarchy is: **OpsStream → Project → Deliverable → WorkItem**.

An entity's **lowest assigned container** is its anchor. The rules:

1. **Auto-fill flows down**: When you set a deliverable, project auto-fills from the deliverable. When you set a project, opsstream auto-fills from the project (if known within the intent).
2. **Cannot reassign above your anchor**: If a work item has a deliverable, you cannot set project_id or opsstream_id on a move — those are locked by the deliverable. If a work item has a project but no deliverable, you cannot set opsstream_id on a move — that's locked by the project.
3. **Can always change at or below your anchor**: You can reassign to any deliverable, and the project/opsstream follow. This is true even if the new deliverable belongs to a different project.
4. **Detach to reassign higher**: To move a work item from one project to another without targeting a specific deliverable, first detach from the deliverable (future `relationship.remove` op), then reassign the project.

### What this replaces

The initial implementation used conflict-matching: "if you provide deliverable_id AND project_id, validate they agree." This was wrong in two ways:
- It allowed reassigning project without changing deliverable (no check)
- It rejected valid operations where the deliverable change implies a project change (unnecessary check)

The anchor model is simpler: the deliverable is the authority on project. Don't compare — just block or auto-fill.

### Scope

**Within-intent only**: The anchor check works by looking up prior ops in the same intent. If a work item was created in a previous intent (already in BQ), its current deliverable is not visible to compile_intent. Cross-intent enforcement requires read-before-write, deferred to a future spec.

### Future: relationship.remove op

A `relationship.remove` or `container.detach` op is needed to drop a work item's deliverable assignment without moving it to another deliverable. This enables the pattern: detach → reassign project → optionally attach to new deliverable.

### Consequences

- No field-matching logic in `_resolve_inheritance` — only "is this field above your anchor?"
- `work_item.move` with `project_id` but no `deliverable_id` is rejected if the work item has a deliverable (within-intent)
- `work_item.move` with `deliverable_id` always succeeds (project auto-fills)
- Explicit `project_id` alongside `deliverable_id` is ignored (auto-fill overwrites it)
- If the anchor has no value for a higher field, the work item's value is cleared (not preserved)
- Cross-intent enforcement deferred (requires BQ read)

---

## ADR-003: Read-Time Hierarchy Resolution

**Date:** 2026-02-13
**Status:** Accepted
**Context:** e011-pm-system (PM container hierarchy — write-time vs read-time)

### Decision

The WAL records **direct assignments only**. The full container hierarchy is resolved at **read time** via BQ view JOINs.

### Problem

When a deliverable is assigned to a project after its work items were already created, those work items don't have `project_id` in their WAL payloads. Should we cascade-update them?

### No cascading writes

The WAL is append-only point-in-time facts. When a deliverable gains a project:
- The deliverable's WAL event records `project_id`
- Its existing work items are **not** retroactively updated
- No cascade ops are generated

This is correct because:
1. The WAL records what happened, not derived state
2. Cascading writes create ordering dependencies and failure modes
3. The hierarchy is a graph relationship, not a denormalized field

### BQ views resolve the chain

The read path (BQ views in e011-04) JOINs through the hierarchy:

```sql
-- view_pm_work_items resolves effective_project_id
SELECT
  wi.*,
  COALESCE(wi.project_id, del.project_id) AS effective_project_id,
  COALESCE(wi.opsstream_id, proj.opsstream_id) AS effective_opsstream_id
FROM current_work_items wi
LEFT JOIN current_deliverables del ON wi.deliverable_id = del.deliverable_id
LEFT JOIN current_projects proj ON COALESCE(wi.project_id, del.project_id) = proj.project_id
```

This means:
- `project_id` on a work item WAL event = "explicitly assigned at write time" (may be stale)
- `effective_project_id` in the BQ view = "current truth resolved through hierarchy"

### Write-time auto-fill is an optimization

`compile_intent` still auto-fills `project_id` from the deliverable at write time (ADR-002). This is a **convenience**, not the source of truth. It means most WAL events have the correct project stamped, reducing JOIN misses. But the BQ view is authoritative.

### Consequences

- No cascade logic in workman or lorchestra
- BQ views own hierarchy resolution (COALESCE through JOINs)
- WAL payloads may have stale or missing container fields — that's expected
- Schema Sheets show `effective_*` columns from views, not raw WAL fields
- If a deliverable moves to a new project, its work items' effective project updates immediately at read time — no WAL events needed

---
