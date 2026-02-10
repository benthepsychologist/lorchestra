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
