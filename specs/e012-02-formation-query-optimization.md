# e012-02: Formation Query Optimization

## Overview

Fix critical performance issue where formation jobs re-process identical data every run, wasting ~85 seconds when no new data exists. The root cause is a broken incremental query that compares timestamps that change on every re-ingest cycle, making the "new data" detection always true.

**Solution:** Switch from left_anti mode (with timestamp comparison) to not_exists mode (pure existence check).

**Expected Impact:** 95% time reduction when no new data (85s → <5s), 50-60% reduction when processing actual new data.

---

## Problem Statement

### Current Behavior

Formation jobs execute the incremental query with **left_anti mode + timestamp comparison:**

```yaml
# Current (BROKEN)
incremental:
  mode: left_anti
  source_ts: processed_at    # <-- PROBLEM
  target_ts: processed_at    # <-- PROBLEM
```

This generates SQL like:
```sql
SELECT s.measurement_event_id, s.*
FROM derived.measurement_events s
LEFT JOIN derived.observations t
  ON s.measurement_event_id = t.measurement_event_id
WHERE (t.measurement_event_id IS NULL OR s.processed_at > t.processed_at)
```

### Root Cause Analysis

**Root Cause: Left_anti Join Key Mismatch**

The form_me/form_obs jobs use left_anti mode with keys that don't match:
- **Source:** `idem_key` from canonical_objects (example: "abc123")
- **Target:** `canonical_object_id` from measurement_events (generated as "co:abc123" by transform)
- **Join condition:** `t.canonical_object_id = s.idem_key`
  - Tries to match: "abc123" = "co:abc123" → **MISMATCH!**
  - Join returns NULL for all rows
  - WHERE clause `(t.canonical_object_id IS NULL OR ...)` becomes TRUE for all rows
  - **Result: All canonical_objects re-processed every run**

**Confirmed via run logs:**
```json
// Run 2026-02-09: form_me_followup
"rows_read": 978,      // ALL canonical_objects (should be 0 if already processed)
"rows_written": 429,   // All re-written to measurement_events
"duration_ms": 85073

// Run 2026-02-10: form_me_followup
"rows_read": 978,      // Same ALL canonical_objects again!
"rows_written": 429,   // Identical results (wasteful re-processing)
"duration_ms": 84694   // Same 85s waste
```

**Why this matters:** The timestamp comparison `OR s.source_ts > t.target_ts` masks the real issue (broken key join). Even if keys matched correctly, timestamp comparison would still cause re-processing whenever timestamps change.

### Target Behavior

Switch to **not_exists mode (pure existence check):**

```yaml
# Fixed
incremental:
  mode: not_exists
  source_key: measurement_event_id
  target_key: measurement_event_id
```

This generates SQL like:
```sql
SELECT s.measurement_event_id, s.*
FROM derived.measurement_events s
WHERE NOT EXISTS (
  SELECT 1 FROM derived.observations t
  WHERE t.measurement_event_id = s.measurement_event_id
)
```

**Result:** Query returns 0 rows when all measurement_events already have corresponding observations → **fast-path: <5s total job time**

---

## Design

### 1. Query Builder Changes

**File: `/workspace/lorchestra/lorchestra/query_builder.py`**

Extend the `_build_incremental_query()` function to support `not_exists` mode:

```python
def _build_incremental_query(self, config: IncementalConfig) -> str:
    """Build incremental query based on mode."""
    source_table = f"{config.source_dataset}.{config.source_table}"
    target_table = f"{config.target_dataset}.{config.target_table}"
    source_key = config.source_key
    target_key = config.target_key

    if config.mode == "not_exists":
        # Pure existence check (NEW)
        return f"""
        SELECT s.*
        FROM {source_table} s
        WHERE NOT EXISTS (
          SELECT 1 FROM {target_table} t
          WHERE t.{target_key} = s.{source_key}
        )
        """

    elif config.mode == "left_anti":
        # Left anti join with optional timestamp filtering (existing)
        join_condition = f"ON s.{source_key} = t.{target_key}"
        if config.source_ts and config.target_ts:
            join_condition += f" AND s.{config.source_ts} > t.{config.target_ts}"

        return f"""
        SELECT s.*
        FROM {source_table} s
        LEFT JOIN {target_table} t {join_condition}
        WHERE t.{target_key} IS NULL
        """

    else:
        raise ValueError(f"Unknown incremental mode: {config.mode}")
```

### 2. Formation Job YAML Updates

Update all formation job definitions to use `not_exists` mode:

**Current (6 files affected):**
```yaml
# All formation/form_*.yaml files currently use:
incremental:
  target_dataset: derived
  target_table: observations
  source_key: measurement_event_id
  target_key: measurement_event_id
  mode: left_anti
  source_ts: processed_at      # REMOVE
  target_ts: processed_at      # REMOVE
```

**Updated:**
```yaml
incremental:
  target_dataset: derived
  target_table: observations
  source_key: measurement_event_id
  target_key: measurement_event_id
  mode: not_exists            # CHANGED
  # source_ts: removed
  # target_ts: removed
```

**Files to update (6 total: 3 form_me + 3 form_obs):**
- [ ] `/workspace/lorchestra/lorchestra/jobs/definitions/formation/form_me_intake_01.yaml`
- [ ] `/workspace/lorchestra/lorchestra/jobs/definitions/formation/form_me_intake_02.yaml`
- [ ] `/workspace/lorchestra/lorchestra/jobs/definitions/formation/form_me_followup.yaml`
- [ ] `/workspace/lorchestra/lorchestra/jobs/definitions/formation/form_obs_intake_01.yaml`
- [ ] `/workspace/lorchestra/lorchestra/jobs/definitions/formation/form_obs_intake_02.yaml`
- [ ] `/workspace/lorchestra/lorchestra/jobs/definitions/formation/form_obs_followup.yaml`

### 3. Optional: Table Optimization (Secondary)

For additional query performance, add BigQuery table clustering and partitioning:

```sql
-- Cluster observations table by keys used in NOT EXISTS join
ALTER TABLE derived.observations
  CLUSTER BY measurement_event_id, binding_id;

-- Cluster measurement_events by binding_id (filtering in read step)
ALTER TABLE derived.measurement_events
  CLUSTER BY binding_id, measurement_event_id;

-- Partition for time-range filtering (optional)
ALTER TABLE derived.observations
  PARTITION BY DATE(processed_at);

ALTER TABLE derived.measurement_events
  PARTITION BY DATE(processed_at);
```

**Benefits:**
- Reduces data scanned in NOT EXISTS subquery
- Enables BigQuery to prune partitions based on filters
- Expected additional 20-30% speedup for queries with date filters

### 4. Backfill / Re-Scoring Strategy

When scoring logic changes and you need to re-score all measurements:

```bash
# 1. Clear previously scored observations for this binding
bq query --use_legacy_sql=false \
  "DELETE FROM \`project.derived.observations\` WHERE binding_id = 'intake_01'"

# 2. Re-run formation job
lorchestra exec run form_obs_intake_01

# Explanation:
# - Query now returns all 60 measurement_events (target table empty)
# - Formation job re-scores all, writes updated observations
# - Subsequent runs return 0 rows (all already scored)
```

---

## Implementation Checklist

### Phase 1: Query Builder Enhancement
- [ ] Add `not_exists` mode support to `_build_incremental_query()`
- [ ] Validate SQL generation for both modes
- [ ] Add unit tests for `not_exists` query generation
- [ ] Ensure backward compatibility with existing `left_anti` mode

### Phase 2: Formation Job Updates
- [ ] Update all 6 formation job YAML files to use `not_exists` mode
- [ ] Remove `source_ts` and `target_ts` from incremental config
- [ ] Verify YAML schema validation passes
- [ ] Document which jobs were updated

### Phase 3: Optional Table Optimization
- [ ] Review current observations table row count and query time
- [ ] Add clustering to observations and measurement_events tables
- [ ] Add date-based partitioning
- [ ] Measure query performance improvement (before/after)

### Phase 4: Validation
- [ ] Execute formation jobs against BigQuery with new query mode
- [ ] Verify results match previous run (same rows_written, same observation values)
- [ ] Measure time reduction: no new data case (85s → <5s)
- [ ] Measure time reduction: with new data case (~50-60% improvement)
- [ ] Check idempotency: re-running job doesn't duplicate observations

### Phase 5: Documentation
- [ ] Document re-scoring procedure in runbook
- [ ] Add troubleshooting guide for when query returns unexpected rows
- [ ] Update performance monitoring to track incremental query efficiency

---

## Expected Behavior

### Scenario 1: No New Data (Most Common)
```
Run form_obs_intake_01:
1. Query measurement_events with NOT EXISTS filter
   → Returns 0 rows (all already have observations)
2. Score step: Skip (no rows to score)
3. Write step: Skip (no rows to write)
4. Total: <5s (vs 85s previously)
→ 95% TIME SAVED ✓
```

### Scenario 2: New Measurement Events Added
```
Run form_obs_intake_01:
1. Query measurement_events with NOT EXISTS filter
   → Returns 10 new measurement_events
2. Score step: Score 10 new events (~3s)
3. Write step: Write 10 new observations (~2s)
4. Total: ~5-8s
→ 50-60% TIME SAVED (was 85s for 60 events, now <10s for similar work) ✓
```

### Scenario 3: Backfill / Re-Score
```
bq DELETE FROM observations WHERE binding_id = 'intake_01'
Run form_obs_intake_01:
1. Query measurement_events with NOT EXISTS filter
   → Returns all 60 measurement_events (target table empty)
2. Score step: Score all 60 events
3. Write step: Write 429 observations
4. Total: ~85s
→ FULL RE-SCORE (expected behavior) ✓
```

---

## Validation Criteria

1. **Query Correctness**
   - NOT EXISTS query returns identical rows as old left_anti query (same measurement_event_ids)
   - Formation jobs produce identical observations (same rows_written, same values)
   - No data loss or duplication

2. **Performance Improvement**
   - No new data case: 85s → <5s (95% reduction)
   - New data case: 85s → <40s for similar workload (50% reduction)
   - Table optimization (clustering) adds additional 20-30% speedup

3. **Idempotency**
   - Running job twice without new data doesn't duplicate observations
   - Observation values are deterministic (same input = same output)

4. **Backward Compatibility**
   - Old left_anti queries still work (code supports both modes)
   - Can gradually roll out not_exists per job if needed

---

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Query returns different rows | Low | High | Unit test: compare query results with old mode |
| Observation values differ | Very Low | High | Deterministic scoring (no randomness in canonizer/finalform) |
| BigQuery table corruption | Very Low | Catastrophic | Backup observations table before clustering |
| Query timeout on large tables | Low | Moderate | NOT EXISTS is simpler, should be faster; partition if needed |
| Idempotency key collision | Very Low | Catastrophic | Audit measurement_event_id uniqueness in source |

---

## Success Metrics

- **Formation no-new-data execution:** <5 seconds (was 85s)
- **Formation new-data execution:** 50-60% faster (was 85s baseline)
- **Query fast-path:** Returns 0 rows in <2 seconds (enables rapid pipeline cycles)
- **Observation correctness:** Identical results to previous mode (validated via tests)
- **End-to-end pipeline:** 1000 emails → canonized observations in <5 minutes

---

## Related Items

- **e012-01 (Pipeline Parallelization):** Running 4 ingest jobs in parallel feeds fresh data to formation jobs, reducing "no new data" frequency
- **e012-03 (Performance Observability):** Monitors query_ms and rows_read per run to validate optimization effectiveness
