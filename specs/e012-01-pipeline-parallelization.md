# e012-01: Pipeline Parallelization Framework

## Overview

Add ThreadPoolExecutor-based concurrent execution to the pipeline runner to execute independent jobs within pipeline stages in parallel. This addresses the critical performance issue where pipeline stages execute jobs sequentially even when they have no inter-job dependencies.

**Expected Impact:** 2-4x speedup for pipelines with 4+ independent jobs

---

## Problem Statement

### Current Behavior
Pipeline stages execute jobs sequentially even when jobs are independent:
```yaml
# Example: 4 ingest jobs, each takes ~30 seconds
stages:
  - name: ingest
    jobs:
      - ingest_gmail_acct1   # 30s
      - ingest_gmail_acct2   # 30s
      - ingest_gmail_acct3   # 30s
      - ingest_gmail_acct4   # 30s
    # Total: 120s (sequential)
```

The executor in `pipeline.py` runs jobs one-by-one in a simple loop, with no parallelization.

### Target Behavior
Enable parallel execution within a stage when jobs are independent:
```yaml
stages:
  - name: ingest
    max_workers: 4    # NEW: Enable 4 concurrent jobs
    jobs:
      - ingest_gmail_acct1
      - ingest_gmail_acct2
      - ingest_gmail_acct3
      - ingest_gmail_acct4
    # Total: ~30s (parallel)
```

### Constraints
- **Backward compatible:** Default `max_workers=1` (sequential, no changes to existing pipelines)
- **Manual declaration:** Jobs within a stage must be declared as independent (no automatic dependency detection)
- **Idempotency:** All jobs must be idempotent or use idempotency keys to prevent race condition data corruption
- **Thread-safety:** FileRunStore may need improvements (file locking or atomic updates)

---

## Design

### Architecture

#### 1. Pipeline YAML (No Changes)
Pipeline YAML stays unchanged. `max_workers` is a **system-level concern** configured at runtime via environment or CLI, not in pipeline definitions. This allows the same pipeline to run on different hardware (different CPU/memory) without modifying YAML.

Example runtime usage:
```bash
lorchestra pipeline pipeline.ingest --max-workers 4
```

Pipeline YAML remains:
```yaml
stages:
  - name: ingest
    stop_on_failure: true       # Existing behavior
    jobs:
      - ingest_gmail_acct1
      - ingest_gmail_acct2
      - ingest_gmail_acct3
      - ingest_gmail_acct4
```

#### 2. Code Changes

**File: `/workspace/lorchestra/lorchestra/pipeline.py`**

Modify `run_pipeline()` function to:
1. Parse `max_workers` from stage definition (default: 1)
2. Use ThreadPoolExecutor for stage execution when `max_workers > 1`
3. Maintain backward compatibility with sequential execution (max_workers=1)

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def _run_stage(stage_def: dict, run_state: RunState, context: PipelineContext):
    """Execute a stage, potentially with parallel job execution."""
    stage_name = stage_def['name']
    jobs = stage_def['jobs']
    max_workers = stage_def.get('max_workers', 1)  # Default: sequential

    if max_workers == 1:
        # Sequential execution (existing behavior)
        return _run_stage_sequential(stage_name, jobs, run_state, context)
    else:
        # Parallel execution (new)
        return _run_stage_parallel(stage_name, jobs, max_workers, run_state, context)

def _run_stage_sequential(stage_name, jobs, run_state, context):
    """Sequential job execution (existing)."""
    for job_id in jobs:
        result = _run_job(job_id, run_state, context)
        if not result.success and context.stop_on_failure:
            raise PipelineError(f"Stage {stage_name}: Job {job_id} failed")

def _run_stage_parallel(stage_name, jobs, max_workers, run_state, context):
    """Parallel job execution using ThreadPoolExecutor."""
    futures = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        for job_id in jobs:
            future = executor.submit(_run_job, job_id, run_state, context)
            futures[future] = job_id

        # Wait for completion and handle results
        for future in as_completed(futures):
            job_id = futures[future]
            try:
                result = future.result()
                if not result.success and context.stop_on_failure:
                    # Cancel remaining tasks
                    for f in futures:
                        f.cancel()
                    raise PipelineError(f"Stage {stage_name}: Job {job_id} failed")
            except Exception as e:
                if context.stop_on_failure:
                    for f in futures:
                        f.cancel()
                    raise
```

#### 3. Thread-Safety: Verify Existing Guarantees

Since `run_state` is written incrementally as jobs complete (not accumulated then written once), partial writes are expected and safe. Each job writes its completion state, which is acceptable even if interleaved with other job writes.

**Verification needed:**
- Confirm JobExecutor/RunStore already handles incremental writes
- If serialization already uses atomic writes (write to temp file + move), no additional locking needed
- If multiple threads can corrupt a single JSON file, add minimal locking (fcntl on Unix/Linux, document Unix-only support)

**Why file locking is undesirable:**
- fcntl unavailable on Windows; complicates cross-platform support
- macOS filelock has different semantics than Linux
- If run_state already supports partial writes, locking adds unnecessary overhead
- Simplest approach: verify incrementally written state is safe, avoid locking entirely

#### 4. Progress Callbacks

Progress callbacks (for monitoring/logging) must be thread-safe:
```python
from threading import Lock

class ThreadSafeProgressHandler:
    def __init__(self):
        self._lock = Lock()

    def on_job_complete(self, job_id, result):
        with self._lock:
            # Thread-safe logging/callback
            logger.info(f"Job {job_id} completed: {result}")
```

---

## Implementation Checklist

### Phase 1: Core ThreadPoolExecutor Integration
- [ ] Modify `pipeline.py` to add `_run_stage_parallel()` function
- [ ] Add `max_workers` parameter parsing from runtime config/CLI
- [ ] Verify incremental run_state writes are safe (no additional locking needed)
- [ ] Add thread-safe progress callback handler if needed
- [ ] Ensure error handling respects `stop_on_failure` semantics

### Phase 2: Runtime Configuration
- [ ] Add `--max-workers` CLI argument to `lorchestra pipeline` command
- [ ] Default behavior: `max_workers=1` (sequential, backward compatible)
- [ ] Document runtime usage: `lorchestra pipeline <pipeline-name> --max-workers 4`
- [ ] Environment variable fallback (optional): `LORCHESTRA_MAX_WORKERS=4`

### Phase 3: Testing
- [ ] Unit tests for `_run_stage_parallel()` with mock jobs
- [ ] Test thread-safe progress callbacks
- [ ] Integration test: verify 3-4x speedup with 4 independent jobs
- [ ] Stress test: 20+ concurrent jobs, monitor resource usage
- [ ] Test error handling: one job fails in parallel stage, others cancelled

### Phase 4: Validation
- [ ] Run actual pipeline with parallelization enabled
- [ ] Measure end-to-end time: 4 ingest jobs (120s → ~30s)
- [ ] Verify idempotency: same run executed twice produces same results
- [ ] Monitor file locking overhead

---

## Validation Criteria

1. **Speedup Achieved**
   - 4 independent jobs @ 30s each: **120s → ~30s** (4x speedup)
   - 2 independent jobs @ 30s each: **60s → ~30s** (2x speedup)

2. **Backward Compatibility**
   - Default `--max-workers 1` preserves sequential behavior
   - No performance regression for sequential pipelines (overhead <2%)
   - Existing CLI invocations work unchanged

3. **Error Handling**
   - If one job fails and `stop_on_failure=true`, remaining jobs are cancelled
   - Partial results are preserved in run record (step executor handles this)

4. **Thread Safety**
   - No data corruption from concurrent writes (verified incremental writes safe)
   - Progress callbacks thread-safe if needed
   - No file locking overhead

5. **Resource Management**
   - ThreadPoolExecutor properly cleaned up (no thread leaks)
   - Memory usage scales linearly with max_workers

---

## Rollout Plan

1. **Code Review:** Get approval for ThreadPoolExecutor implementation
2. **Staging:** Deploy to staging environment, run parallelization tests
3. **Gradual Rollout:**
   - Initial: `lorchestra pipeline ingest.yaml --max-workers 2` (safe, 2x speedup)
   - After monitoring: `--max-workers 4` (target, 4x speedup)
4. **Monitoring:** Track parallelization speedup and error rates for 1 week
5. **Production:** Full rollout once monitoring validates stable performance

---

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| File locking contention | Low | Moderate | Lock held for <100ms (JSON write), acceptable |
| BigQuery rate limits hit | Moderate | High | Start with max_workers=2, monitor quota usage |
| Thread pool resource leak | Low | High | Comprehensive testing, context manager ensures cleanup |
| Idempotency key collision | Very Low | Catastrophic | Audit all jobs use idempotency keys; document requirement |
| Existing pipeline breakage | Very Low | High | Default max_workers=1 preserves sequential behavior |

---

## Success Metrics

- **Execution Time:** 4-job pipelines complete in <40s (was 120s)
- **Speedup Ratio:** 3-4x for pipelines with 4+ independent jobs
- **Stability:** No increase in job failure rates after parallelization
- **Resource Usage:** CPU and memory increase < 2x for max_workers=4
- **Test Coverage:** All scenarios covered (sequential, parallel, error handling, concurrency)
