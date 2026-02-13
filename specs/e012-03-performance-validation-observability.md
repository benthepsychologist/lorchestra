# e012-03: Performance Validation & Observability

## Overview

Implement comprehensive performance validation for parallelization (e012-01) and formation query optimization (e012-02). Add enhanced observability to run records with step-level timing breakdown and performance metrics logged to ops.event_log for historical trending and debugging.

**Dependencies:** e012-01, e012-02 (validates both are working)

**Expected Impact:** Enables quick identification of bottlenecks, validates 3-4x parallelization speedup and 95% formation waste elimination achieved.

---

## Problem Statement

### Current State

Run records only capture:
```json
{
  "run_id": "01KH4SATJ0CWDFWGC8NVQ5HF36",
  "job_id": "form_obs_intake_01",
  "duration_ms": 84694,        // Total time (opaque)
  "rows_read": 978,
  "rows_written": 429
}
```

**Issues:**
1. No visibility into step-level timing (where are the 85 seconds going?)
2. Can't measure e012-01 parallelization speedup without before/after tests
3. Can't validate e012-02 query optimization per-run
4. No historical trend data for performance regression detection

### Requirements

1. **Integration tests** for parallel ingest (validate 3-4x speedup)
2. **Integration tests** for formation query optimization (validate 60%+ reduction)
3. **Enhanced run records** with step-level timing breakdown
4. **Performance metrics** logged to ops.event_log for trending
5. **Observability dashboard** queries possible (queryable performance history)

---

## Design

### 1. Enhanced Run Record Schema

**File: `/workspace/lorchestra/lorchestra/models/run_record.py`**

Extend RunRecord dataclass to include step-level timing:

```python
from dataclasses import dataclass, field
from typing import List, Dict, Optional
import json

@dataclass
class StepTiming:
    """Per-step execution metrics."""
    step_id: str
    duration_ms: int
    rows_read: Optional[int] = None
    rows_written: Optional[int] = None

    def to_dict(self):
        return {
            "step_id": self.step_id,
            "duration_ms": self.duration_ms,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
        }

@dataclass
class PerformanceIndicators:
    """Derived performance metrics for quick analysis."""
    query_efficiency: str  # "good", "moderate", "poor"
    transform_throughput: float  # rows/second
    write_throughput: float  # rows/second
    bottleneck_step: str  # step_id consuming most time

    def to_dict(self):
        return {
            "query_efficiency": self.query_efficiency,
            "transform_throughput": self.transform_throughput,
            "write_throughput": self.write_throughput,
            "bottleneck_step": self.bottleneck_step,
        }

@dataclass
class RunRecord:
    """Enhanced run record with step-level observability."""
    run_id: str
    job_id: str
    status: str  # "success", "failure", "partial"
    duration_ms: int
    rows_read: int
    rows_written: int

    # NEW: Step-level timing breakdown
    step_timings: List[StepTiming] = field(default_factory=list)

    # NEW: Derived performance indicators
    performance_indicators: Optional[PerformanceIndicators] = None

    # Existing fields
    started_at: str
    completed_at: str
    errors: List[str] = field(default_factory=list)

    def to_json(self):
        """Serialize to JSON, including nested objects."""
        result = {
            "run_id": self.run_id,
            "job_id": self.job_id,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "errors": self.errors,
        }

        if self.step_timings:
            result["step_timings"] = [s.to_dict() for s in self.step_timings]

        if self.performance_indicators:
            result["performance_indicators"] = self.performance_indicators.to_dict()

        return json.dumps(result, indent=2)
```

### 2. Step-Level Timing Instrumentation

**File: `/workspace/lorchestra/lorchestra/executor.py`**

Instrument job execution to capture per-step timing:

```python
import time
from typing import List, Dict

class JobExecutor:
    """Executes job steps with timing instrumentation."""

    def execute_job(self, job_def, run_state):
        """Execute all steps in a job, capturing timing."""
        step_timings: List[Dict] = []
        job_start = time.time()

        for step_def in job_def['steps']:
            step_id = step_def['step_id']
            step_start = time.time()

            # Execute step
            result = self._execute_step(step_def, run_state)

            # Record timing
            step_duration_ms = int((time.time() - step_start) * 1000)
            timing = {
                "step_id": step_id,
                "duration_ms": step_duration_ms,
                "rows_read": result.get('rows_read'),
                "rows_written": result.get('rows_written'),
            }
            step_timings.append(timing)

        # Compute performance indicators
        perf_indicators = self._compute_performance_indicators(step_timings)

        # Store in run_state
        run_state['step_timings'] = step_timings
        run_state['performance_indicators'] = perf_indicators

        return {
            "step_timings": step_timings,
            "performance_indicators": perf_indicators,
        }

    def _compute_performance_indicators(self, step_timings):
        """Derive high-level performance metrics."""
        total_rows_read = sum(s.get('rows_read') or 0 for s in step_timings)
        total_rows_written = sum(s.get('rows_written') or 0 for s in step_timings)
        total_duration_ms = sum(s['duration_ms'] for s in step_timings)

        # Find bottleneck
        bottleneck = max(step_timings, key=lambda s: s['duration_ms'])
        bottleneck_step = bottleneck['step_id']

        # Query efficiency: how long to read data?
        read_steps = [s for s in step_timings if 'read' in s['step_id'].lower()]
        read_time = sum(s['duration_ms'] for s in read_steps)

        if read_time < 5000:  # < 5s
            query_efficiency = "good"
        elif read_time < 15000:  # < 15s
            query_efficiency = "moderate"
        else:
            query_efficiency = "poor"

        # Throughput metrics
        transform_throughput = total_rows_read / (total_duration_ms / 1000) if total_duration_ms > 0 else 0
        write_throughput = total_rows_written / (total_duration_ms / 1000) if total_duration_ms > 0 else 0

        return {
            "query_efficiency": query_efficiency,
            "transform_throughput": round(transform_throughput, 2),
            "write_throughput": round(write_throughput, 2),
            "bottleneck_step": bottleneck_step,
        }
```

### 3. Performance Metrics to ops.event_log

**File: `/workspace/lorchestra/lorchestra/ops/event_logger.py`**

Log performance metrics for historical trending:

```python
from datetime import datetime
from typing import Dict, Any

class PerformanceEventLogger:
    """Log performance metrics to ops.event_log for trending."""

    def log_formation_performance(self, job_id: str, step_timings: List[Dict], run_id: str):
        """Log formation job performance metrics."""

        # Extract key timing data
        query_step = next((s for s in step_timings if s['step_id'] == 'read_me'), None)
        transform_step = next((s for s in step_timings if s['step_id'] in ['prepare', 'score', 'shape']), None)
        write_step = next((s for s in step_timings if s['step_id'] == 'write'), None)

        event = {
            "event_type": "formation.performance",
            "timestamp": datetime.utcnow().isoformat(),
            "producer_component": job_id,
            "run_id": run_id,
            "payload": {
                "query_ms": query_step['duration_ms'] if query_step else 0,
                "transform_ms": transform_step['duration_ms'] if transform_step else 0,
                "write_ms": write_step['duration_ms'] if write_step else 0,
                "rows_read": query_step.get('rows_read') if query_step else 0,
                "rows_written": write_step.get('rows_written') if write_step else 0,
            }
        }

        # Write to ops.event_log (BigQuery)
        self._write_event(event)

    def log_parallel_ingest_performance(self, stage_name: str, job_count: int,
                                       max_workers: int, duration_ms: int, run_id: str):
        """Log parallel ingest stage performance."""
        event = {
            "event_type": "ingest.parallel_performance",
            "timestamp": datetime.utcnow().isoformat(),
            "producer_component": f"pipeline.{stage_name}",
            "run_id": run_id,
            "payload": {
                "job_count": job_count,
                "max_workers": max_workers,
                "duration_ms": duration_ms,
                "speedup_potential": job_count / max_workers,  # If sequential: job_count * 30s
                "actual_per_job": duration_ms / job_count,
            }
        }
        self._write_event(event)

    def _write_event(self, event: Dict[str, Any]):
        """Write event to BigQuery ops.event_log."""
        # Implementation: Insert into ops.event_log table
        pass
```

### 4. Integration Tests

**File: `/workspace/lorchestra/tests/e2e/test_performance_target.py`**

Comprehensive performance validation tests:

```python
import pytest
import time
from lorchestra.executor import run_job
from lorchestra.pipeline import run_pipeline

@pytest.mark.perf
class TestParallelizationPerformance:
    """Validate e012-01 parallelization speedup."""

    def test_parallel_ingest_4_jobs_3x_speedup(self):
        """
        Test: 4 independent ingest jobs with parallelization.
        Expected: ~3x speedup (120s → 40s).
        """
        # Run sequentially (max_workers=1)
        start = time.time()
        result_seq = run_pipeline(
            "pipeline.ingest_test",
            max_workers_override=1
        )
        elapsed_seq = time.time() - start

        # Run in parallel (max_workers=4)
        start = time.time()
        result_par = run_pipeline(
            "pipeline.ingest_test",
            max_workers_override=4
        )
        elapsed_par = time.time() - start

        # Calculate speedup
        speedup = elapsed_seq / elapsed_par

        # Assertions
        assert speedup >= 2.5, f"Expected 3x speedup, got {speedup:.1f}x"
        assert result_seq['rows_processed'] == result_par['rows_processed'], \
            "Parallel and sequential produce different results"

        print(f"Parallelization speedup: {speedup:.1f}x (seq: {elapsed_seq:.1f}s, par: {elapsed_par:.1f}s)")

    def test_backward_compatibility_sequential(self):
        """Test: Default max_workers=1 doesn't break existing pipelines."""
        result = run_pipeline("pipeline.ingest_test")  # No max_workers specified
        assert result['status'] == 'success'
        assert result['rows_processed'] > 0


@pytest.mark.perf
class TestFormationQueryOptimization:
    """Validate e012-02 query optimization speedup."""

    def test_not_exists_query_60_percent_faster(self):
        """
        Test: Formation query with NOT EXISTS is 60%+ faster (no new data).
        Expected: ~5s (was 85s).
        """
        # Execute formation job (uses new NOT EXISTS query)
        start = time.time()
        result = run_job({
            'job_id': 'form_obs_intake_01',
            'use_old_query': False,  # Use NOT EXISTS
        })
        elapsed_new = time.time() - start

        # Verify rows_read is low (no new data scenario)
        assert result['rows_read'] < 100, \
            f"Expected small result set (no new data), got {result['rows_read']} rows"

        # Check execution time
        assert elapsed_new < 10, \
            f"Expected <10s for no-new-data case, got {elapsed_new:.1f}s"

        print(f"Formation query (no new data): {elapsed_new:.1f}s")

    def test_not_exists_correctness_matches_left_anti(self):
        """
        Test: NOT EXISTS query produces identical results as left_anti.
        Validates correctness before full rollout.
        """
        # Run with old left_anti query
        result_old = run_job({
            'job_id': 'form_obs_intake_01',
            'use_old_query': True,
        })

        # Run with new NOT EXISTS query
        result_new = run_job({
            'job_id': 'form_obs_intake_01',
            'use_old_query': False,
        })

        # Compare results
        assert result_old['rows_read'] == result_new['rows_read'], \
            f"Query modes return different row counts: old={result_old['rows_read']}, new={result_new['rows_read']}"

        assert result_old['rows_written'] == result_new['rows_written'], \
            f"Query modes produce different results: old={result_old['rows_written']}, new={result_new['rows_written']}"

        # Spot-check a few observation values
        assert result_old['observations'] == result_new['observations'], \
            "Observation values differ between query modes"

        print("✓ NOT EXISTS query produces identical results as left_anti")


@pytest.mark.perf
class TestEndToEndPerformance:
    """Validate combined e012-01 + e012-02 achieves overall targets."""

    def test_1000_emails_under_5_minutes(self):
        """
        Test: End-to-end pipeline (ingest + formation) for 1000 emails < 5 minutes.
        Expected: With parallelization + query optimization.
        """
        # Run full pipeline with 1000 emails
        start = time.time()
        result = run_pipeline(
            "pipeline.full",
            test_data_size=1000
        )
        elapsed = time.time() - start

        # Assertions
        assert result['status'] == 'success'
        assert result['total_rows_processed'] == 1000
        assert elapsed < 300, \
            f"Expected <5 minutes (300s), got {elapsed:.1f}s"

        print(f"1000 emails end-to-end: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")


@pytest.mark.perf
class TestRunRecordObservability:
    """Validate enhanced run records with step-level timing."""

    def test_run_record_includes_step_timings(self):
        """Test: Run record contains step-level timing breakdown."""
        result = run_job({'job_id': 'form_obs_intake_01'})
        run_record = result['run_record']

        # Verify structure
        assert 'step_timings' in run_record, "Missing step_timings in run record"
        assert len(run_record['step_timings']) > 0, "step_timings empty"

        # Verify each step has required fields
        for step in run_record['step_timings']:
            assert 'step_id' in step
            assert 'duration_ms' in step
            assert step['duration_ms'] >= 0

        # Verify performance indicators
        assert 'performance_indicators' in run_record
        indicators = run_record['performance_indicators']
        assert 'query_efficiency' in indicators
        assert indicators['query_efficiency'] in ['good', 'moderate', 'poor']

        print(f"Run record steps: {len(run_record['step_timings'])}")
        print(f"Bottleneck: {indicators['bottleneck_step']}")

    def test_performance_metrics_logged_to_event_log(self):
        """Test: Performance metrics logged to ops.event_log."""
        run_id = run_job({'job_id': 'form_obs_intake_01'})['run_id']

        # Query ops.event_log for this run
        events = query_event_log({
            'run_id': run_id,
            'event_type': 'formation.performance'
        })

        assert len(events) > 0, f"No performance events logged for run {run_id}"

        event = events[0]
        assert 'query_ms' in event['payload']
        assert 'transform_ms' in event['payload']
        assert 'write_ms' in event['payload']
        assert 'rows_read' in event['payload']

        print(f"Logged event: {event['event_type']}")
```

---

## Implementation Checklist

### Phase 1: Enhanced Run Record Schema
- [ ] Update RunRecord dataclass with step_timings and performance_indicators
- [ ] Add StepTiming and PerformanceIndicators dataclasses
- [ ] Update RunRecord.to_json() serialization
- [ ] Update FileRunStore to persist new fields
- [ ] Ensure backward compatibility (old run records still load)

### Phase 2: Step-Level Timing Instrumentation
- [ ] Instrument JobExecutor to capture step execution times
- [ ] Add step timing recording to each step execution
- [ ] Implement _compute_performance_indicators() function
- [ ] Test with sample job, verify step_timings populated

### Phase 3: Performance Event Logging
- [ ] Create PerformanceEventLogger class
- [ ] Implement log_formation_performance() method
- [ ] Implement log_parallel_ingest_performance() method
- [ ] Integrate logging into pipeline execution path
- [ ] Verify events written to ops.event_log

### Phase 4: Integration Tests
- [ ] Create test_performance_target.py with all test classes
- [ ] Implement test_parallel_ingest_4_jobs_3x_speedup
- [ ] Implement test_not_exists_query_60_percent_faster
- [ ] Implement test_not_exists_correctness_matches_left_anti
- [ ] Implement test_1000_emails_under_5_minutes
- [ ] Implement test_run_record_includes_step_timings
- [ ] All tests marked with @pytest.mark.perf

### Phase 5: Verification
- [ ] Run `pytest --perf` to validate all performance tests pass
- [ ] Collect baseline metrics (before e012-01 and e012-02)
- [ ] Deploy e012-01 and e012-02, collect new metrics
- [ ] Calculate actual speedup vs. expected
- [ ] Document any gaps, plan mitigation

---

## Validation Criteria

1. **Parallelization Speedup**
   - 4 independent jobs @ 30s each: sequential 120s → parallel ~40s (3x)
   - 2 independent jobs @ 30s each: sequential 60s → parallel ~30s (2x)

2. **Formation Query Optimization**
   - No new data case: 85s → <5s (95% reduction)
   - New data case: <10s to score 10 new events (baseline: 85s for 60 events)

3. **End-to-End Performance**
   - 1000 emails ingested + canonized in <5 minutes
   - Parallelization + query optimization working together

4. **Observability Quality**
   - Run record includes step_timings with duration_ms per step
   - Performance indicators correctly identify bottleneck
   - Performance metrics queryable via ops.event_log

5. **Test Coverage**
   - All performance tests pass (pytest --perf)
   - No performance regression compared to baseline
   - Correctness validated (NOT EXISTS produces same results as left_anti)

---

## Example: Run Record with Enhanced Observability

```json
{
  "run_id": "01KH4SATJ0CWDFWGC8NVQ5HF36",
  "job_id": "form_obs_intake_01",
  "status": "success",
  "duration_ms": 15234,
  "rows_read": 60,
  "rows_written": 429,
  "started_at": "2026-02-12T10:00:00Z",
  "completed_at": "2026-02-12T10:00:15Z",
  "step_timings": [
    {
      "step_id": "read_me",
      "duration_ms": 2134,
      "rows_read": 60,
      "rows_written": null
    },
    {
      "step_id": "prepare",
      "duration_ms": 234,
      "rows_read": null,
      "rows_written": null
    },
    {
      "step_id": "score",
      "duration_ms": 10234,
      "rows_read": 60,
      "rows_written": 429
    },
    {
      "step_id": "shape",
      "duration_ms": 1234,
      "rows_read": 429,
      "rows_written": null
    },
    {
      "step_id": "persist",
      "duration_ms": 234,
      "rows_read": null,
      "rows_written": null
    },
    {
      "step_id": "write",
      "duration_ms": 1164,
      "rows_read": null,
      "rows_written": 429
    }
  ],
  "performance_indicators": {
    "query_efficiency": "good",
    "transform_throughput": 29.1,
    "write_throughput": 376.8,
    "bottleneck_step": "score"
  }
}
```

---

## Success Metrics

- **Parallelization speedup:** 3-4x for independent jobs
- **Formation query speedup:** 95% reduction (no new data), 60%+ reduction (with data)
- **End-to-end performance:** 1000 emails in <5 minutes
- **Observability:** Run records and event_log enable quick performance debugging
- **Test coverage:** All performance tests green (pytest --perf)
