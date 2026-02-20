"""Tests for pipeline runner (e005b-06, e013-03).

Tests cover:
- Pipeline YAML loading (all definitions)
- run_pipeline() with mocked execute()
- stop_on_failure=false: continue on child failure
- stop_on_failure=true: stop on stage failure
- Recursive pipeline execution (daily_all calls sub-pipelines)
- PipelineResult shape
- Smoke namespace passthrough
- Job output capture (@run context)
- Loop directive: iteration, payload binding, reference resolution
- Batch pipeline E2E: query → loop → extraction jobs
"""

from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from lorchestra.pipeline import (
    PipelineResult,
    load_pipeline,
    run_pipeline,
    _is_pipeline,
    _resolve_context_ref,
    _resolve_payload_template,
    _capture_job_output,
    DEFINITIONS_DIR,
)


# ---------------------------------------------------------------------------
# Pipeline YAML loading
# ---------------------------------------------------------------------------


class TestLoadPipeline:
    """Tests for loading pipeline YAML definitions."""

    def test_load_formation(self):
        spec = load_pipeline("pipeline.formation")
        assert spec["pipeline_id"] == "pipeline.formation"
        assert len(spec["stages"]) == 2
        assert spec["stages"][0]["name"] == "measurement_events"
        assert spec["stages"][1]["name"] == "observations"
        assert len(spec["stages"][0]["jobs"]) == 3
        assert len(spec["stages"][1]["jobs"]) == 3
        assert spec["stop_on_failure"] is False

    def test_load_ingest(self):
        spec = load_pipeline("pipeline.ingest")
        assert spec["pipeline_id"] == "pipeline.ingest"
        assert len(spec["stages"]) == 1
        assert spec["stages"][0]["name"] == "ingestion"
        assert len(spec["stages"][0]["jobs"]) == 19

    def test_load_canonize(self):
        spec = load_pipeline("pipeline.canonize")
        assert spec["pipeline_id"] == "pipeline.canonize"
        assert len(spec["stages"]) == 1
        assert len(spec["stages"][0]["jobs"]) == 13

    def test_load_project(self):
        spec = load_pipeline("pipeline.project")
        assert spec["pipeline_id"] == "pipeline.project"
        assert len(spec["stages"]) == 2
        assert spec["stages"][0]["name"] == "bq_to_sqlite_sync"
        assert spec["stages"][1]["name"] == "sqlite_to_markdown"
        assert len(spec["stages"][0]["jobs"]) == 8
        assert len(spec["stages"][1]["jobs"]) == 5

    def test_load_views(self):
        spec = load_pipeline("pipeline.views")
        assert spec["pipeline_id"] == "pipeline.views"
        assert len(spec["stages"]) == 1
        assert len(spec["stages"][0]["jobs"]) == 12

    def test_load_daily_all(self):
        spec = load_pipeline("pipeline.daily_all")
        assert spec["pipeline_id"] == "pipeline.daily_all"
        assert len(spec["stages"]) == 4
        assert spec["stop_on_failure"] is True
        # Each stage has one job (a sub-pipeline)
        for stage in spec["stages"]:
            assert len(stage["jobs"]) == 1

    def test_load_extract_batch(self):
        spec = load_pipeline("pipeline.extract_batch")
        assert spec["pipeline_id"] == "pipeline.extract_batch"
        assert len(spec["stages"]) == 2
        assert spec["stages"][0]["name"] == "query_phase"
        assert spec["stages"][0]["jobs"] == ["peek"]
        # Second stage is a loop
        loop_stage = spec["stages"][1]
        assert "loop" in loop_stage
        assert loop_stage["loop"]["over"] == "@run.peek.items"
        assert len(loop_stage["loop"]["jobs"]) == 3

    def test_load_nonexistent_raises(self):
        with pytest.raises(FileNotFoundError, match="Pipeline definition not found"):
            load_pipeline("pipeline.nonexistent")

    def test_all_pipeline_yamls_have_stages(self):
        """All pipeline YAML definitions have at least one stage."""
        pipeline_ids = [
            "pipeline.formation",
            "pipeline.ingest",
            "pipeline.canonize",
            "pipeline.project",
            "pipeline.views",
            "pipeline.daily_all",
            "pipeline.extract_batch",
        ]
        for pid in pipeline_ids:
            spec = load_pipeline(pid)
            assert len(spec["stages"]) >= 1, f"{pid} has no stages"


# ---------------------------------------------------------------------------
# Pipeline detection
# ---------------------------------------------------------------------------


class TestIsPipeline:
    """Tests for _is_pipeline detection."""

    def test_pipeline_formation_is_pipeline(self):
        assert _is_pipeline("pipeline.formation") is True

    def test_pipeline_daily_all_is_pipeline(self):
        assert _is_pipeline("pipeline.daily_all") is True

    def test_regular_job_is_not_pipeline(self):
        assert _is_pipeline("form_me_intake_01") is False

    def test_nonexistent_is_not_pipeline(self):
        assert _is_pipeline("nonexistent_thing") is False


# ---------------------------------------------------------------------------
# PipelineResult
# ---------------------------------------------------------------------------


class TestPipelineResult:
    """Tests for PipelineResult dataclass."""

    def test_default_result(self):
        r = PipelineResult(pipeline_id="test")
        assert r.success is True
        assert r.total == 0
        assert r.succeeded == 0
        assert r.failed == 0
        assert r.failures == []
        assert r.stopped_early is False

    def test_to_dict(self):
        r = PipelineResult(
            pipeline_id="test",
            success=False,
            total=3,
            succeeded=2,
            failed=1,
            failures=[{"job_id": "bad_job", "error": "boom"}],
            duration_ms=500,
        )
        d = r.to_dict()
        assert d["pipeline_id"] == "test"
        assert d["success"] is False
        assert d["total"] == 3
        assert d["failed"] == 1
        assert "stopped_early" not in d  # only included when True

    def test_to_dict_stopped_early(self):
        r = PipelineResult(pipeline_id="test", stopped_early=True)
        d = r.to_dict()
        assert d["stopped_early"] is True


# ---------------------------------------------------------------------------
# Reference resolution helpers
# ---------------------------------------------------------------------------


class TestResolveContextRef:
    """Tests for _resolve_context_ref."""

    def test_resolve_run_ref(self):
        context = {"@run": {"peek": {"items": [{"id": 1}, {"id": 2}]}}}
        result = _resolve_context_ref("@run.peek.items", context)
        assert result == [{"id": 1}, {"id": 2}]

    def test_resolve_nested_ref(self):
        context = {"@run": {"query": {"result": {"rows": [1, 2, 3]}}}}
        result = _resolve_context_ref("@run.query.result.rows", context)
        assert result == [1, 2, 3]

    def test_missing_job_returns_empty_list(self):
        context = {"@run": {}}
        result = _resolve_context_ref("@run.nonexistent.items", context)
        assert result == []

    def test_missing_field_returns_empty_list(self):
        context = {"@run": {"peek": {"other_field": 123}}}
        result = _resolve_context_ref("@run.peek.items", context)
        assert result == []

    def test_non_run_ref_passes_through(self):
        assert _resolve_context_ref("literal_value", {}) == "literal_value"
        assert _resolve_context_ref(42, {}) == 42


class TestResolvePayloadTemplate:
    """Tests for _resolve_payload_template."""

    def test_item_refs(self):
        template = {"session_id": "@item.session_id", "client_id": "@item.client_id"}
        item = {"session_id": "sess_abc", "client_id": "cli_123"}
        context = {"@run": {}, "@payload": {}}

        resolved = _resolve_payload_template(template, item, context)
        assert resolved == {"session_id": "sess_abc", "client_id": "cli_123"}

    def test_payload_refs(self):
        template = {"model": "@payload.model"}
        item = {}
        context = {"@run": {}, "@payload": {"model": "gpt-4o"}}

        resolved = _resolve_payload_template(template, item, context)
        assert resolved == {"model": "gpt-4o"}

    def test_run_refs(self):
        template = {"prev_output": "@run.step1.result"}
        item = {}
        context = {"@run": {"step1": {"result": "ok"}}, "@payload": {}}

        resolved = _resolve_payload_template(template, item, context)
        assert resolved == {"prev_output": "ok"}

    def test_literal_values(self):
        template = {"mode": "extract", "version": 2}
        item = {}
        context = {"@run": {}, "@payload": {}}

        resolved = _resolve_payload_template(template, item, context)
        assert resolved == {"mode": "extract", "version": 2}

    def test_mixed_refs(self):
        template = {
            "session_id": "@item.session_id",
            "model": "@payload.model",
            "mode": "extract",
        }
        item = {"session_id": "sess_abc"}
        context = {"@run": {}, "@payload": {"model": "gpt-4o"}}

        resolved = _resolve_payload_template(template, item, context)
        assert resolved == {
            "session_id": "sess_abc",
            "model": "gpt-4o",
            "mode": "extract",
        }


class TestCaptureJobOutput:
    """Tests for _capture_job_output."""

    def test_captures_last_step_dict_output(self):
        exec_result = MagicMock()
        exec_result.step_outputs = {
            "envelope": {"job_id": "peek"},
            "read": {"items": [{"id": 1}]},
            "write": {"rows_affected": 1},
        }
        context = {"@run": {}}
        _capture_job_output("peek", exec_result, context)
        assert context["@run"]["peek"] == {"rows_affected": 1}

    def test_skips_non_dict_output(self):
        exec_result = MagicMock()
        exec_result.step_outputs = {
            "envelope": {},
            "submit": [{"jsonrpc": "2.0"}],  # list, not dict
        }
        context = {"@run": {}}
        _capture_job_output("writer", exec_result, context)
        assert "writer" not in context["@run"]

    def test_no_exec_result(self):
        context = {"@run": {}}
        _capture_job_output("job", None, context)
        assert context["@run"] == {}

    def test_empty_step_outputs(self):
        exec_result = MagicMock()
        exec_result.step_outputs = {}
        context = {"@run": {}}
        _capture_job_output("job", exec_result, context)
        assert context["@run"] == {}

    def test_handles_mock_without_step_outputs(self):
        """MagicMock auto-creates step_outputs as MagicMock, not dict."""
        exec_result = MagicMock(spec=[])  # no attributes
        context = {"@run": {}}
        _capture_job_output("job", exec_result, context)
        assert context["@run"] == {}


# ---------------------------------------------------------------------------
# run_pipeline with mocked execute()
# ---------------------------------------------------------------------------


def _mock_execute_success(envelope):
    """Mock execute() that always succeeds."""
    result = MagicMock()
    result.success = True
    result.error = None
    result.run_id = "01MOCK000000000000000000000"
    result.step_outputs = {}
    return result


def _mock_execute_fail_on(fail_job_ids):
    """Create a mock execute() that fails for specific job_ids."""
    def mock_execute(envelope):
        result = MagicMock()
        job_id = envelope["job_id"]
        if job_id in fail_job_ids:
            result.success = False
            result.error = f"{job_id} failed"
        else:
            result.success = True
            result.error = None
        result.run_id = "01MOCK000000000000000000000"
        result.step_outputs = {}
        return result
    return mock_execute


class TestRunPipeline:
    """Tests for run_pipeline() execution."""

    @patch("lorchestra.pipeline.load_pipeline")
    @patch("lorchestra.executor.execute", side_effect=_mock_execute_success)
    def test_formation_runs_all_6_jobs(self, mock_execute, mock_load):
        """pipeline.formation executes all 6 child jobs in order."""
        spec = load_pipeline("pipeline.formation")
        result = run_pipeline(spec)

        assert result.pipeline_id == "pipeline.formation"
        assert result.total == 6
        assert result.succeeded == 6
        assert result.failed == 0
        assert result.success is True
        assert result.stopped_early is False

        # Verify execute() called with correct job_ids
        called_job_ids = [c.args[0]["job_id"] for c in mock_execute.call_args_list]
        assert called_job_ids == [
            "form_me_intake_01", "form_me_intake_02", "form_me_followup",
            "form_obs_intake_01", "form_obs_intake_02", "form_obs_followup",
        ]

    @patch("lorchestra.pipeline.load_pipeline")
    @patch("lorchestra.executor.execute", side_effect=_mock_execute_fail_on({"form_me_intake_02"}))
    def test_continue_on_failure(self, mock_execute, mock_load):
        """stop_on_failure=false: remaining jobs still execute after failure."""
        spec = load_pipeline("pipeline.formation")
        result = run_pipeline(spec)

        assert result.success is False
        assert result.succeeded == 5
        assert result.failed == 1
        assert result.stopped_early is False
        assert len(result.failures) == 1
        assert result.failures[0]["job_id"] == "form_me_intake_02"

        # All 6 jobs should have been attempted
        assert mock_execute.call_count == 6

    @patch("lorchestra.pipeline.load_pipeline")
    @patch("lorchestra.executor.execute", side_effect=_mock_execute_success)
    def test_smoke_namespace_passthrough(self, mock_execute, mock_load):
        """Smoke namespace is passed to child envelopes."""
        spec = load_pipeline("pipeline.formation")
        run_pipeline(spec, smoke_namespace="test_ns")

        for c in mock_execute.call_args_list:
            assert c.args[0]["smoke_namespace"] == "test_ns"

    @patch("lorchestra.pipeline.load_pipeline")
    @patch("lorchestra.executor.execute")
    def test_exception_in_execute_counted_as_failure(self, mock_execute, mock_load):
        """If execute() raises, it counts as a failed job."""
        mock_execute.side_effect = RuntimeError("connection lost")
        spec = {
            "pipeline_id": "test",
            "stages": [{"name": "s1", "jobs": ["job_a"]}],
            "stop_on_failure": False,
        }
        result = run_pipeline(spec)

        assert result.failed == 1
        assert result.success is False
        assert "connection lost" in result.failures[0]["error"]


# ---------------------------------------------------------------------------
# stop_on_failure semantics
# ---------------------------------------------------------------------------


class TestStopOnFailure:
    """Tests for stop_on_failure=true (pipeline.daily_all pattern)."""

    @patch("lorchestra.pipeline.load_pipeline")
    @patch("lorchestra.executor.execute")
    def test_stop_on_failure_stops_at_failed_stage(self, mock_execute, mock_load):
        """stop_on_failure=true: stops after first stage failure."""
        # Simulate: stage 1 succeeds, stage 2 fails
        spec = {
            "pipeline_id": "test_sequential",
            "stages": [
                {"name": "stage_1", "jobs": ["job_a", "job_b"]},
                {"name": "stage_2", "jobs": ["job_c"]},
                {"name": "stage_3", "jobs": ["job_d"]},
            ],
            "stop_on_failure": True,
        }

        mock_execute.side_effect = _mock_execute_fail_on({"job_c"})
        result = run_pipeline(spec)

        assert result.success is False
        assert result.stopped_early is True
        assert result.succeeded == 2  # job_a, job_b
        assert result.failed == 1     # job_c
        # job_d should NOT have been attempted
        called_ids = [c.args[0]["job_id"] for c in mock_execute.call_args_list]
        assert "job_d" not in called_ids

    @patch("lorchestra.pipeline.load_pipeline")
    @patch("lorchestra.executor.execute", side_effect=_mock_execute_success)
    def test_stop_on_failure_all_succeed(self, mock_execute, mock_load):
        """stop_on_failure=true with all successes runs everything."""
        spec = {
            "pipeline_id": "test_sequential",
            "stages": [
                {"name": "stage_1", "jobs": ["job_a"]},
                {"name": "stage_2", "jobs": ["job_b"]},
            ],
            "stop_on_failure": True,
        }
        result = run_pipeline(spec)

        assert result.success is True
        assert result.stopped_early is False
        assert mock_execute.call_count == 2


# ---------------------------------------------------------------------------
# Recursive pipeline (daily_all pattern)
# ---------------------------------------------------------------------------


class TestRecursivePipeline:
    """Tests for pipelines that contain sub-pipelines (daily_all)."""

    @patch("lorchestra.executor.execute", side_effect=_mock_execute_success)
    def test_daily_all_runs_sub_pipelines(self, mock_execute):
        """daily_all loads sub-pipeline specs and runs them recursively."""
        spec = load_pipeline("pipeline.daily_all")
        result = run_pipeline(spec)

        # daily_all has 4 sub-pipelines: ingest(19), canonize(13), formation(6), project(13)
        # Total child execute() calls = 19 + 13 + 6 + 13 = 51
        assert mock_execute.call_count == 51
        # The pipeline itself reports 4 jobs (sub-pipelines)
        assert result.total == 4
        assert result.succeeded == 4
        assert result.success is True

    @patch("lorchestra.executor.execute")
    def test_daily_all_stops_on_sub_pipeline_failure(self, mock_execute):
        """daily_all stops if a sub-pipeline fails (stop_on_failure=true)."""
        # Make one ingest job fail
        mock_execute.side_effect = _mock_execute_fail_on({"ingest_gmail_acct1"})

        spec = load_pipeline("pipeline.daily_all")
        result = run_pipeline(spec)

        assert result.success is False
        assert result.stopped_early is True
        # pipeline.ingest failed, so canonize/formation/project should NOT run
        # Only ingest's 27 jobs were attempted
        assert result.total == 4
        assert result.failed == 1
        assert result.succeeded == 0
        assert result.failures[0]["job_id"] == "pipeline.ingest"


# ---------------------------------------------------------------------------
# Job output capture (@run context)
# ---------------------------------------------------------------------------


class TestJobOutputCapture:
    """Tests for @run context tracking in static stages."""

    @patch("lorchestra.executor.execute")
    def test_static_stage_captures_job_output(self, mock_execute):
        """Static stage jobs capture output into @run context."""
        def mock_with_output(envelope):
            result = MagicMock()
            result.success = True
            result.error = None
            result.run_id = "01MOCK000000000000000000000"
            if envelope["job_id"] == "query_job":
                result.step_outputs = {
                    "envelope": envelope,
                    "read": {"items": [{"id": "a"}, {"id": "b"}]},
                }
            else:
                result.step_outputs = {}
            return result

        mock_execute.side_effect = mock_with_output

        spec = {
            "pipeline_id": "test_capture",
            "stages": [
                {"name": "query", "jobs": ["query_job", "write_job"]},
            ],
        }
        result = run_pipeline(spec)

        assert result.success is True
        assert result.succeeded == 2


# ---------------------------------------------------------------------------
# Loop directive (batch pipeline)
# ---------------------------------------------------------------------------


def _make_batch_mock():
    """Create a mock execute() that simulates a batch pipeline.

    - 'peek' job returns items in step_outputs
    - Extraction jobs succeed and receive payload
    """
    call_log = []

    def mock_execute(envelope):
        call_log.append(dict(envelope))
        result = MagicMock()
        result.success = True
        result.error = None
        result.run_id = "01MOCK000000000000000000000"

        if envelope["job_id"] == "peek":
            result.step_outputs = {
                "envelope": envelope,
                "read": {
                    "items": [
                        {"session_id": "sess_001", "client_id": "cli_A"},
                        {"session_id": "sess_002", "client_id": "cli_B"},
                    ]
                },
            }
        else:
            result.step_outputs = {}
        return result

    return mock_execute, call_log


class TestLoopDirective:
    """Tests for loop stage execution."""

    @patch("lorchestra.executor.execute")
    def test_loop_iterates_over_items(self, mock_execute):
        """Loop stage iterates over @run.peek.items and runs jobs per item."""
        mock_fn, call_log = _make_batch_mock()
        mock_execute.side_effect = mock_fn

        spec = {
            "pipeline_id": "test_batch",
            "stages": [
                {"name": "query", "jobs": ["peek"]},
                {
                    "name": "extract",
                    "loop": {
                        "over": "@run.peek.items",
                        "payload": {
                            "session_id": "@item.session_id",
                            "client_id": "@item.client_id",
                        },
                        "jobs": ["extract_a", "extract_b"],
                    },
                },
            ],
        }
        result = run_pipeline(spec)

        assert result.success is True
        # 1 static job + 2 items × 2 loop jobs = 5 total
        assert result.total == 1 + 4
        assert result.succeeded == 5

    @patch("lorchestra.executor.execute")
    def test_loop_passes_resolved_payload(self, mock_execute):
        """Loop jobs receive resolved @item.* and @payload.* in envelope payload."""
        mock_fn, call_log = _make_batch_mock()
        mock_execute.side_effect = mock_fn

        spec = {
            "pipeline_id": "test_batch",
            "stages": [
                {"name": "query", "jobs": ["peek"]},
                {
                    "name": "extract",
                    "loop": {
                        "over": "@run.peek.items",
                        "payload": {
                            "session_id": "@item.session_id",
                            "client_id": "@item.client_id",
                            "model": "@payload.model",
                        },
                        "jobs": ["extract_job"],
                    },
                },
            ],
        }
        result = run_pipeline(spec, payload={"model": "gpt-4o"})

        assert result.success is True

        # Find extract_job calls (skip the peek call)
        extract_calls = [c for c in call_log if c["job_id"] == "extract_job"]
        assert len(extract_calls) == 2

        # First item: sess_001, cli_A
        assert extract_calls[0]["payload"]["session_id"] == "sess_001"
        assert extract_calls[0]["payload"]["client_id"] == "cli_A"
        assert extract_calls[0]["payload"]["model"] == "gpt-4o"

        # Second item: sess_002, cli_B
        assert extract_calls[1]["payload"]["session_id"] == "sess_002"
        assert extract_calls[1]["payload"]["client_id"] == "cli_B"
        assert extract_calls[1]["payload"]["model"] == "gpt-4o"

    @patch("lorchestra.executor.execute")
    def test_loop_with_empty_items(self, mock_execute):
        """Loop with no items runs no jobs."""
        def mock_fn(envelope):
            result = MagicMock()
            result.success = True
            result.error = None
            result.run_id = "01MOCK000000000000000000000"
            if envelope["job_id"] == "peek":
                result.step_outputs = {
                    "envelope": envelope,
                    "read": {"items": []},
                }
            else:
                result.step_outputs = {}
            return result

        mock_execute.side_effect = mock_fn

        spec = {
            "pipeline_id": "test_empty_loop",
            "stages": [
                {"name": "query", "jobs": ["peek"]},
                {
                    "name": "extract",
                    "loop": {
                        "over": "@run.peek.items",
                        "payload": {"session_id": "@item.session_id"},
                        "jobs": ["extract_job"],
                    },
                },
            ],
        }
        result = run_pipeline(spec)

        assert result.success is True
        assert result.total == 1  # only the peek job
        assert result.succeeded == 1
        assert mock_execute.call_count == 1  # only peek called

    @patch("lorchestra.executor.execute")
    def test_loop_missing_run_ref_skips(self, mock_execute):
        """Loop referencing a non-existent @run entry runs no loop jobs."""
        mock_execute.side_effect = _mock_execute_success

        spec = {
            "pipeline_id": "test_missing_ref",
            "stages": [
                {"name": "query", "jobs": ["some_job"]},
                {
                    "name": "extract",
                    "loop": {
                        "over": "@run.nonexistent.items",
                        "payload": {},
                        "jobs": ["extract_job"],
                    },
                },
            ],
        }
        result = run_pipeline(spec)

        assert result.success is True
        assert result.total == 1  # only the static job
        assert result.succeeded == 1

    @patch("lorchestra.executor.execute")
    def test_loop_failure_counted(self, mock_execute):
        """Failed jobs in loop stages are counted correctly."""
        def mock_fn(envelope):
            result = MagicMock()
            result.run_id = "01MOCK000000000000000000000"
            result.step_outputs = {}

            if envelope["job_id"] == "peek":
                result.success = True
                result.error = None
                result.step_outputs = {
                    "envelope": envelope,
                    "read": {"items": [{"id": 1}, {"id": 2}]},
                }
            elif envelope["job_id"] == "failing_job":
                result.success = False
                result.error = "extract failed"
            else:
                result.success = True
                result.error = None
            return result

        mock_execute.side_effect = mock_fn

        spec = {
            "pipeline_id": "test_loop_fail",
            "stages": [
                {"name": "query", "jobs": ["peek"]},
                {
                    "name": "extract",
                    "loop": {
                        "over": "@run.peek.items",
                        "payload": {},
                        "jobs": ["failing_job"],
                    },
                },
            ],
        }
        result = run_pipeline(spec)

        assert result.success is False
        assert result.failed == 2  # 2 items × 1 failing_job
        assert result.succeeded == 1  # only peek
        assert len(result.failures) == 2

    @patch("lorchestra.executor.execute")
    def test_loop_with_literal_payload_values(self, mock_execute):
        """Literal values in loop payload are passed through unchanged."""
        call_log = []

        def mock_fn(envelope):
            call_log.append(dict(envelope))
            result = MagicMock()
            result.success = True
            result.error = None
            result.run_id = "01MOCK000000000000000000000"
            if envelope["job_id"] == "peek":
                result.step_outputs = {
                    "envelope": envelope,
                    "read": {"items": [{"id": 1}]},
                }
            else:
                result.step_outputs = {}
            return result

        mock_execute.side_effect = mock_fn

        spec = {
            "pipeline_id": "test_literal",
            "stages": [
                {"name": "query", "jobs": ["peek"]},
                {
                    "name": "extract",
                    "loop": {
                        "over": "@run.peek.items",
                        "payload": {
                            "mode": "extract",
                            "version": 2,
                        },
                        "jobs": ["extract_job"],
                    },
                },
            ],
        }
        run_pipeline(spec)

        extract_calls = [c for c in call_log if c["job_id"] == "extract_job"]
        assert len(extract_calls) == 1
        assert extract_calls[0]["payload"]["mode"] == "extract"
        assert extract_calls[0]["payload"]["version"] == 2


# ---------------------------------------------------------------------------
# E2E batch pipeline: query → loop → extraction
# ---------------------------------------------------------------------------


class TestBatchPipelineE2E:
    """End-to-end test simulating the full batch extraction pattern."""

    @patch("lorchestra.executor.execute")
    def test_batch_pipeline_query_then_extract(self, mock_execute):
        """Full batch pipeline: query job → loop → 3 extraction jobs per item."""
        mock_fn, call_log = _make_batch_mock()
        mock_execute.side_effect = mock_fn

        spec = {
            "pipeline_id": "pipeline.extract_batch",
            "stages": [
                {"name": "query_phase", "jobs": ["peek"]},
                {
                    "name": "extraction",
                    "loop": {
                        "over": "@run.peek.items",
                        "payload": {
                            "session_id": "@item.session_id",
                            "client_id": "@item.client_id",
                            "model": "@payload.model",
                        },
                        "jobs": [
                            "llm_extract_evidence",
                            "llm_extract_flash_actions",
                            "llm_extract_session_summary",
                        ],
                    },
                },
            ],
            "stop_on_failure": False,
        }

        result = run_pipeline(spec, payload={"model": "gpt-4o"})

        # Verify overall success
        assert result.success is True
        # 1 peek + (2 items × 3 extraction jobs) = 7 total
        assert result.total == 7
        assert result.succeeded == 7
        assert result.failed == 0

        # Verify execution order: peek first, then 6 extraction jobs
        called_job_ids = [c["job_id"] for c in call_log]
        assert called_job_ids[0] == "peek"
        assert set(called_job_ids[1:]) == {
            "llm_extract_evidence",
            "llm_extract_flash_actions",
            "llm_extract_session_summary",
        }
        # Should be 7 total calls
        assert len(called_job_ids) == 7

        # Verify payload binding for all extraction jobs
        extraction_calls = [c for c in call_log if c["job_id"] != "peek"]
        for ec in extraction_calls:
            assert "payload" in ec
            assert ec["payload"]["session_id"] in ("sess_001", "sess_002")
            assert ec["payload"]["client_id"] in ("cli_A", "cli_B")
            assert ec["payload"]["model"] == "gpt-4o"

        # Verify item ordering: first 3 calls for sess_001, next 3 for sess_002
        assert extraction_calls[0]["payload"]["session_id"] == "sess_001"
        assert extraction_calls[1]["payload"]["session_id"] == "sess_001"
        assert extraction_calls[2]["payload"]["session_id"] == "sess_001"
        assert extraction_calls[3]["payload"]["session_id"] == "sess_002"
        assert extraction_calls[4]["payload"]["session_id"] == "sess_002"
        assert extraction_calls[5]["payload"]["session_id"] == "sess_002"

    @patch("lorchestra.executor.execute")
    def test_batch_pipeline_stop_on_failure(self, mock_execute):
        """Batch pipeline with stop_on_failure stops after loop stage fails."""
        def mock_fn(envelope):
            result = MagicMock()
            result.run_id = "01MOCK000000000000000000000"
            result.step_outputs = {}

            if envelope["job_id"] == "peek":
                result.success = True
                result.error = None
                result.step_outputs = {
                    "envelope": envelope,
                    "read": {"items": [{"id": 1}]},
                }
            elif envelope["job_id"] == "extract_a":
                result.success = False
                result.error = "extract_a failed"
            else:
                result.success = True
                result.error = None
            return result

        mock_execute.side_effect = mock_fn

        spec = {
            "pipeline_id": "test_batch_stop",
            "stages": [
                {"name": "query", "jobs": ["peek"]},
                {
                    "name": "extract",
                    "loop": {
                        "over": "@run.peek.items",
                        "payload": {},
                        "jobs": ["extract_a"],
                    },
                },
                {"name": "post", "jobs": ["cleanup_job"]},
            ],
            "stop_on_failure": True,
        }

        result = run_pipeline(spec)

        assert result.success is False
        assert result.stopped_early is True
        # peek succeeded, extract_a failed, cleanup_job not attempted
        assert result.succeeded == 1
        assert result.failed == 1
        called_ids = [c.args[0]["job_id"] for c in mock_execute.call_args_list]
        assert "cleanup_job" not in called_ids

    @patch("lorchestra.executor.execute")
    def test_batch_pipeline_silent_write_jobs_fine(self, mock_execute):
        """Write-only jobs in loop don't need to produce @run output."""
        def mock_fn(envelope):
            result = MagicMock()
            result.success = True
            result.error = None
            result.run_id = "01MOCK000000000000000000000"

            if envelope["job_id"] == "peek":
                result.step_outputs = {
                    "envelope": envelope,
                    "read": {"items": [{"id": 1}]},
                }
            else:
                # Write-only job: no dict output
                result.step_outputs = {"envelope": envelope}
            return result

        mock_execute.side_effect = mock_fn

        spec = {
            "pipeline_id": "test_silent",
            "stages": [
                {"name": "query", "jobs": ["peek"]},
                {
                    "name": "write",
                    "loop": {
                        "over": "@run.peek.items",
                        "payload": {},
                        "jobs": ["write_job"],
                    },
                },
            ],
        }
        result = run_pipeline(spec)

        assert result.success is True
        assert result.total == 2
        assert result.succeeded == 2
