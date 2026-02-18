"""Tests for pipeline runner (e005b-06).

Tests cover:
- Pipeline YAML loading (all 6 definitions)
- run_pipeline() with mocked execute()
- stop_on_failure=false: continue on child failure
- stop_on_failure=true: stop on stage failure
- Recursive pipeline execution (daily_all calls sub-pipelines)
- PipelineResult shape
- Smoke namespace passthrough
"""

from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from lorchestra.pipeline import (
    PipelineResult,
    load_pipeline,
    run_pipeline,
    _is_pipeline,
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
        assert len(spec["stages"][0]["jobs"]) == 11

    def test_load_daily_all(self):
        spec = load_pipeline("pipeline.daily_all")
        assert spec["pipeline_id"] == "pipeline.daily_all"
        assert len(spec["stages"]) == 4
        assert spec["stop_on_failure"] is True
        # Each stage has one job (a sub-pipeline)
        for stage in spec["stages"]:
            assert len(stage["jobs"]) == 1

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
# run_pipeline with mocked execute()
# ---------------------------------------------------------------------------


def _mock_execute_success(envelope):
    """Mock execute() that always succeeds."""
    result = MagicMock()
    result.success = True
    result.error = None
    result.run_id = "01MOCK000000000000000000000"
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
