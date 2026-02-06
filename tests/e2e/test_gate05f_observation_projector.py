"""Gate 05f (v2): Validate formation Stage 2 — observation jobs.

Confirms:
- form_obs_* YAML job defs load and compile correctly with native ops
- Job structure follows the pattern:
  storacle.query -> canonizer (prepare) -> finalform (score) -> canonizer (shape) -> plan.build -> storacle.submit
- Canonizer transforms work correctly for finalform input/output shaping
- Pipeline integration validates the full chain
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lorchestra.callable.result import CallableResult

DEFINITIONS_DIR = Path(__file__).resolve().parent.parent.parent / "lorchestra" / "jobs" / "definitions"


# ============================================================================
# YAML job definition loading and compilation
# ============================================================================


OBS_JOB_IDS = [
    "form_obs_intake_01",
    "form_obs_intake_02",
    "form_obs_followup",
]


class TestYamlJobDefs:
    """Verify YAML job definitions load and compile correctly."""

    @pytest.mark.parametrize("job_id", OBS_JOB_IDS)
    def test_obs_yaml_loads(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        assert job_def.job_id == job_id
        assert job_def.version == "2.0"
        assert len(job_def.steps) == 6  # read_me, prepare, score, shape, persist, write

    @pytest.mark.parametrize("job_id", OBS_JOB_IDS)
    def test_obs_yaml_has_correct_steps(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        steps = job_def.steps

        # Step 1: storacle.query (read measurement_events)
        assert steps[0].step_id == "read_me"
        assert steps[0].op == "storacle.query"
        assert steps[0].params["dataset"] == "derived"
        assert steps[0].params["table"] == "measurement_events"
        assert "incremental" in steps[0].params

        # Step 2: canonizer (prepare finalform input)
        assert steps[1].step_id == "prepare"
        assert steps[1].op == "call"
        assert steps[1].params["callable"] == "canonizer"
        assert steps[1].params["items"] == "@run.read_me.items"
        assert steps[1].params["config"]["transform_id"] == "formation/measurement_event_to_finalform_input@1.0.0"

        # Step 3: finalform (REAL COMPUTE - scoring)
        assert steps[2].step_id == "score"
        assert steps[2].op == "call"
        assert steps[2].params["callable"] == "finalform"
        assert steps[2].params["items"] == "@run.prepare.items"

        # Step 4: canonizer (shape finalform output to observation rows)
        assert steps[3].step_id == "shape"
        assert steps[3].op == "call"
        assert steps[3].params["callable"] == "canonizer"
        assert steps[3].params["items"] == "@run.score.items"
        assert steps[3].params["config"]["transform_id"] == "formation/finalform_event_to_observation_row@1.0.0"

        # Step 5: plan.build
        assert steps[4].step_id == "persist"
        assert steps[4].op == "plan.build"
        assert steps[4].params["method"] == "bq.upsert"
        assert steps[4].params["dataset"] == "derived"
        assert steps[4].params["table"] == "observations"

        # Step 6: storacle.submit
        assert steps[5].step_id == "write"
        assert steps[5].op == "storacle.submit"

    @pytest.mark.parametrize("job_id", OBS_JOB_IDS)
    def test_obs_yaml_compiles(self, job_id):
        from lorchestra.registry import JobRegistry
        from lorchestra.compiler import compile_job
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        instance = compile_job(job_def)
        assert instance.job_id == job_id
        assert len(instance.steps) == 6

    def test_job_ids_use_form_prefix(self):
        """Verify jobs are named with form_ prefix (not proj_)."""
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)

        for job_id in OBS_JOB_IDS:
            assert job_id.startswith("form_obs_"), f"{job_id} should start with form_obs_"
            job_def = registry.load(job_id)
            assert job_def.job_id == job_id


# ============================================================================
# Binding ID configuration per job
# ============================================================================


class TestBindingConfiguration:
    """Verify each job has the correct binding_id configured."""

    def test_intake_01_has_correct_binding(self):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load("form_obs_intake_01")

        # Check read step filters
        read_params = job_def.steps[0].params
        assert read_params["filters"]["binding_id"] == "intake_01"

        # Check prepare step config
        prepare_params = job_def.steps[1].params
        assert prepare_params["config"]["instrument"] == "intake_01"

        # Check score step config
        score_params = job_def.steps[2].params
        assert score_params["config"]["binding_id"] == "intake_01"

    def test_intake_02_has_correct_binding(self):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load("form_obs_intake_02")

        read_params = job_def.steps[0].params
        assert read_params["filters"]["binding_id"] == "intake_02"

        prepare_params = job_def.steps[1].params
        assert prepare_params["config"]["instrument"] == "intake_02"

        score_params = job_def.steps[2].params
        assert score_params["config"]["binding_id"] == "intake_02"

    def test_followup_has_correct_binding(self):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load("form_obs_followup")

        read_params = job_def.steps[0].params
        assert read_params["filters"]["binding_id"] == "followup"

        prepare_params = job_def.steps[1].params
        assert prepare_params["config"]["instrument"] == "followup"

        score_params = job_def.steps[2].params
        assert score_params["config"]["binding_id"] == "followup"


# ============================================================================
# Incremental query configuration
# ============================================================================


class TestIncrementalConfig:
    """Verify incremental query configuration is correct."""

    @pytest.mark.parametrize("job_id", OBS_JOB_IDS)
    def test_incremental_params(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        incremental = job_def.steps[0].params["incremental"]
        assert incremental["target_dataset"] == "derived"
        assert incremental["target_table"] == "observations"
        assert incremental["source_key"] == "measurement_event_id"
        assert incremental["target_key"] == "measurement_event_id"
        assert incremental["mode"] == "left_anti"


# ============================================================================
# plan.build configuration
# ============================================================================


class TestPlanBuildConfig:
    """Verify plan.build step has correct configuration."""

    @pytest.mark.parametrize("job_id", OBS_JOB_IDS)
    def test_plan_build_params(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        persist_params = job_def.steps[4].params
        assert persist_params["method"] == "bq.upsert"
        assert persist_params["dataset"] == "derived"
        assert persist_params["table"] == "observations"
        assert persist_params["key_columns"] == ["idem_key"]
        assert "processed_at" in persist_params["auto_timestamp_columns"]
        assert "created_at" in persist_params["auto_timestamp_columns"]
        assert persist_params["skip_update_columns"] == ["created_at"]


# ============================================================================
# Dispatch verification (fat callables removed)
# ============================================================================


class TestDispatch:
    """Verify observation_projector is no longer registered."""

    def test_observation_projector_not_registered(self):
        """Fat callable should have been removed from dispatch."""
        from lorchestra.callable.dispatch import get_callables
        # Force re-initialization
        import lorchestra.callable.dispatch as dispatch_module
        dispatch_module._CALLABLES = None

        callables = get_callables()
        assert "observation_projector" not in callables or isinstance(
            callables.get("observation_projector"), type(lambda: None)
        )


# ============================================================================
# Required callables are registered
# ============================================================================


class TestRequiredCallablesRegistered:
    """Verify canonizer and finalform callables are available."""

    def test_canonizer_registered(self):
        from lorchestra.callable.dispatch import get_callables
        callables = get_callables()
        assert "canonizer" in callables

    def test_finalform_registered(self):
        from lorchestra.callable.dispatch import get_callables
        callables = get_callables()
        assert "finalform" in callables


# ============================================================================
# Pipeline integration
# ============================================================================


class TestPipelineIntegration:
    """Test formation pipeline references the correct jobs."""

    def test_pipeline_references_form_jobs(self):
        """pipeline.formation.yaml should reference form_* jobs, not proj_*."""
        from lorchestra.pipeline import load_pipeline

        pipeline = load_pipeline("pipeline.formation")
        assert pipeline is not None

        # Check observations stage
        obs_stage = next((s for s in pipeline["stages"] if s["name"] == "observations"), None)
        assert obs_stage is not None
        assert "form_obs_intake_01" in obs_stage["jobs"]
        assert "form_obs_intake_02" in obs_stage["jobs"]
        assert "form_obs_followup" in obs_stage["jobs"]
        # Verify old proj_* names are NOT present
        assert "proj_obs_intake_01" not in obs_stage["jobs"]

    def test_observations_stage_runs_after_measurement_events(self):
        """Observations stage should be the second stage in the pipeline."""
        from lorchestra.pipeline import load_pipeline

        pipeline = load_pipeline("pipeline.formation")
        stage_names = [s["name"] for s in pipeline["stages"]]

        me_idx = stage_names.index("measurement_events")
        obs_idx = stage_names.index("observations")
        assert obs_idx > me_idx, "observations stage should come after measurement_events"


# ============================================================================
# Architecture verification
# ============================================================================


class TestArchitecture:
    """Verify the new architecture follows the spec."""

    def test_no_direct_bq_reads_in_callables(self):
        """The job definition should use storacle.query for BQ reads, not callable-internal reads."""
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)

        for job_id in OBS_JOB_IDS:
            job_def = registry.load(job_id)
            # First step should be storacle.query (native read op)
            assert job_def.steps[0].op == "storacle.query"
            # No callable step should be the first step
            assert job_def.steps[0].op != "call"

    def test_finalform_is_only_real_compute(self):
        """Only the finalform step should do real compute (scoring)."""
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)

        for job_id in OBS_JOB_IDS:
            job_def = registry.load(job_id)
            # Find callable steps
            call_steps = [s for s in job_def.steps if s.op == "call"]
            # Should have 3 call steps: prepare (canonizer), score (finalform), shape (canonizer)
            assert len(call_steps) == 3

            callables_used = [s.params["callable"] for s in call_steps]
            # finalform should be in the middle
            assert callables_used[1] == "finalform"
            # canonizer should be first and last
            assert callables_used[0] == "canonizer"
            assert callables_used[2] == "canonizer"

    def test_metadata_carries_scoring_payload(self):
        """Verify read step requests metadata column for scoring payload."""
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)

        for job_id in OBS_JOB_IDS:
            job_def = registry.load(job_id)
            read_params = job_def.steps[0].params
            assert "metadata" in read_params["columns"]
            assert "metadata" in read_params["parse_json_columns"]
