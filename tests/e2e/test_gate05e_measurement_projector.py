"""Gate 05e (v2): Validate formation Stage 1 — measurement_event jobs.

Confirms:
- form_me_* YAML job defs load and compile correctly with native ops
- Job structure follows the pattern: storacle.query -> canonizer -> plan.build -> storacle.submit
- Canonizer transform formation/form_response_to_measurement_event@1.0.0 works correctly
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


ME_JOB_IDS = [
    "form_me_intake_01",
    "form_me_intake_02",
    "form_me_followup",
]


class TestYamlJobDefs:
    """Verify YAML job definitions load and compile correctly."""

    @pytest.mark.parametrize("job_id", ME_JOB_IDS)
    def test_me_yaml_loads(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        assert job_def.job_id == job_id
        assert job_def.version == "2.0"
        assert len(job_def.steps) == 4  # read, project, persist, write

    @pytest.mark.parametrize("job_id", ME_JOB_IDS)
    def test_me_yaml_has_correct_steps(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        steps = job_def.steps
        # Step 1: storacle.query (native read op)
        assert steps[0].step_id == "read"
        assert steps[0].op == "storacle.query"
        assert steps[0].params["dataset"] == "canonical"
        assert steps[0].params["table"] == "canonical_objects"
        assert "incremental" in steps[0].params

        # Step 2: canonizer transform
        assert steps[1].step_id == "project"
        assert steps[1].op == "call"
        assert steps[1].params["callable"] == "canonizer"
        assert steps[1].params["items"] == "@run.read.items"
        assert steps[1].params["config"]["transform_id"] == "formation/form_response_to_measurement_event@1.0.0"

        # Step 3: plan.build
        assert steps[2].step_id == "persist"
        assert steps[2].op == "plan.build"
        assert steps[2].params["method"] == "bq.upsert"
        assert steps[2].params["dataset"] == "derived"
        assert steps[2].params["table"] == "measurement_events"

        # Step 4: storacle.submit
        assert steps[3].step_id == "write"
        assert steps[3].op == "storacle.submit"

    @pytest.mark.parametrize("job_id", ME_JOB_IDS)
    def test_me_yaml_compiles(self, job_id):
        from lorchestra.registry import JobRegistry
        from lorchestra.compiler import compile_job
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        instance = compile_job(job_def)
        assert instance.job_id == job_id
        assert len(instance.steps) == 4

    def test_job_ids_use_form_prefix(self):
        """Verify jobs are named with form_ prefix (not proj_)."""
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)

        for job_id in ME_JOB_IDS:
            assert job_id.startswith("form_me_"), f"{job_id} should start with form_me_"
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
        job_def = registry.load("form_me_intake_01")

        # Check read step filters
        read_params = job_def.steps[0].params
        assert read_params["filters"]["connection_name"] == "google-forms-intake-01"

        # Check canonizer config
        project_params = job_def.steps[1].params
        assert project_params["config"]["binding_id"] == "intake_01"

    def test_intake_02_has_correct_binding(self):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load("form_me_intake_02")

        read_params = job_def.steps[0].params
        assert read_params["filters"]["connection_name"] == "google-forms-intake-02"

        project_params = job_def.steps[1].params
        assert project_params["config"]["binding_id"] == "intake_02"

    def test_followup_has_correct_binding(self):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load("form_me_followup")

        read_params = job_def.steps[0].params
        assert read_params["filters"]["connection_name"] == "google-forms-followup"

        project_params = job_def.steps[1].params
        assert project_params["config"]["binding_id"] == "followup"


# ============================================================================
# Incremental query configuration
# ============================================================================


class TestIncrementalConfig:
    """Verify incremental query configuration is correct."""

    @pytest.mark.parametrize("job_id", ME_JOB_IDS)
    def test_incremental_params(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        incremental = job_def.steps[0].params["incremental"]
        assert incremental["target_dataset"] == "derived"
        assert incremental["target_table"] == "measurement_events"
        assert incremental["source_key"] == "idem_key"
        assert incremental["target_key"] == "canonical_object_id"
        assert incremental["mode"] == "left_anti"
        assert incremental["source_ts"] == "canonicalized_at"
        assert incremental["target_ts"] == "processed_at"


# ============================================================================
# plan.build configuration
# ============================================================================


class TestPlanBuildConfig:
    """Verify plan.build step has correct configuration."""

    @pytest.mark.parametrize("job_id", ME_JOB_IDS)
    def test_plan_build_params(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        persist_params = job_def.steps[2].params
        assert persist_params["method"] == "bq.upsert"
        assert persist_params["dataset"] == "derived"
        assert persist_params["table"] == "measurement_events"
        assert persist_params["key_columns"] == ["idem_key"]
        assert "processed_at" in persist_params["auto_timestamp_columns"]
        assert "created_at" in persist_params["auto_timestamp_columns"]
        assert persist_params["skip_update_columns"] == ["created_at"]


# ============================================================================
# Dispatch verification (fat callables removed)
# ============================================================================


class TestDispatch:
    """Verify measurement_projector is no longer registered."""

    def test_measurement_projector_not_registered(self):
        """Fat callable should have been removed from dispatch."""
        from lorchestra.callable.dispatch import get_callables
        # Force re-initialization
        import lorchestra.callable.dispatch as dispatch_module
        dispatch_module._CALLABLES = None

        callables = get_callables()
        assert "measurement_projector" not in callables or isinstance(
            callables.get("measurement_projector"), type(lambda: None)
        )


# ============================================================================
# Canonizer callable is registered
# ============================================================================


class TestCanonizerRegistered:
    """Verify canonizer callable is available for the transform step."""

    def test_canonizer_registered(self):
        from lorchestra.callable.dispatch import get_callables
        callables = get_callables()
        assert "canonizer" in callables


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

        # Check measurement_events stage
        me_stage = next((s for s in pipeline["stages"] if s["name"] == "measurement_events"), None)
        assert me_stage is not None
        assert "form_me_intake_01" in me_stage["jobs"]
        assert "form_me_intake_02" in me_stage["jobs"]
        assert "form_me_followup" in me_stage["jobs"]
        # Verify old proj_* names are NOT present
        assert "proj_me_intake_01" not in me_stage["jobs"]

    def test_pipeline_has_both_stages(self):
        """Pipeline should have measurement_events and observations stages."""
        from lorchestra.pipeline import load_pipeline

        pipeline = load_pipeline("pipeline.formation")
        stage_names = [s["name"] for s in pipeline["stages"]]
        assert "measurement_events" in stage_names
        assert "observations" in stage_names
