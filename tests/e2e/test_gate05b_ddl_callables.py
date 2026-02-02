"""Gate 05b: Validate view_creator and molt_projector callables.

Confirms:
- view_creator.execute() returns CallableResult with CREATE VIEW SQL
- molt_projector.execute() returns CallableResult with CTAS SQL
- Both callables are registered in dispatch and dispatchable
- All 6 view YAML job defs load and compile correctly
- All 3 molt YAML job defs load and compile correctly
- Full 3-step pipeline produces correct storacle plan
"""

from __future__ import annotations

import pytest

from pathlib import Path

from lorchestra.callable.result import CallableResult
from lorchestra.sql.projections import PROJECTIONS
from lorchestra.sql.molt_projections import MOLT_PROJECTIONS

DEFINITIONS_DIR = Path(__file__).resolve().parent.parent.parent / "lorchestra" / "jobs" / "definitions"


# ============================================================================
# view_creator callable
# ============================================================================


class TestViewCreator:
    """Test view_creator.execute() produces correct SQL items."""

    def test_returns_callable_result(self):
        from lorchestra.callable.view_creator import execute

        result = execute({"projection_name": "proj_clients"})
        assert result["schema_version"] == "1.0"
        assert len(result["items"]) == 1
        assert "sql" in result["items"][0]

    def test_sql_is_create_view(self):
        from lorchestra.callable.view_creator import execute

        result = execute({"projection_name": "proj_clients"})
        sql = result["items"][0]["sql"]
        assert "CREATE OR REPLACE VIEW" in sql
        assert "test-project" in sql
        assert "test_canonical" in sql

    @pytest.mark.parametrize("projection_name", list(PROJECTIONS.keys()))
    def test_all_projections_produce_sql(self, projection_name):
        from lorchestra.callable.view_creator import execute

        result = execute({"projection_name": projection_name})
        assert len(result["items"]) == 1
        sql = result["items"][0]["sql"]
        assert "CREATE OR REPLACE VIEW" in sql

    def test_missing_projection_name_raises(self):
        from lorchestra.callable.view_creator import execute

        with pytest.raises(ValueError, match="Missing required param"):
            execute({})

    def test_unknown_projection_name_raises(self):
        from lorchestra.callable.view_creator import execute

        with pytest.raises(KeyError, match="Unknown projection"):
            execute({"projection_name": "nonexistent_view"})

    def test_stats(self):
        from lorchestra.callable.view_creator import execute

        result = execute({"projection_name": "proj_clients"})
        assert result["stats"] == {"input": 1, "output": 1, "skipped": 0, "errors": 0}


# ============================================================================
# molt_projector callable
# ============================================================================


class TestMoltProjector:
    """Test molt_projector.execute() produces correct CTAS items."""

    def test_returns_callable_result(self):
        from lorchestra.callable.molt_projector import execute

        result = execute({
            "query_name": "context_calendar",
            "sink_project": "molt-chatbot",
            "sink_dataset": "molt",
            "sink_table": "context_calendar",
        })
        assert result["schema_version"] == "1.0"
        assert len(result["items"]) == 1
        assert "sql" in result["items"][0]

    def test_sql_is_ctas(self):
        from lorchestra.callable.molt_projector import execute

        result = execute({
            "query_name": "context_calendar",
            "sink_project": "molt-chatbot",
            "sink_dataset": "molt",
            "sink_table": "context_calendar",
        })
        sql = result["items"][0]["sql"]
        assert "CREATE OR REPLACE TABLE" in sql
        assert "`molt-chatbot.molt.context_calendar`" in sql

    def test_ctas_contains_source_sql(self):
        from lorchestra.callable.molt_projector import execute

        result = execute({
            "query_name": "context_calendar",
            "sink_project": "molt-chatbot",
            "sink_dataset": "molt",
            "sink_table": "context_calendar",
        })
        sql = result["items"][0]["sql"]
        # Source SQL references the source project/dataset
        assert "test-project" in sql
        assert "test_canonical" in sql

    def test_context_emails_with_phi_config(self):
        """context_emails requires phi_clinical.yaml for PHI scrubbing."""
        from lorchestra.callable.molt_projector import execute

        result = execute({
            "query_name": "context_emails",
            "sink_project": "molt-chatbot",
            "sink_dataset": "molt",
            "sink_table": "context_emails",
        })
        sql = result["items"][0]["sql"]
        assert "CREATE OR REPLACE TABLE" in sql
        assert "`molt-chatbot.molt.context_emails`" in sql

    def test_context_actions(self):
        from lorchestra.callable.molt_projector import execute

        result = execute({
            "query_name": "context_actions",
            "sink_project": "molt-chatbot",
            "sink_dataset": "molt",
            "sink_table": "context_actions",
        })
        sql = result["items"][0]["sql"]
        assert "CREATE OR REPLACE TABLE" in sql
        assert "`molt-chatbot.molt.context_actions`" in sql

    def test_missing_params_raises(self):
        from lorchestra.callable.molt_projector import execute

        with pytest.raises(ValueError, match="Missing required params"):
            execute({"query_name": "context_calendar"})

    def test_unknown_query_name_raises(self):
        from lorchestra.callable.molt_projector import execute

        with pytest.raises(KeyError, match="Unknown molt projection"):
            execute({
                "query_name": "nonexistent",
                "sink_project": "p",
                "sink_dataset": "d",
                "sink_table": "t",
            })

    def test_stats(self):
        from lorchestra.callable.molt_projector import execute

        result = execute({
            "query_name": "context_calendar",
            "sink_project": "molt-chatbot",
            "sink_dataset": "molt",
            "sink_table": "context_calendar",
        })
        assert result["stats"] == {"input": 1, "output": 1, "skipped": 0, "errors": 0}


# ============================================================================
# Dispatch integration
# ============================================================================


class TestDispatchIntegration:
    """Verify callables are registered and dispatchable."""

    def test_view_creator_registered(self):
        from lorchestra.callable.dispatch import get_callables
        callables = get_callables()
        assert "view_creator" in callables

    def test_molt_projector_registered(self):
        from lorchestra.callable.dispatch import get_callables
        callables = get_callables()
        assert "molt_projector" in callables

    def test_dispatch_view_creator(self):
        from lorchestra.callable.dispatch import dispatch_callable
        result = dispatch_callable("view_creator", {"projection_name": "proj_clients"})
        assert isinstance(result, CallableResult)
        assert len(result.items) == 1
        assert "sql" in result.items[0]

    def test_dispatch_molt_projector(self):
        from lorchestra.callable.dispatch import dispatch_callable
        result = dispatch_callable("molt_projector", {
            "query_name": "context_calendar",
            "sink_project": "molt-chatbot",
            "sink_dataset": "molt",
            "sink_table": "context_calendar",
        })
        assert isinstance(result, CallableResult)
        assert len(result.items) == 1
        assert "sql" in result.items[0]


# ============================================================================
# YAML job definition loading and compilation
# ============================================================================


VIEW_JOB_IDS = [
    "view_proj_clients",
    "view_proj_sessions",
    "view_proj_transcripts",
    "view_proj_clinical_documents",
    "view_proj_form_responses",
    "view_proj_contact_events",
]

MOLT_JOB_IDS = [
    "sync_molt_emails",
    "sync_molt_calendar",
    "sync_molt_actions",
]


class TestYamlJobDefs:
    """Verify YAML job definitions load and compile correctly."""

    @pytest.mark.parametrize("job_id", VIEW_JOB_IDS)
    def test_view_yaml_loads(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        assert job_def.job_id == job_id
        assert job_def.version == "2.0"
        assert len(job_def.steps) == 3

    @pytest.mark.parametrize("job_id", VIEW_JOB_IDS)
    def test_view_yaml_has_correct_steps(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        steps = job_def.steps
        assert steps[0].op == "call"
        assert steps[0].params["callable"] == "view_creator"
        assert steps[1].op == "plan.build"
        assert steps[1].params["method"] == "bq.execute"
        assert steps[2].op == "storacle.submit"

    @pytest.mark.parametrize("job_id", VIEW_JOB_IDS)
    def test_view_yaml_compiles(self, job_id):
        from lorchestra.registry import JobRegistry
        from lorchestra.compiler import compile_job
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        instance = compile_job(job_def)
        assert instance.job_id == job_id
        assert len(instance.steps) == 3

    @pytest.mark.parametrize("job_id", MOLT_JOB_IDS)
    def test_molt_yaml_loads(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        assert job_def.job_id == job_id
        assert job_def.version == "2.0"
        assert len(job_def.steps) == 3

    @pytest.mark.parametrize("job_id", MOLT_JOB_IDS)
    def test_molt_yaml_has_correct_steps(self, job_id):
        from lorchestra.registry import JobRegistry
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)

        steps = job_def.steps
        assert steps[0].op == "call"
        assert steps[0].params["callable"] == "molt_projector"
        assert steps[1].op == "plan.build"
        assert steps[1].params["method"] == "bq.execute"
        assert steps[2].op == "storacle.submit"

    @pytest.mark.parametrize("job_id", MOLT_JOB_IDS)
    def test_molt_yaml_compiles(self, job_id):
        from lorchestra.registry import JobRegistry
        from lorchestra.compiler import compile_job
        registry = JobRegistry(DEFINITIONS_DIR)
        job_def = registry.load(job_id)
        instance = compile_job(job_def)
        assert instance.job_id == job_id
        assert len(instance.steps) == 3


# ============================================================================
# Pipeline integration: call -> plan.build produces correct plan
# ============================================================================


class TestPipelineIntegration:
    """Test the call -> plan.build pipeline produces valid storacle plans."""

    def test_view_creator_to_plan(self):
        """view_creator items feed into plan.build with bq.execute method."""
        from lorchestra.callable.view_creator import execute
        from lorchestra.plan_builder import build_plan_from_items

        result = execute({"projection_name": "proj_clients"})
        items = result["items"]

        plan = build_plan_from_items(
            items=items,
            correlation_id="test_05b",
            method="bq.execute",
        )

        assert plan.kind == "storacle.plan"
        assert len(plan.ops) == 1
        assert plan.ops[0].method == "bq.execute"
        assert "CREATE OR REPLACE VIEW" in plan.ops[0].params["sql"]
        assert plan.ops[0].idempotency_key is None

    def test_molt_projector_to_plan(self):
        """molt_projector items feed into plan.build with bq.execute method."""
        from lorchestra.callable.molt_projector import execute
        from lorchestra.plan_builder import build_plan_from_items

        result = execute({
            "query_name": "context_calendar",
            "sink_project": "molt-chatbot",
            "sink_dataset": "molt",
            "sink_table": "context_calendar",
        })
        items = result["items"]

        plan = build_plan_from_items(
            items=items,
            correlation_id="test_05b",
            method="bq.execute",
        )

        assert plan.kind == "storacle.plan"
        assert len(plan.ops) == 1
        assert plan.ops[0].method == "bq.execute"
        assert "CREATE OR REPLACE TABLE" in plan.ops[0].params["sql"]
        assert "`molt-chatbot.molt.context_calendar`" in plan.ops[0].params["sql"]
        assert plan.ops[0].idempotency_key is None
