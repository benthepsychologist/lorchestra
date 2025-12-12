"""Tests for pipeline composite job definitions.

These tests verify that:
1. All expected child jobs are present in composite definitions
2. No jobs are accidentally dropped when refactoring
3. Composite job structure is valid

The expected job lists here serve as a "snapshot" - any changes to the
composite definitions will cause test failures, forcing explicit review.
"""

import json
from pathlib import Path

import pytest


DEFINITIONS_DIR = Path(__file__).parent.parent / "lorchestra" / "jobs" / "definitions"


def load_composite_job(job_id: str) -> dict:
    """Load a composite job definition by job_id."""
    pipeline_dir = DEFINITIONS_DIR / "pipeline"
    job_path = pipeline_dir / f"{job_id}.json"
    if not job_path.exists():
        raise FileNotFoundError(f"Composite job not found: {job_path}")
    with open(job_path) as f:
        return json.load(f)


def get_all_child_jobs(composite: dict) -> set[str]:
    """Extract all child job IDs from a composite definition."""
    jobs = set()
    for stage in composite.get("stages", []):
        jobs.update(stage.get("jobs", []))
    return jobs


class TestPipelineIngest:
    """Tests for pipeline.ingest composite job."""

    def test_job_exists(self):
        """pipeline.ingest job definition exists."""
        job = load_composite_job("pipeline.ingest")
        assert job["job_id"] == "pipeline.ingest"
        assert job["job_type"] == "composite"

    def test_has_two_stages(self):
        """pipeline.ingest has ingestion and validation stages."""
        job = load_composite_job("pipeline.ingest")
        stages = job.get("stages", [])
        assert len(stages) == 2
        assert stages[0]["name"] == "ingestion"
        assert stages[1]["name"] == "validation"

    def test_ingestion_stage_jobs(self):
        """Ingestion stage contains all expected ingest jobs."""
        job = load_composite_job("pipeline.ingest")
        ingestion_stage = job["stages"][0]
        expected = {
            # Gmail
            "ingest_gmail_acct1",
            "ingest_gmail_acct2",
            "ingest_gmail_acct3",
            # Exchange
            "ingest_exchange_ben_mensio",
            "ingest_exchange_booking_mensio",
            "ingest_exchange_info_mensio",
            # Dataverse
            "ingest_dataverse_contacts",
            "ingest_dataverse_sessions",
            "ingest_dataverse_reports",
            # Stripe
            "ingest_stripe_customers",
            "ingest_stripe_invoices",
            "ingest_stripe_payment_intents",
            "ingest_stripe_refunds",
            # Google Forms
            "ingest_google_forms_intake_01",
            "ingest_google_forms_intake_02",
            "ingest_google_forms_followup",
            "ingest_google_forms_ipip120",
        }
        actual = set(ingestion_stage.get("jobs", []))
        assert actual == expected, f"Missing: {expected - actual}, Extra: {actual - expected}"

    def test_validation_stage_jobs(self):
        """Validation stage contains all expected validate jobs."""
        job = load_composite_job("pipeline.ingest")
        validation_stage = job["stages"][1]
        expected = {
            "validate_gmail_source",
            "validate_exchange_source",
            "validate_google_forms_source",
            "validate_dataverse_contacts",
            "validate_dataverse_sessions",
            "validate_dataverse_reports",
            "validate_stripe_customers",
            "validate_stripe_invoices",
            "validate_stripe_payment_intents",
            "validate_stripe_refunds",
        }
        actual = set(validation_stage.get("jobs", []))
        assert actual == expected, f"Missing: {expected - actual}, Extra: {actual - expected}"

    def test_stop_on_failure_is_false(self):
        """Phase composites run all children even if some fail."""
        job = load_composite_job("pipeline.ingest")
        assert job.get("stop_on_failure") is False


class TestPipelineCanonize:
    """Tests for pipeline.canonize composite job."""

    def test_job_exists(self):
        """pipeline.canonize job definition exists."""
        job = load_composite_job("pipeline.canonize")
        assert job["job_id"] == "pipeline.canonize"
        assert job["job_type"] == "composite"

    def test_canonization_jobs(self):
        """Canonization stage contains all expected canonize jobs."""
        job = load_composite_job("pipeline.canonize")
        expected = {
            # Email
            "canonize_gmail_jmap",
            "canonize_exchange_jmap",
            # Dataverse
            "canonize_dataverse_contacts",
            "canonize_dataverse_sessions",
            "canonize_dataverse_transcripts",
            "canonize_dataverse_session_notes",
            "canonize_dataverse_session_summaries",
            "canonize_dataverse_reports",
            # Stripe
            "canonize_stripe_customers",
            "canonize_stripe_invoices",
            "canonize_stripe_payment_intents",
            "canonize_stripe_refunds",
            # Forms
            "canonize_google_forms",
        }
        actual = get_all_child_jobs(job)
        assert actual == expected, f"Missing: {expected - actual}, Extra: {actual - expected}"

    def test_stop_on_failure_is_false(self):
        """Phase composites run all children even if some fail."""
        job = load_composite_job("pipeline.canonize")
        assert job.get("stop_on_failure") is False


class TestPipelineFormation:
    """Tests for pipeline.formation composite job."""

    def test_job_exists(self):
        """pipeline.formation job definition exists."""
        job = load_composite_job("pipeline.formation")
        assert job["job_id"] == "pipeline.formation"
        assert job["job_type"] == "composite"

    def test_has_two_stages(self):
        """pipeline.formation has measurement_events and observations stages."""
        job = load_composite_job("pipeline.formation")
        stages = job.get("stages", [])
        assert len(stages) == 2
        assert stages[0]["name"] == "measurement_events"
        assert stages[1]["name"] == "observations"

    def test_measurement_events_jobs(self):
        """Measurement events stage contains expected jobs."""
        job = load_composite_job("pipeline.formation")
        me_stage = job["stages"][0]
        expected = {
            "proj_me_intake_01",
            "proj_me_intake_02",
            "proj_me_followup",
        }
        actual = set(me_stage.get("jobs", []))
        assert actual == expected, f"Missing: {expected - actual}, Extra: {actual - expected}"

    def test_observations_jobs(self):
        """Observations stage contains expected jobs."""
        job = load_composite_job("pipeline.formation")
        obs_stage = job["stages"][1]
        expected = {
            "proj_obs_intake_01",
            "proj_obs_intake_02",
            "proj_obs_followup",
        }
        actual = set(obs_stage.get("jobs", []))
        assert actual == expected, f"Missing: {expected - actual}, Extra: {actual - expected}"


class TestPipelineProject:
    """Tests for pipeline.project composite job."""

    def test_job_exists(self):
        """pipeline.project job definition exists."""
        job = load_composite_job("pipeline.project")
        assert job["job_id"] == "pipeline.project"
        assert job["job_type"] == "composite"

    def test_has_two_stages(self):
        """pipeline.project has sync and file projection stages."""
        job = load_composite_job("pipeline.project")
        stages = job.get("stages", [])
        assert len(stages) == 2
        assert stages[0]["name"] == "bq_to_sqlite_sync"
        assert stages[1]["name"] == "sqlite_to_markdown"

    def test_sync_stage_jobs(self):
        """BQ to SQLite sync stage contains expected jobs."""
        job = load_composite_job("pipeline.project")
        sync_stage = job["stages"][0]
        expected = {
            "sync_proj_clients",
            "sync_proj_sessions",
            "sync_proj_transcripts",
            "sync_proj_clinical_documents",
            "sync_proj_form_responses",
            "sync_proj_contact_events",
            "sync_measurement_events",
            "sync_observations",
        }
        actual = set(sync_stage.get("jobs", []))
        assert actual == expected, f"Missing: {expected - actual}, Extra: {actual - expected}"

    def test_file_projection_jobs(self):
        """SQLite to markdown stage contains expected jobs."""
        job = load_composite_job("pipeline.project")
        file_stage = job["stages"][1]
        expected = {
            "file_proj_clients",
            "file_proj_transcripts",
            "file_proj_session_notes",
            "file_proj_session_summaries",
            "file_proj_reports",
        }
        actual = set(file_stage.get("jobs", []))
        assert actual == expected, f"Missing: {expected - actual}, Extra: {actual - expected}"


class TestPipelineViews:
    """Tests for pipeline.views composite job."""

    def test_job_exists(self):
        """pipeline.views job definition exists."""
        job = load_composite_job("pipeline.views")
        assert job["job_id"] == "pipeline.views"
        assert job["job_type"] == "composite"

    def test_view_creation_jobs(self):
        """View creation stage contains expected jobs."""
        job = load_composite_job("pipeline.views")
        expected = {
            "view_proj_clients",
            "view_proj_sessions",
            "view_proj_transcripts",
            "view_proj_clinical_documents",
            "view_proj_form_responses",
            "view_proj_contact_events",
        }
        actual = get_all_child_jobs(job)
        assert actual == expected, f"Missing: {expected - actual}, Extra: {actual - expected}"


class TestPipelineDailyAll:
    """Tests for pipeline.daily_all composite job."""

    def test_job_exists(self):
        """pipeline.daily_all job definition exists."""
        job = load_composite_job("pipeline.daily_all")
        assert job["job_id"] == "pipeline.daily_all"
        assert job["job_type"] == "composite"

    def test_has_four_phases(self):
        """pipeline.daily_all runs 4 phases in order."""
        job = load_composite_job("pipeline.daily_all")
        stages = job.get("stages", [])
        assert len(stages) == 4
        assert stages[0]["name"] == "pipeline.ingest"
        assert stages[1]["name"] == "pipeline.canonize"
        assert stages[2]["name"] == "pipeline.formation"
        assert stages[3]["name"] == "pipeline.project"

    def test_phases_are_composites(self):
        """Each phase runs a single composite job."""
        job = load_composite_job("pipeline.daily_all")
        for stage in job["stages"]:
            assert len(stage["jobs"]) == 1
            assert stage["jobs"][0] == stage["name"]

    def test_stop_on_failure_is_true(self):
        """pipeline.daily_all stops on first failed phase."""
        job = load_composite_job("pipeline.daily_all")
        assert job.get("stop_on_failure") is True

    def test_views_not_included(self):
        """pipeline.views is NOT included (one-time setup, not daily)."""
        job = load_composite_job("pipeline.daily_all")
        all_jobs = get_all_child_jobs(job)
        assert "pipeline.views" not in all_jobs


class TestCompositeResultSchema:
    """Tests that CompositeResult matches expected schema."""

    def test_composite_result_to_dict(self):
        """CompositeResult.to_dict() produces expected schema."""
        from lorchestra.processors.composite import CompositeResult

        result = CompositeResult(
            job_id="pipeline.ingest",
            success=False,
            total=23,
            succeeded=21,
            failed=2,
            failures=[
                {"job_id": "ingest_gmail_acct3", "error": "Connection timeout"},
                {"job_id": "validate_stripe_refunds", "error": "Invalid data format"},
            ],
            duration_ms=45321,
        )

        d = result.to_dict()
        assert d["job_id"] == "pipeline.ingest"
        assert d["success"] is False
        assert d["total"] == 23
        assert d["succeeded"] == 21
        assert d["failed"] == 2
        assert len(d["failures"]) == 2
        assert d["duration_ms"] == 45321
        # Sequential fields should not be present for phase composites
        assert "phases_completed" not in d
        assert "failed_phase" not in d
        assert "phase_results" not in d

    def test_sequential_composite_result(self):
        """Sequential composite result includes phase fields."""
        from lorchestra.processors.composite import CompositeResult

        result = CompositeResult(
            job_id="pipeline.daily_all",
            success=False,
            total=4,
            succeeded=2,
            failed=1,
            failures=[{"job_id": "pipeline.formation", "error": "Phase failed"}],
            duration_ms=123456,
            phases_completed=["pipeline.ingest", "pipeline.canonize"],
            failed_phase="pipeline.formation",
            phase_results={
                "pipeline.ingest": {"success": True},
                "pipeline.canonize": {"success": True},
                "pipeline.formation": {"success": False},
            },
        )

        d = result.to_dict()
        assert d["phases_completed"] == ["pipeline.ingest", "pipeline.canonize"]
        assert d["failed_phase"] == "pipeline.formation"
        assert "pipeline.ingest" in d["phase_results"]
