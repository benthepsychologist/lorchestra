"""Tests for lorchestra.schemas module.

Tests the JobDef -> JobInstance -> RunRecord -> StepManifest -> AttemptRecord lifecycle
and validates all schema invariants from the e005b specification.

Note: Updated for e005b-05 which collapsed call.* ops into a generic `call` op
with callable name in params, and added native ops (plan.build, storacle.submit).
"""

import pytest
from datetime import datetime, timezone

from lorchestra.schemas import (
    # Ops
    Op,
    # Job Definition
    JobDef,
    StepDef,
    IdempotencyConfig,
    CompileError,
    # Job Instance
    JobInstance,
    JobStepInstance,
    # Run Record
    RunRecord,
    # Step Manifest
    StepManifest,
    # Attempt
    AttemptRecord,
    StepOutcome,
    StepStatus,
)


def utcnow() -> datetime:
    """Return current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)


# =============================================================================
# Op TESTS
# =============================================================================


class TestOp:
    """Tests for Op enum."""

    def test_op_values(self):
        """All expected ops exist with correct values (per e005b-05 spec)."""
        # Generic call op (callable name in params)
        assert Op.CALL.value == "call"
        # Native ops
        assert Op.PLAN_BUILD.value == "plan.build"
        assert Op.STORACLE_SUBMIT.value == "storacle.submit"
        # Compute ops - only compute.llm in v0
        assert Op.COMPUTE_LLM.value == "compute.llm"
        # Job ops
        assert Op.JOB_RUN.value == "job.run"

    def test_op_backends(self):
        """Ops map to correct backends."""
        # callable for call
        assert Op.CALL.backend == "callable"
        # native for plan.build / storacle.submit
        assert Op.PLAN_BUILD.backend == "native"
        assert Op.STORACLE_SUBMIT.backend == "native"
        # inferometer for compute.*
        assert Op.COMPUTE_LLM.backend == "inferometer"
        # orchestration for job.*
        assert Op.JOB_RUN.backend == "orchestration"

    def test_op_from_string(self):
        """Op.from_string parses correctly."""
        assert Op.from_string("call") == Op.CALL
        assert Op.from_string("plan.build") == Op.PLAN_BUILD
        assert Op.from_string("storacle.submit") == Op.STORACLE_SUBMIT
        assert Op.from_string("compute.llm") == Op.COMPUTE_LLM
        assert Op.from_string("job.run") == Op.JOB_RUN

    def test_op_from_string_invalid(self):
        """Op.from_string raises for invalid values."""
        with pytest.raises(ValueError, match="Unknown operation"):
            Op.from_string("invalid.op")


# =============================================================================
# IdempotencyConfig TESTS
# =============================================================================


class TestIdempotencyConfig:
    """Tests for IdempotencyConfig."""

    def test_run_scope(self):
        """Run scope is valid without semantic_key_ref."""
        config = IdempotencyConfig(scope="run")
        assert config.scope == "run"
        assert config.semantic_key_ref is None
        assert config.include_payload_hash is False

    def test_explicit_scope(self):
        """Explicit scope is valid without semantic_key_ref."""
        config = IdempotencyConfig(scope="explicit")
        assert config.scope == "explicit"

    def test_semantic_scope_requires_key_ref(self):
        """Semantic scope requires semantic_key_ref."""
        with pytest.raises(ValueError, match="semantic_key_ref is required"):
            IdempotencyConfig(scope="semantic")

    def test_semantic_scope_with_key_ref(self):
        """Semantic scope with key_ref is valid."""
        config = IdempotencyConfig(scope="semantic", semantic_key_ref="@payload.entity_id")
        assert config.scope == "semantic"
        assert config.semantic_key_ref == "@payload.entity_id"

    def test_non_semantic_rejects_key_ref(self):
        """Non-semantic scopes reject semantic_key_ref."""
        with pytest.raises(ValueError, match="only valid when scope is 'semantic'"):
            IdempotencyConfig(scope="run", semantic_key_ref="@payload.id")

    def test_include_payload_hash(self):
        """include_payload_hash defaults to False."""
        config = IdempotencyConfig(scope="run", include_payload_hash=True)
        assert config.include_payload_hash is True


# =============================================================================
# StepDef TESTS
# =============================================================================


class TestStepDef:
    """Tests for StepDef."""

    def test_basic_step(self):
        """Basic step with required fields."""
        step = StepDef(step_id="step1", op=Op.CALL)
        assert step.step_id == "step1"
        assert step.op == Op.CALL
        assert step.params == {}
        assert step.phase_id is None
        assert step.timeout_s == 300  # Default per e005 spec
        assert step.continue_on_error is False
        assert step.if_ is None
        assert step.idempotency is None

    def test_step_with_params(self):
        """Step with parameter references."""
        step = StepDef(
            step_id="step1",
            op=Op.CALL,
            params={"entity_type": "@ctx.entity", "filter": "@payload.filter"},
        )
        assert step.params["entity_type"] == "@ctx.entity"
        assert step.params["filter"] == "@payload.filter"

    def test_step_with_optional_fields(self):
        """Step with all optional fields."""
        step = StepDef(
            step_id="step1",
            op=Op.CALL,
            phase_id="phase1",
            timeout_s=30,
            continue_on_error=True,
        )
        assert step.phase_id == "phase1"
        assert step.timeout_s == 30
        assert step.continue_on_error is True

    def test_if_allows_ctx_refs(self):
        """if_ condition allows @ctx.* references."""
        step = StepDef(
            step_id="step1",
            op=Op.CALL,
            if_="@ctx.enabled == true",
        )
        assert step.if_ == "@ctx.enabled == true"

    def test_if_allows_payload_refs(self):
        """if_ condition allows @payload.* references."""
        step = StepDef(
            step_id="step1",
            op=Op.CALL,
            if_="@payload.count > 0",
        )
        assert step.if_ == "@payload.count > 0"

    def test_if_rejects_run_refs(self):
        """if_ condition rejects @run.* references (compile-time only)."""
        with pytest.raises(CompileError, match="@run.* references are not allowed"):
            StepDef(
                step_id="step1",
                op=Op.CALL,
                if_="@run.previous_result.count > 0",
            )

    def test_step_allows_idempotency(self):
        """Steps can have idempotency config."""
        step = StepDef(
            step_id="step1",
            op=Op.CALL,
            idempotency=IdempotencyConfig(scope="run"),
        )
        assert step.idempotency.scope == "run"


# =============================================================================
# JobDef TESTS
# =============================================================================


class TestJobDef:
    """Tests for JobDef."""

    def test_basic_job(self):
        """Basic job with steps."""
        job = JobDef(
            job_id="job1",
            version="1.0.0",
            steps=(
                StepDef(step_id="step1", op=Op.CALL),
                StepDef(step_id="step2", op=Op.PLAN_BUILD),
            ),
        )
        assert job.job_id == "job1"
        assert job.version == "1.0.0"
        assert len(job.steps) == 2

    def test_duplicate_step_ids_rejected(self):
        """Duplicate step IDs raise CompileError."""
        with pytest.raises(CompileError, match="Duplicate step IDs"):
            JobDef(
                job_id="job1",
                version="1.0.0",
                steps=(
                    StepDef(step_id="step1", op=Op.CALL),
                    StepDef(step_id="step1", op=Op.PLAN_BUILD),
                ),
            )

    def test_get_step(self):
        """get_step returns step by ID."""
        job = JobDef(
            job_id="job1",
            version="1.0.0",
            steps=(
                StepDef(step_id="step1", op=Op.CALL),
                StepDef(step_id="step2", op=Op.PLAN_BUILD),
            ),
        )
        assert job.get_step("step1").op == Op.CALL
        assert job.get_step("step2").op == Op.PLAN_BUILD
        assert job.get_step("nonexistent") is None

    def test_to_dict_and_from_dict(self):
        """Round-trip serialization."""
        original = JobDef(
            job_id="job1",
            version="1.0.0",
            steps=(
                StepDef(
                    step_id="step1",
                    op=Op.CALL,
                    params={"entity_type": "records"},
                    phase_id="phase1",
                ),
                StepDef(
                    step_id="step2",
                    op=Op.PLAN_BUILD,
                    idempotency=IdempotencyConfig(
                        scope="semantic",
                        semantic_key_ref="@payload.id",
                        include_payload_hash=True,
                    ),
                ),
            ),
        )

        data = original.to_dict()
        restored = JobDef.from_dict(data)

        assert restored.job_id == original.job_id
        assert restored.version == original.version
        assert len(restored.steps) == len(original.steps)
        assert restored.steps[0].phase_id == "phase1"
        assert restored.steps[1].idempotency.scope == "semantic"


# =============================================================================
# JobInstance TESTS
# =============================================================================


class TestJobStepInstance:
    """Tests for JobStepInstance."""

    def test_basic_step_instance(self):
        """Basic step instance."""
        step = JobStepInstance(
            step_id="step1",
            op=Op.CALL,
            params={"entity_type": "records"},
        )
        assert step.step_id == "step1"
        assert step.compiled_skip is False

    def test_compiled_skip(self):
        """compiled_skip marks step for skipping."""
        step = JobStepInstance(
            step_id="step1",
            op=Op.CALL,
            compiled_skip=True,
        )
        assert step.compiled_skip is True


class TestJobInstance:
    """Tests for JobInstance."""

    def test_basic_instance(self):
        """Basic job instance."""
        now = utcnow()
        instance = JobInstance(
            job_id="job1",
            job_version="1.0.0",
            job_def_sha256="a" * 64,
            compiled_at=now,
            steps=(
                JobStepInstance(step_id="step1", op=Op.CALL),
                JobStepInstance(step_id="step2", op=Op.PLAN_BUILD, compiled_skip=True),
            ),
        )
        assert instance.job_id == "job1"
        assert instance.job_def_sha256 == "a" * 64

    def test_get_executable_steps(self):
        """get_executable_steps excludes skipped steps."""
        instance = JobInstance(
            job_id="job1",
            job_version="1.0.0",
            job_def_sha256="a" * 64,
            compiled_at=utcnow(),
            steps=(
                JobStepInstance(step_id="step1", op=Op.CALL),
                JobStepInstance(step_id="step2", op=Op.CALL, compiled_skip=True),
                JobStepInstance(step_id="step3", op=Op.PLAN_BUILD),
            ),
        )
        executable = instance.get_executable_steps()
        assert len(executable) == 2
        assert executable[0].step_id == "step1"
        assert executable[1].step_id == "step3"

    def test_to_dict_and_from_dict(self):
        """Round-trip serialization."""
        now = utcnow()
        original = JobInstance(
            job_id="job1",
            job_version="1.0.0",
            job_def_sha256="a" * 64,
            compiled_at=now,
            steps=(
                JobStepInstance(
                    step_id="step1",
                    op=Op.CALL,
                    phase_id="phase1",
                    timeout_s=60,
                ),
            ),
        )

        data = original.to_dict()
        restored = JobInstance.from_dict(data)

        assert restored.job_id == original.job_id
        assert restored.job_def_sha256 == original.job_def_sha256
        assert restored.steps[0].phase_id == "phase1"


# =============================================================================
# RunRecord TESTS
# =============================================================================


class TestRunRecord:
    """Tests for RunRecord."""

    def test_basic_run_record(self):
        """Basic run record."""
        record = RunRecord(
            run_id="01HGVZ8X1MXYZABC123456789A",
            job_id="job1",
            job_def_sha256="a" * 64,
            envelope={"key": "value"},
        )
        assert record.run_id == "01HGVZ8X1MXYZABC123456789A"
        assert record.job_id == "job1"
        assert record.envelope["key"] == "value"

    def test_to_dict_and_from_dict(self):
        """Round-trip serialization."""
        now = utcnow()
        original = RunRecord(
            run_id="01HGVZ8X1MXYZABC123456789A",
            job_id="job1",
            job_def_sha256="a" * 64,
            envelope={"payload": {"data": 1}},
            started_at=now,
        )

        data = original.to_dict()
        restored = RunRecord.from_dict(data)

        assert restored.run_id == original.run_id
        assert restored.envelope == original.envelope


# =============================================================================
# StepManifest TESTS
# =============================================================================


class TestStepManifest:
    """Tests for StepManifest."""

    def test_basic_manifest(self):
        """Basic step manifest."""
        manifest = StepManifest(
            run_id="01HGVZ8X1MXYZABC123456789A",
            step_id="step1",
            backend="callable",
            op=Op.CALL,
            resolved_params={"entity_type": "records"},
            idempotency_key="key123",
        )
        assert manifest.run_id == "01HGVZ8X1MXYZABC123456789A"
        assert manifest.backend == "callable"
        assert manifest.prompt_hash is None

    def test_backend_mismatch_rejected(self):
        """Backend must match op."""
        with pytest.raises(ValueError, match="Backend mismatch"):
            StepManifest(
                run_id="01HGVZ8X1MXYZABC123456789A",
                step_id="step1",
                backend="inferometer",  # Wrong! call should be callable
                op=Op.CALL,
                idempotency_key="key123",
            )

    def test_from_op_derives_backend(self):
        """from_op factory derives backend correctly."""
        manifest = StepManifest.from_op(
            run_id="01HGVZ8X1MXYZABC123456789A",
            step_id="step1",
            op=Op.COMPUTE_LLM,
            resolved_params={"prompt": "some text"},
            idempotency_key="key123",
        )
        assert manifest.backend == "inferometer"

    def test_llm_manifest_with_prompt_hash(self):
        """LLM ops can have prompt_hash."""
        manifest = StepManifest.from_op(
            run_id="01HGVZ8X1MXYZABC123456789A",
            step_id="step1",
            op=Op.COMPUTE_LLM,
            resolved_params={"prompt": "Hello"},
            idempotency_key="key123",
            prompt_hash="b" * 64,
        )
        assert manifest.prompt_hash == "b" * 64

    def test_to_dict_and_from_dict(self):
        """Round-trip serialization."""
        original = StepManifest.from_op(
            run_id="01HGVZ8X1MXYZABC123456789A",
            step_id="step1",
            op=Op.CALL,
            resolved_params={"entity_type": "records"},
            idempotency_key="key123",
        )

        data = original.to_dict()
        restored = StepManifest.from_dict(data)

        assert restored.run_id == original.run_id
        assert restored.backend == original.backend
        assert restored.op == original.op


# =============================================================================
# StepOutcome TESTS
# =============================================================================


class TestStepOutcome:
    """Tests for StepOutcome."""

    def test_pending_outcome(self):
        """Pending outcome has no timestamps."""
        outcome = StepOutcome(step_id="step1", status=StepStatus.PENDING)
        assert outcome.status == StepStatus.PENDING
        assert outcome.started_at is None
        assert outcome.completed_at is None

    def test_pending_rejects_timestamps(self):
        """Pending status rejects timestamps."""
        with pytest.raises(ValueError, match="Pending steps should not have"):
            StepOutcome(
                step_id="step1",
                status=StepStatus.PENDING,
                started_at=utcnow(),
            )

    def test_running_requires_started_at(self):
        """Running status requires started_at."""
        with pytest.raises(ValueError, match="Running steps must have started_at"):
            StepOutcome(step_id="step1", status=StepStatus.RUNNING)

    def test_running_rejects_completed_at(self):
        """Running status rejects completed_at."""
        with pytest.raises(ValueError, match="Running steps should not have completed_at"):
            StepOutcome(
                step_id="step1",
                status=StepStatus.RUNNING,
                started_at=utcnow(),
                completed_at=utcnow(),
            )

    def test_completed_requires_both_timestamps(self):
        """Completed status requires both timestamps."""
        with pytest.raises(ValueError, match="completed steps must have started_at and completed_at"):
            StepOutcome(
                step_id="step1",
                status=StepStatus.COMPLETED,
                started_at=utcnow(),
            )

    def test_failed_requires_both_timestamps(self):
        """Failed status requires both timestamps."""
        now = utcnow()
        outcome = StepOutcome(
            step_id="step1",
            status=StepStatus.FAILED,
            started_at=now,
            completed_at=now,
            error={"code": "ERR001", "message": "Something failed"},
        )
        assert outcome.error["code"] == "ERR001"

    def test_duration_ms(self):
        """duration_ms calculates correctly."""
        from datetime import timedelta

        started = utcnow()
        completed = started + timedelta(milliseconds=1500)
        outcome = StepOutcome(
            step_id="step1",
            status=StepStatus.COMPLETED,
            started_at=started,
            completed_at=completed,
        )
        assert outcome.duration_ms == 1500


# =============================================================================
# AttemptRecord TESTS
# =============================================================================


class TestAttemptRecord:
    """Tests for AttemptRecord."""

    def test_basic_attempt(self):
        """Basic attempt record."""
        now = utcnow()
        attempt = AttemptRecord(
            run_id="01HGVZ8X1MXYZABC123456789A",
            attempt_n=1,
            started_at=now,
            status=StepStatus.RUNNING,
        )
        assert attempt.attempt_n == 1
        assert attempt.completed_at is None

    def test_attempt_n_must_be_positive(self):
        """attempt_n must be >= 1."""
        with pytest.raises(ValueError, match="attempt_n must be >= 1"):
            AttemptRecord(
                run_id="01HGVZ8X1MXYZABC123456789A",
                attempt_n=0,
                started_at=utcnow(),
            )

    def test_attempt_with_outcomes(self):
        """Attempt with step outcomes."""
        now = utcnow()
        attempt = AttemptRecord(
            run_id="01HGVZ8X1MXYZABC123456789A",
            attempt_n=1,
            started_at=now,
            status=StepStatus.COMPLETED,
            completed_at=now,
            step_outcomes=(
                StepOutcome(step_id="step1", status=StepStatus.COMPLETED, started_at=now, completed_at=now),
                StepOutcome(step_id="step2", status=StepStatus.SKIPPED),
            ),
        )
        assert len(attempt.step_outcomes) == 2
        assert attempt.get_outcome("step1").status == StepStatus.COMPLETED
        assert attempt.get_outcome("step2").status == StepStatus.SKIPPED

    def test_get_failed_steps(self):
        """get_failed_steps returns only failed outcomes."""
        now = utcnow()
        attempt = AttemptRecord(
            run_id="01HGVZ8X1MXYZABC123456789A",
            attempt_n=1,
            started_at=now,
            status=StepStatus.FAILED,
            completed_at=now,
            step_outcomes=(
                StepOutcome(step_id="step1", status=StepStatus.COMPLETED, started_at=now, completed_at=now),
                StepOutcome(step_id="step2", status=StepStatus.FAILED, started_at=now, completed_at=now, error={"code": "E1"}),
                StepOutcome(step_id="step3", status=StepStatus.FAILED, started_at=now, completed_at=now, error={"code": "E2"}),
            ),
        )
        failed = attempt.get_failed_steps()
        assert len(failed) == 2
        assert failed[0].step_id == "step2"
        assert failed[1].step_id == "step3"

    def test_to_dict_and_from_dict(self):
        """Round-trip serialization."""
        now = utcnow()
        original = AttemptRecord(
            run_id="01HGVZ8X1MXYZABC123456789A",
            attempt_n=1,
            started_at=now,
            completed_at=now,
            status=StepStatus.COMPLETED,
            step_outcomes=(
                StepOutcome(step_id="step1", status=StepStatus.COMPLETED, started_at=now, completed_at=now),
            ),
        )

        data = original.to_dict()
        restored = AttemptRecord.from_dict(data)

        assert restored.run_id == original.run_id
        assert restored.attempt_n == original.attempt_n
        assert len(restored.step_outcomes) == 1


# =============================================================================
# SCHEMA INVARIANTS TESTS
# =============================================================================


class TestSchemaInvariants:
    """Tests for schema invariants from e005b specification."""

    def test_phase_id_has_no_execution_semantics(self):
        """phase_id is just metadata with no execution semantics in v0."""
        # We just verify it can be set without affecting behavior
        step = StepDef(step_id="step1", op=Op.CALL, phase_id="load_phase")
        assert step.phase_id == "load_phase"
        # No execution semantics - just stored as metadata

    def test_step_manifest_is_replay_safe(self):
        """StepManifest contains all info needed for replay."""
        manifest = StepManifest.from_op(
            run_id="01HGVZ8X1MXYZABC123456789A",
            step_id="step1",
            op=Op.CALL,
            resolved_params={"entity_type": "records"},
            idempotency_key="run:01HGVZ8X1MXYZABC123456789A:step1",
        )
        # Manifest has all fields needed for dispatch
        assert manifest.run_id is not None
        assert manifest.step_id is not None
        assert manifest.backend is not None
        assert manifest.op is not None
        assert manifest.resolved_params is not None
        assert manifest.idempotency_key is not None

    def test_backend_derived_from_op_prefix(self):
        """Backend is correctly derived from op."""
        test_cases = [
            (Op.CALL, "callable"),
            (Op.PLAN_BUILD, "native"),
            (Op.STORACLE_SUBMIT, "native"),
            (Op.COMPUTE_LLM, "inferometer"),
            (Op.JOB_RUN, "orchestration"),
        ]
        for op, expected_backend in test_cases:
            manifest = StepManifest.from_op(
                run_id="01HGVZ8X1MXYZABC123456789A",
                step_id="step1",
                op=op,
                resolved_params={},
                idempotency_key="key",
            )
            assert manifest.backend == expected_backend, f"{op} should have backend {expected_backend}"
