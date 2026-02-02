"""Tests for lorchestra executor module.

Tests the complete executor lifecycle:
JobDef -> JobInstance -> RunRecord -> StepManifest -> AttemptRecord
"""

import json
import pytest
from datetime import datetime, timezone
from pathlib import Path

from lorchestra.schemas import (
    JobDef,
    StepDef,
    JobStepInstance,
    StepManifest,
    AttemptRecord,
    StepStatus,
    Op,
    IdempotencyConfig,
    CompileError,
)

from lorchestra.registry import JobRegistry, JobNotFoundError
from lorchestra.compiler import Compiler, compile_job, _resolve_value, _evaluate_condition
from lorchestra.run_store import InMemoryRunStore, FileRunStore, generate_ulid
from lorchestra.executor import (
    Executor,
    NoOpBackend,
    execute_job,
    _resolve_run_refs,
    _compute_idempotency_key,
)


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def simple_job_def() -> JobDef:
    """A simple job definition for testing."""
    return JobDef(
        job_id="test_job",
        version="2.0",
        steps=(
            StepDef(
                step_id="injest_data",
                op=Op.CALL,
                params={"callable": "injest", "source_system": "test", "object_type": "test_obj"},
            ),
            StepDef(
                step_id="llm_process",
                op=Op.COMPUTE_LLM,
                params={"prompt": "Process data", "input_rows": "@run.injest_data.items"},
            ),
            StepDef(
                step_id="persist",
                op=Op.PLAN_BUILD,
                params={"items": "@run.llm_process.response", "method": "wal.append"},
            ),
        ),
    )


@pytest.fixture
def job_with_conditions() -> JobDef:
    """A job with conditional steps."""
    return JobDef(
        job_id="conditional_job",
        version="2.0",
        steps=(
            StepDef(
                step_id="always_run",
                op=Op.CALL,
                params={"callable": "injest", "source_system": "@ctx.source", "object_type": "default"},
            ),
            StepDef(
                step_id="prod_only",
                op=Op.PLAN_BUILD,
                params={"items": "@run.always_run.items", "method": "wal.append"},
                if_="@ctx.env == 'prod'",
            ),
            StepDef(
                step_id="when_enabled",
                op=Op.COMPUTE_LLM,
                params={"prompt": "process in full mode"},
                if_="@payload.enabled",
            ),
        ),
    )


@pytest.fixture
def temp_definitions_dir(tmp_path) -> Path:
    """Create a temporary definitions directory with test jobs."""
    defs_dir = tmp_path / "definitions"
    defs_dir.mkdir()

    # Simple job
    simple_job = {
        "job_id": "simple_job",
        "version": "2.0",
        "steps": [
            {"step_id": "step1", "op": "call", "params": {"callable": "injest", "source_system": "test", "object_type": "test"}},
        ],
    }
    (defs_dir / "simple_job.json").write_text(json.dumps(simple_job))

    # Job in subdirectory
    ingest_dir = defs_dir / "ingest"
    ingest_dir.mkdir()
    ingest_job = {
        "job_id": "ingest_test",
        "version": "2.0",
        "steps": [
            {"step_id": "fetch", "op": "call", "params": {"callable": "injest", "source_system": "api", "object_type": "data"}},
            {
                "step_id": "persist",
                "op": "plan.build",
                "params": {"items": "@run.fetch.items", "method": "wal.append"},
            },
        ],
    }
    (ingest_dir / "ingest_test.json").write_text(json.dumps(ingest_job))

    return defs_dir


# =============================================================================
# REGISTRY TESTS
# =============================================================================


class TestJobRegistry:
    """Tests for JobRegistry."""

    def test_load_job_from_root(self, temp_definitions_dir):
        """Load a job from root directory."""
        registry = JobRegistry(temp_definitions_dir)
        job = registry.load("simple_job")

        assert job.job_id == "simple_job"
        assert job.version == "2.0"
        assert len(job.steps) == 1

    def test_load_job_from_subdirectory(self, temp_definitions_dir):
        """Load a job from subdirectory."""
        registry = JobRegistry(temp_definitions_dir)
        job = registry.load("ingest_test")

        assert job.job_id == "ingest_test"
        assert len(job.steps) == 2

    def test_load_caches_result(self, temp_definitions_dir):
        """Loading the same job twice returns cached result."""
        registry = JobRegistry(temp_definitions_dir)
        job1 = registry.load("simple_job")
        job2 = registry.load("simple_job")

        assert job1 is job2

    def test_load_nonexistent_raises(self, temp_definitions_dir):
        """Loading a nonexistent job raises JobNotFoundError."""
        registry = JobRegistry(temp_definitions_dir)

        with pytest.raises(JobNotFoundError):
            registry.load("nonexistent")

    def test_list_jobs(self, temp_definitions_dir):
        """List available jobs."""
        registry = JobRegistry(temp_definitions_dir)
        jobs = registry.list_jobs()

        assert "simple_job" in jobs
        assert "ingest_test" in jobs

    def test_compute_hash_deterministic(self, simple_job_def):
        """Hash computation is deterministic."""
        hash1 = JobRegistry.compute_hash(simple_job_def)
        hash2 = JobRegistry.compute_hash(simple_job_def)

        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex

    def test_load_by_hash(self, temp_definitions_dir):
        """Load job by content hash."""
        registry = JobRegistry(temp_definitions_dir)
        job = registry.load("simple_job")
        sha256 = registry.compute_hash(job)

        loaded = registry.load_by_hash(sha256)
        assert loaded is job


# =============================================================================
# COMPILER TESTS
# =============================================================================


class TestCompiler:
    """Tests for the Compiler."""

    def test_compile_simple_job(self, simple_job_def):
        """Compile a simple job without references."""
        instance = compile_job(simple_job_def)

        assert instance.job_id == "test_job"
        assert instance.job_version == "2.0"
        assert len(instance.steps) == 3
        assert instance.job_def_sha256

    def test_compile_resolves_ctx_refs(self, job_with_conditions):
        """Compile resolves @ctx.* references."""
        instance = compile_job(
            job_with_conditions,
            ctx={"source": "my_source", "env": "dev"},
            payload={"enabled": False},  # Must provide payload since job has @payload.enabled condition
        )

        # Check @ctx.source was resolved
        step = instance.get_step("always_run")
        assert step.params["source_system"] == "my_source"

    def test_compile_resolves_payload_refs(self):
        """Compile resolves @payload.* references."""
        job_def = JobDef(
            job_id="test",
            version="1.0.0",
            steps=(
                StepDef(
                    step_id="s1",
                    op=Op.CALL,
                    params={"id": "@payload.entity_id"},
                ),
            ),
        )

        instance = compile_job(job_def, payload={"entity_id": "123"})
        assert instance.get_step("s1").params["id"] == "123"

    def test_compile_preserves_run_refs(self, simple_job_def):
        """Compile preserves @run.* references for runtime."""
        instance = compile_job(simple_job_def)

        llm_step = instance.get_step("llm_process")
        assert llm_step.params["input_rows"] == "@run.injest_data.items"

    def test_compile_evaluates_if_true(self, job_with_conditions):
        """If condition evaluates to true - step not skipped."""
        instance = compile_job(
            job_with_conditions,
            ctx={"source": "test", "env": "prod"},
            payload={"enabled": True},
        )

        prod_step = instance.get_step("prod_only")
        assert prod_step.compiled_skip is False

        enabled_step = instance.get_step("when_enabled")
        assert enabled_step.compiled_skip is False

    def test_compile_evaluates_if_false(self, job_with_conditions):
        """If condition evaluates to false - step is skipped."""
        instance = compile_job(
            job_with_conditions,
            ctx={"source": "test", "env": "dev"},
            payload={"enabled": False},
        )

        prod_step = instance.get_step("prod_only")
        assert prod_step.compiled_skip is True

        enabled_step = instance.get_step("when_enabled")
        assert enabled_step.compiled_skip is True

    def test_get_executable_steps_excludes_skipped(self, job_with_conditions):
        """get_executable_steps filters out skipped steps."""
        instance = compile_job(
            job_with_conditions,
            ctx={"source": "test", "env": "dev"},
            payload={"enabled": False},
        )

        executable = instance.get_executable_steps()
        assert len(executable) == 1
        assert executable[0].step_id == "always_run"


class TestReferenceResolution:
    """Tests for reference resolution functions."""

    def test_resolve_simple_ctx_ref(self):
        """Resolve simple @ctx.key reference."""
        result = _resolve_value("@ctx.name", {"name": "test"}, {})
        assert result == "test"

    def test_resolve_nested_ref(self):
        """Resolve nested @ctx.path.to.value reference."""
        result = _resolve_value(
            "@ctx.config.database.host",
            {"config": {"database": {"host": "localhost"}}},
            {},
        )
        assert result == "localhost"

    def test_resolve_in_dict(self):
        """Resolve references in dict values."""
        result = _resolve_value(
            {"key": "@ctx.value", "static": 123},
            {"value": "resolved"},
            {},
        )
        assert result == {"key": "resolved", "static": 123}

    def test_resolve_in_list(self):
        """Resolve references in list items."""
        result = _resolve_value(
            ["@ctx.a", "@ctx.b"],
            {"a": 1, "b": 2},
            {},
        )
        assert result == [1, 2]

    def test_preserve_run_refs(self):
        """@run.* references are preserved."""
        result = _resolve_value("@run.step1.output", {}, {})
        assert result == "@run.step1.output"


class TestConditionEvaluation:
    """Tests for condition evaluation."""

    def test_simple_truthy(self):
        """Simple reference to truthy value."""
        assert _evaluate_condition("@ctx.enabled", {"enabled": True}, {}) is True
        assert _evaluate_condition("@ctx.enabled", {"enabled": False}, {}) is False

    def test_equality_comparison(self):
        """Equality comparison."""
        assert _evaluate_condition("@ctx.env == 'prod'", {"env": "prod"}, {}) is True
        assert _evaluate_condition("@ctx.env == 'prod'", {"env": "dev"}, {}) is False

    def test_inequality_comparison(self):
        """Inequality comparison."""
        assert _evaluate_condition("@ctx.count > 10", {"count": 20}, {}) is True
        assert _evaluate_condition("@ctx.count > 10", {"count": 5}, {}) is False

    def test_run_ref_in_condition_raises(self):
        """@run.* in condition raises CompileError."""
        with pytest.raises(CompileError, match="@run.* references are not allowed"):
            _evaluate_condition("@run.step1.success", {}, {})


# =============================================================================
# RUN STORE TESTS
# =============================================================================


class TestInMemoryRunStore:
    """Tests for InMemoryRunStore."""

    def test_create_run(self, simple_job_def):
        """Create a run record."""
        store = InMemoryRunStore()
        instance = compile_job(simple_job_def)

        run = store.create_run(instance, envelope={"key": "value"})

        assert run.run_id
        assert run.job_id == "test_job"
        assert run.envelope == {"key": "value"}

    def test_get_run(self, simple_job_def):
        """Retrieve a run record."""
        store = InMemoryRunStore()
        instance = compile_job(simple_job_def)
        created = store.create_run(instance, {})

        retrieved = store.get_run(created.run_id)
        assert retrieved == created

    def test_store_and_get_manifest(self, simple_job_def):
        """Store and retrieve a manifest."""
        store = InMemoryRunStore()
        manifest = StepManifest.from_op(
            run_id="test-run-id",
            step_id="step1",
            op=Op.CALL,
            resolved_params={"source_system": "test", "object_type": "data"},
            idempotency_key="test-key",
        )

        ref = store.store_manifest(manifest)
        retrieved = store.get_manifest(ref)

        assert retrieved == manifest

    def test_store_and_get_output(self):
        """Store and retrieve step output."""
        store = InMemoryRunStore()
        output = {"rows": [{"id": 1}, {"id": 2}]}

        ref = store.store_output("run-123", "step1", output)
        retrieved = store.get_output(ref)

        assert retrieved == output

    def test_store_and_get_attempt(self):
        """Store and retrieve attempt record."""
        store = InMemoryRunStore()
        now = datetime.now(timezone.utc)
        attempt = AttemptRecord(
            run_id="run-123",
            attempt_n=1,
            started_at=now,
            status=StepStatus.PENDING,
        )

        store.store_attempt(attempt)
        retrieved = store.get_attempt("run-123", 1)

        assert retrieved == attempt

    def test_get_latest_attempt(self):
        """Get the latest attempt for a run."""
        store = InMemoryRunStore()
        now = datetime.now(timezone.utc)

        store.store_attempt(AttemptRecord(run_id="run-123", attempt_n=1, started_at=now))
        store.store_attempt(AttemptRecord(run_id="run-123", attempt_n=2, started_at=now))
        store.store_attempt(AttemptRecord(run_id="run-123", attempt_n=3, started_at=now))

        latest = store.get_latest_attempt("run-123")
        assert latest.attempt_n == 3


class TestFileRunStore:
    """Tests for FileRunStore."""

    def test_create_run(self, simple_job_def, tmp_path):
        """Create a run record with file storage."""
        store = FileRunStore(tmp_path)
        instance = compile_job(simple_job_def)

        run = store.create_run(instance, {"key": "value"})

        # Verify file was created
        run_file = tmp_path / "runs" / f"{run.run_id}.json"
        assert run_file.exists()

    def test_persistence(self, simple_job_def, tmp_path):
        """Data persists across store instances."""
        instance = compile_job(simple_job_def)

        # Create with first store
        store1 = FileRunStore(tmp_path)
        run = store1.create_run(instance, {})

        # Retrieve with second store
        store2 = FileRunStore(tmp_path)
        retrieved = store2.get_run(run.run_id)

        assert retrieved.run_id == run.run_id
        assert retrieved.job_id == run.job_id


class TestULIDGeneration:
    """Tests for ULID generation."""

    def test_ulid_format(self):
        """ULID is 26 characters."""
        ulid = generate_ulid()
        assert len(ulid) == 26

    def test_ulid_uniqueness(self):
        """ULIDs are unique."""
        ulids = [generate_ulid() for _ in range(100)]
        assert len(set(ulids)) == 100

    def test_ulid_lexicographic_ordering(self):
        """ULIDs are lexicographically ordered by time."""
        import time
        ulid1 = generate_ulid()
        time.sleep(0.01)
        ulid2 = generate_ulid()

        assert ulid1 < ulid2


# =============================================================================
# EXECUTOR TESTS
# =============================================================================


class TestRunRefResolution:
    """Tests for @run.* reference resolution."""

    def test_resolve_simple_run_ref(self):
        """Resolve @run.step_id.field reference."""
        outputs = {"step1": {"result": "success"}}
        result = _resolve_run_refs("@run.step1.result", outputs)
        assert result == "success"

    def test_resolve_nested_run_ref(self):
        """Resolve @run.step_id.nested.path reference."""
        outputs = {"step1": {"data": {"rows": [1, 2, 3]}}}
        result = _resolve_run_refs("@run.step1.data.rows", outputs)
        assert result == [1, 2, 3]

    def test_resolve_in_complex_structure(self):
        """Resolve @run.* refs in nested dicts/lists."""
        outputs = {"query": {"count": 42}}
        value = {"total": "@run.query.count", "items": ["@run.query.count"]}
        result = _resolve_run_refs(value, outputs)
        assert result == {"total": 42, "items": [42]}

    def test_unknown_step_raises(self):
        """Referencing unknown step raises ValueError."""
        with pytest.raises(ValueError, match="unknown step"):
            _resolve_run_refs("@run.missing.value", {})


class TestIdempotencyKey:
    """Tests for idempotency key computation."""

    def test_run_scope_key(self, simple_job_def):
        """Run-scoped key includes run_id and step_id."""
        instance = compile_job(simple_job_def)
        step = instance.get_step("injest_data")

        key = _compute_idempotency_key(
            "run-123", "injest_data", step, {}, None
        )

        assert "run-123" in key
        assert "injest_data" in key

    def test_semantic_scope_key(self):
        """Semantic-scoped key uses semantic_key_ref."""
        step = JobStepInstance(
            step_id="canonize",
            op=Op.CALL,
            params={"entity_id": "ent-456"},
        )
        idempotency = IdempotencyConfig(
            scope="semantic",
            semantic_key_ref="entity_id",
        )

        key = _compute_idempotency_key(
            "run-123", "canonize", step, {"entity_id": "ent-456"}, idempotency
        )

        assert "semantic:ent-456" in key

    def test_payload_hash_included(self):
        """include_payload_hash adds hash suffix."""
        step = JobStepInstance(
            step_id="canonize",
            op=Op.CALL,
            params={"data": "test"},
        )
        idempotency = IdempotencyConfig(scope="run", include_payload_hash=True)

        key = _compute_idempotency_key(
            "run-123", "canonize", step, {"data": "test"}, idempotency
        )

        # Key should have hash suffix
        parts = key.split(":")
        assert len(parts) == 3  # run_id:step_id:hash


class MockBackend:
    """Mock backend that returns appropriate test data for @run.* resolution."""

    def execute(self, manifest: StepManifest) -> dict:
        """Return mock output based on operation."""
        if manifest.op == Op.COMPUTE_LLM:
            return {"response": "processed", "model": "test", "usage": {}}
        else:
            return {"status": "ok"}


def _mock_handle_call(manifest: StepManifest) -> dict:
    """Mock native call handler."""
    return {"items": [{"id": 1}, {"id": 2}], "stats": {"count": 2}}


def _mock_handle_plan_build(manifest: StepManifest) -> dict:
    """Mock native plan.build handler."""
    return {"plan": {"ops": []}}


def _mock_handle_storacle_submit(manifest: StepManifest) -> dict:
    """Mock native storacle.submit handler."""
    return {"status": "submitted"}


def _make_mock_executor(store) -> Executor:
    """Create an executor with mocked native ops and mock backends for non-native ops."""
    mock = MockBackend()
    backends = {
        "inferometer": mock,
        "orchestration": NoOpBackend(),
    }
    executor = Executor(store=store, backends=backends)
    # Patch native op handlers to avoid calling real callables/storacle
    executor._handle_call = _mock_handle_call
    executor._handle_plan_build = _mock_handle_plan_build
    executor._handle_storacle_submit = _mock_handle_storacle_submit
    return executor


class TestExecutor:
    """Tests for the Executor."""

    def test_execute_simple_job(self, simple_job_def):
        """Execute a simple job successfully."""
        store = InMemoryRunStore()
        executor = _make_mock_executor(store)
        instance = compile_job(simple_job_def)

        result = executor.execute(instance)

        assert result.success
        assert result.run_id
        assert result.attempt.status == StepStatus.COMPLETED

    def test_execute_creates_run_record(self, simple_job_def):
        """Execution creates a run record."""
        store = InMemoryRunStore()
        executor = _make_mock_executor(store)
        instance = compile_job(simple_job_def)

        result = executor.execute(instance, envelope={"test": True})

        run = store.get_run(result.run_id)
        assert run is not None
        assert run.job_id == "test_job"
        assert run.envelope == {"test": True}

    def test_execute_records_step_outcomes(self, simple_job_def):
        """Execution records outcome for each step."""
        store = InMemoryRunStore()
        executor = _make_mock_executor(store)
        instance = compile_job(simple_job_def)

        result = executor.execute(instance)

        assert len(result.attempt.step_outcomes) == 3
        for outcome in result.attempt.step_outcomes:
            assert outcome.status == StepStatus.COMPLETED
            assert outcome.started_at is not None
            assert outcome.completed_at is not None

    def test_execute_skips_compiled_skip_steps(self, job_with_conditions):
        """Steps with compiled_skip=True are skipped."""
        store = InMemoryRunStore()
        executor = _make_mock_executor(store)
        instance = compile_job(
            job_with_conditions,
            ctx={"source": "test", "env": "dev"},
            payload={"enabled": False},
        )

        result = executor.execute(instance)

        # Find skipped step outcomes
        skipped = [o for o in result.attempt.step_outcomes if o.status == StepStatus.SKIPPED]
        assert len(skipped) == 2  # prod_only and when_enabled

    def test_execute_stores_manifests(self, simple_job_def):
        """Execution stores manifest for each step."""
        store = InMemoryRunStore()
        executor = _make_mock_executor(store)
        instance = compile_job(simple_job_def)

        result = executor.execute(instance)

        # Check manifests were stored
        for outcome in result.attempt.step_outcomes:
            if outcome.status == StepStatus.COMPLETED:
                assert outcome.manifest_ref is not None
                manifest = store.get_manifest(outcome.manifest_ref)
                assert manifest is not None

    def test_execute_resolves_run_refs(self, simple_job_def):
        """Execution resolves @run.* references between steps."""
        # Custom backend that tracks resolved params
        class TrackingBackend:
            def __init__(self):
                self.calls = []

            def execute(self, manifest):
                self.calls.append(manifest)
                # Return mock output that enables @run.* resolution
                if manifest.op == Op.COMPUTE_LLM:
                    return {"response": "processed_data", "model": "test", "usage": {}}
                return {"status": "ok"}

        tracking = TrackingBackend()
        backends = {
            "inferometer": tracking,
            "orchestration": NoOpBackend(),
        }

        store = InMemoryRunStore()
        executor = Executor(store=store, backends=backends)
        # Patch native op handlers
        executor._handle_call = lambda m: {"items": [{"id": 1}, {"id": 2}], "stats": {"count": 2}}
        executor._handle_plan_build = lambda m: {"plan": {"ops": []}}
        executor._handle_storacle_submit = lambda m: {"status": "submitted"}
        instance = compile_job(simple_job_def)

        result = executor.execute(instance)
        assert result.success

        # Check llm_process step received resolved params
        llm_call = next(c for c in tracking.calls if c.step_id == "llm_process")
        assert llm_call.resolved_params["input_rows"] == [{"id": 1}, {"id": 2}]


class TestExecuteJobFunction:
    """Tests for the execute_job() function (internal API)."""

    @pytest.fixture(autouse=True)
    def _mock_native_ops(self, monkeypatch):
        """Mock native op handlers on Executor so tests don't need real callables."""
        monkeypatch.setattr(Executor, "_handle_call", lambda self, m: {"items": [{"id": 1}], "stats": {"count": 1}})
        monkeypatch.setattr(Executor, "_handle_plan_build", lambda self, m: {"plan": {"ops": []}})
        monkeypatch.setattr(Executor, "_handle_storacle_submit", lambda self, m: {"status": "submitted"})

    def test_execute_job_compiles_and_runs(self, simple_job_def):
        """execute_job() compiles and executes in one call."""
        mock = MockBackend()
        backends = {"inferometer": mock, "orchestration": NoOpBackend()}
        result = execute_job(simple_job_def, backends=backends)

        assert result.success
        assert result.run_record.job_id == "test_job"

    def test_execute_job_with_context(self, job_with_conditions):
        """execute_job() accepts ctx and payload."""
        mock = MockBackend()
        backends = {"inferometer": mock, "orchestration": NoOpBackend()}
        result = execute_job(
            job_with_conditions,
            ctx={"source": "api", "env": "prod"},
            payload={"enabled": True},
            backends=backends,
        )

        assert result.success
        # All steps should have run (none skipped due to conditions)
        completed = [o for o in result.attempt.step_outcomes if o.status == StepStatus.COMPLETED]
        assert len(completed) == 3

    def test_execute_job_with_envelope(self, simple_job_def):
        """execute_job() passes envelope to runtime."""
        mock = MockBackend()
        backends = {"inferometer": mock, "orchestration": NoOpBackend()}
        result = execute_job(
            simple_job_def,
            envelope={"run_context": "test"},
            backends=backends,
        )

        assert result.run_record.envelope == {"run_context": "test"}


# =============================================================================
# INTEGRATION TESTS
# =============================================================================


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_full_lifecycle(self, temp_definitions_dir):
        """Test complete JobDef -> execution lifecycle."""
        # 1. Load from registry
        registry = JobRegistry(temp_definitions_dir)
        registry.load("ingest_test")  # Verify it loads successfully

        # 2. Compile
        compiler = Compiler(registry)
        instance = compiler.compile("ingest_test")

        assert instance.job_id == "ingest_test"
        assert len(instance.steps) == 2

        # 3. Execute
        store = InMemoryRunStore()
        executor = _make_mock_executor(store)
        result = executor.execute(instance, envelope={"source": "test"})

        assert result.success
        assert len(result.attempt.step_outcomes) == 2

        # 4. Verify artifacts stored
        run = store.get_run(result.run_id)
        assert run is not None

        attempt = store.get_latest_attempt(result.run_id)
        assert attempt is not None
        assert attempt.status == StepStatus.COMPLETED

    def test_file_store_persistence(self, temp_definitions_dir, tmp_path):
        """Test persistence with FileRunStore."""
        registry = JobRegistry(temp_definitions_dir)
        store = FileRunStore(tmp_path / "runs")

        # Execute job
        compiler = Compiler(registry)
        instance = compiler.compile("simple_job")
        executor = _make_mock_executor(store)
        result = executor.execute(instance)

        # Verify files created
        runs_dir = tmp_path / "runs" / "runs"
        assert runs_dir.exists()
        run_files = list(runs_dir.glob("*.json"))
        assert len(run_files) == 1

        # Reload from disk
        store2 = FileRunStore(tmp_path / "runs")
        reloaded = store2.get_run(result.run_id)
        assert reloaded is not None
        assert reloaded.job_id == "simple_job"
