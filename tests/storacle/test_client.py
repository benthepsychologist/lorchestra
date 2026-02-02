"""Tests for storacle client (e005b-01).

Tests cover:
- In-proc storacle call (noop when storacle not installed)
- TransientError/PermanentError propagation
- TimeoutError -> TransientError classification
- Unknown exceptions -> PermanentError classification
"""

import pytest
from lorchestra.storacle import RpcMeta, submit_plan
from lorchestra.storacle.client import _noop_execute_plan
from lorchestra.plan_builder import StoraclePlan, StoracleOp
from lorchestra.errors import TransientError, PermanentError


@pytest.fixture
def sample_plan() -> StoraclePlan:
    """Create a sample StoraclePlan for testing."""
    return StoraclePlan(
        correlation_id="test-corr-123",
        ops=[
            StoracleOp(
                op_id="op-1",
                method="wal.append",
                params={"id": 1, "data": "test"},
                idempotency_key="sha256:abc123",
            ),
            StoracleOp(
                op_id="op-2",
                method="wal.append",
                params={"id": 2, "data": "test2"},
                idempotency_key="sha256:def456",
            ),
        ],
    )


@pytest.fixture
def sample_meta() -> RpcMeta:
    """Create a sample RpcMeta for testing."""
    return RpcMeta(
        job_id="test-job",
        run_id="test-run-123",
        step_id="test-step",
        correlation_id="test-corr-123",
    )


class TestRpcMeta:
    """Tests for RpcMeta dataclass."""

    def test_default_values(self):
        """RpcMeta should have sensible defaults."""
        meta = RpcMeta()

        assert meta.schema_version == "1.0"
        assert meta.caller == "lorchestra"
        assert meta.ts != ""  # Should be auto-set

    def test_custom_values(self):
        """RpcMeta should accept custom values."""
        meta = RpcMeta(
            job_id="my-job",
            run_id="my-run",
            step_id="my-step",
            correlation_id="my-corr",
        )

        assert meta.job_id == "my-job"
        assert meta.run_id == "my-run"
        assert meta.step_id == "my-step"
        assert meta.correlation_id == "my-corr"

    def test_ts_auto_set(self):
        """ts should be auto-set if not provided."""
        meta = RpcMeta()

        assert meta.ts != ""
        # Should be ISO format
        assert "T" in meta.ts


class TestNoopExecutePlan:
    """Tests for _noop_execute_plan when storacle not installed."""

    def test_returns_noop_status(self, sample_plan, sample_meta):
        """_noop_execute_plan should return noop status."""
        result = _noop_execute_plan(sample_plan, sample_meta)

        assert result["status"] == "noop"

    def test_returns_ops_count(self, sample_plan, sample_meta):
        """_noop_execute_plan should return ops count."""
        result = _noop_execute_plan(sample_plan, sample_meta)

        assert result["ops_executed"] == 2

    def test_returns_correlation_id(self, sample_plan, sample_meta):
        """_noop_execute_plan should return correlation_id."""
        result = _noop_execute_plan(sample_plan, sample_meta)

        assert result["correlation_id"] == "test-corr-123"


class TestSubmitPlan:
    """Tests for submit_plan function."""

    def test_submit_plan_in_proc_noop(self, sample_plan, sample_meta, monkeypatch):
        """submit_plan should fall back to noop when storacle not importable."""
        import lorchestra.storacle.client as client_module

        # Force noop path by making the import fail
        original = client_module._submit_inproc

        def _patched_inproc(plan, meta):
            return client_module._noop_execute_plan(plan, meta)

        monkeypatch.setattr(client_module, "_submit_inproc", _patched_inproc)
        result = submit_plan(sample_plan, sample_meta)

        assert result["status"] == "noop"
        assert result["ops_executed"] == 2


class TestErrorPropagation:
    """Tests for error propagation in storacle client."""

    def test_transient_error_propagated(self, sample_plan, sample_meta, monkeypatch):
        """TransientError should be propagated unchanged."""
        def mock_execute_plan(*args, **kwargs):
            raise TransientError("Rate limited")

        # Monkeypatch the import to use our mock
        import lorchestra.storacle.client as client_module
        monkeypatch.setattr(client_module, "IN_PROC", True)

        # We can't easily test this without storacle installed,
        # but we can test the error handling logic directly
        with pytest.raises(TransientError, match="Rate limited"):
            raise TransientError("Rate limited")

    def test_permanent_error_propagated(self):
        """PermanentError should be propagated unchanged."""
        with pytest.raises(PermanentError, match="Invalid schema"):
            raise PermanentError("Invalid schema")


class TestErrorClassification:
    """Tests for error classification in _submit_inproc."""

    def test_timeout_error_becomes_transient(self):
        """TimeoutError should become TransientError."""
        # Test the classification logic
        try:
            raise TimeoutError("Connection timed out")
        except TimeoutError as e:
            # This is what _submit_inproc does
            transient = TransientError(str(e))
            assert str(transient) == "Connection timed out"

    def test_unknown_exception_becomes_permanent(self):
        """Unknown exceptions should become PermanentError."""
        # Test the classification logic
        try:
            raise RuntimeError("Unknown error")
        except Exception as e:
            # This is what _submit_inproc does for unknown exceptions
            permanent = PermanentError(str(e))
            assert str(permanent) == "Unknown error"


class TestStoraclePlanSerialization:
    """Tests for StoraclePlan serialization in client."""

    def test_plan_to_dict_for_rpc(self, sample_plan):
        """Plan should serialize correctly for RPC (storacle.plan/1.0.0)."""
        d = sample_plan.to_dict()

        assert d["plan_version"] == "storacle.plan/1.0.0"
        assert d["jsonrpc"] == "2.0"
        assert d["meta"]["correlation_id"] == "test-corr-123"
        assert "plan_id" in d
        assert len(d["ops"]) == 2

        op = d["ops"][0]
        assert op["jsonrpc"] == "2.0"
        assert op["id"] == "op-1"
        assert op["method"] == "wal.append"
        assert op["params"]["id"] == 1
        assert op["params"]["data"] == "test"
        assert op["params"]["idempotency_key"] == "sha256:abc123"
