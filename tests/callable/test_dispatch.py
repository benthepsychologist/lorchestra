"""Tests for callable dispatch (e005b-01).

Tests cover:
- CALLABLES map dispatch
- Exception propagation (not classification)
- Register callable for testing
"""

import pytest
from lorchestra.callable import CallableResult, dispatch_callable, register_callable
from lorchestra.callable.dispatch import get_callables
from lorchestra.schemas.ops import Op
from lorchestra.errors import TransientError, PermanentError


class TestDispatchCallable:
    """Tests for dispatch_callable function."""

    def test_dispatch_returns_callable_result(self):
        """dispatch_callable should return CallableResult."""
        # Register a mock callable
        def mock_injest(params: dict) -> dict:
            return {"items": [{"id": 1}], "stats": {"count": 1}}

        register_callable(Op.CALL_INJEST, mock_injest)

        result = dispatch_callable(Op.CALL_INJEST, {"source": "test"})

        assert isinstance(result, CallableResult)
        assert result.items == [{"id": 1}]
        assert result.stats == {"count": 1}

    def test_dispatch_non_call_op_raises(self):
        """dispatch_callable should raise for non-call.* ops."""
        with pytest.raises(ValueError, match="only handles call"):
            dispatch_callable(Op.COMPUTE_LLM, {})

    def test_dispatch_non_call_op_job_run_raises(self):
        """dispatch_callable should raise for job.run."""
        with pytest.raises(ValueError, match="only handles call"):
            dispatch_callable(Op.JOB_RUN, {})


class TestExceptionPropagation:
    """Tests that exceptions are propagated unchanged."""

    def test_transient_error_propagated(self):
        """TransientError should be propagated unchanged."""
        def raise_transient(params: dict) -> dict:
            raise TransientError("Rate limited")

        register_callable(Op.CALL_CANONIZER, raise_transient)

        with pytest.raises(TransientError, match="Rate limited"):
            dispatch_callable(Op.CALL_CANONIZER, {})

    def test_permanent_error_propagated(self):
        """PermanentError should be propagated unchanged."""
        def raise_permanent(params: dict) -> dict:
            raise PermanentError("Invalid input")

        register_callable(Op.CALL_FINALFORM, raise_permanent)

        with pytest.raises(PermanentError, match="Invalid input"):
            dispatch_callable(Op.CALL_FINALFORM, {})

    def test_generic_exception_propagated(self):
        """Generic exceptions should be propagated unchanged."""
        def raise_generic(params: dict) -> dict:
            raise RuntimeError("Something went wrong")

        register_callable(Op.CALL_PROJECTIONIST, raise_generic)

        with pytest.raises(RuntimeError, match="Something went wrong"):
            dispatch_callable(Op.CALL_PROJECTIONIST, {})


class TestRegisterCallable:
    """Tests for register_callable function."""

    def test_register_call_op_succeeds(self):
        """Should be able to register call.* ops."""
        def custom_callable(params: dict) -> dict:
            return {"items": [], "stats": {"custom": True}}

        register_callable(Op.CALL_WORKMAN, custom_callable)

        result = dispatch_callable(Op.CALL_WORKMAN, {})
        assert result.stats == {"custom": True}

    def test_register_non_call_op_raises(self):
        """Should raise when registering non-call.* ops."""
        def some_callable(params: dict) -> dict:
            return {"items": []}

        with pytest.raises(ValueError, match="Can only register call"):
            register_callable(Op.COMPUTE_LLM, some_callable)


class TestCallablesRegistry:
    """Tests for the CALLABLES registry."""

    def test_get_callables_returns_dict(self):
        """get_callables should return a dict."""
        callables = get_callables()
        assert isinstance(callables, dict)

    def test_all_call_ops_in_registry(self):
        """All call.* ops should be in the registry."""
        callables = get_callables()

        expected_ops = [
            Op.CALL_INJEST,
            Op.CALL_CANONIZER,
            Op.CALL_FINALFORM,
            Op.CALL_PROJECTIONIST,
            Op.CALL_WORKMAN,
        ]

        for op in expected_ops:
            assert op in callables, f"{op} should be in registry"

    def test_non_call_ops_not_in_registry(self):
        """Non-call.* ops should not be in the registry."""
        callables = get_callables()

        assert Op.COMPUTE_LLM not in callables
        assert Op.JOB_RUN not in callables


class TestStubCallables:
    """Tests for stub callables (when real modules not installed)."""

    def test_stub_raises_not_implemented(self):
        """Stub callables should raise NotImplementedError."""
        # Reset a callable to its stub
        from lorchestra.callable.dispatch import _stub_callable

        stub = _stub_callable("test_module")
        register_callable(Op.CALL_INJEST, stub)

        with pytest.raises(NotImplementedError, match="not installed"):
            dispatch_callable(Op.CALL_INJEST, {})
