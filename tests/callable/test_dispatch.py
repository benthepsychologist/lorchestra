"""Tests for callable dispatch (e005b-05).

Tests cover:
- Name-based dispatch to callables
- Exception propagation (not classification)
- Register callable for testing
"""

import pytest
from lorchestra.callable import CallableResult, dispatch_callable, register_callable
from lorchestra.callable.dispatch import get_callables
from lorchestra.errors import TransientError, PermanentError


class TestDispatchCallable:
    """Tests for dispatch_callable function."""

    def test_dispatch_returns_callable_result(self):
        """dispatch_callable should return CallableResult."""
        # Register a mock callable
        def mock_injest(params: dict) -> dict:
            return {"items": [{"id": 1}], "stats": {"count": 1}}

        register_callable("injest", mock_injest)

        result = dispatch_callable("injest", {"source": "test"})

        assert isinstance(result, CallableResult)
        assert result.items == [{"id": 1}]
        assert result.stats == {"count": 1}

    def test_dispatch_unknown_callable_raises(self):
        """dispatch_callable should raise for unknown callable names."""
        with pytest.raises(ValueError, match="Unknown callable"):
            dispatch_callable("nonexistent_module", {})


class TestExceptionPropagation:
    """Tests that exceptions are propagated unchanged."""

    def test_transient_error_propagated(self):
        """TransientError should be propagated unchanged."""
        def raise_transient(params: dict) -> dict:
            raise TransientError("Rate limited")

        register_callable("canonizer", raise_transient)

        with pytest.raises(TransientError, match="Rate limited"):
            dispatch_callable("canonizer", {})

    def test_permanent_error_propagated(self):
        """PermanentError should be propagated unchanged."""
        def raise_permanent(params: dict) -> dict:
            raise PermanentError("Invalid input")

        register_callable("finalform", raise_permanent)

        with pytest.raises(PermanentError, match="Invalid input"):
            dispatch_callable("finalform", {})

    def test_generic_exception_propagated(self):
        """Generic exceptions should be propagated unchanged."""
        def raise_generic(params: dict) -> dict:
            raise RuntimeError("Something went wrong")

        register_callable("projectionist", raise_generic)

        with pytest.raises(RuntimeError, match="Something went wrong"):
            dispatch_callable("projectionist", {})


class TestRegisterCallable:
    """Tests for register_callable function."""

    def test_register_callable_succeeds(self):
        """Should be able to register callables by name."""
        def custom_callable(params: dict) -> dict:
            return {"items": [], "stats": {"custom": True}}

        register_callable("workman", custom_callable)

        result = dispatch_callable("workman", {})
        assert result.stats == {"custom": True}


class TestCallablesRegistry:
    """Tests for the CALLABLES registry."""

    def test_get_callables_returns_dict(self):
        """get_callables should return a dict."""
        callables = get_callables()
        assert isinstance(callables, dict)

    def test_all_callable_names_in_registry(self):
        """All callable module names should be in the registry."""
        callables = get_callables()

        expected_names = [
            "injest",
            "canonizer",
            "finalform",
            "projectionist",
            "workman",
            "inferometer",
        ]

        for name in expected_names:
            assert name in callables, f"{name} should be in registry"

    def test_non_callable_names_not_in_registry(self):
        """Non-callable names should not be in the registry."""
        callables = get_callables()

        assert "compute.llm" not in callables
        assert "job.run" not in callables


class TestStubCallables:
    """Tests for stub callables (when real modules not installed)."""

    def test_stub_raises_not_implemented(self):
        """Stub callables should raise NotImplementedError."""
        # Reset a callable to its stub
        from lorchestra.callable.dispatch import _stub_callable

        stub = _stub_callable("test_module")
        register_callable("injest", stub)

        with pytest.raises(NotImplementedError, match="not installed"):
            dispatch_callable("injest", {})
