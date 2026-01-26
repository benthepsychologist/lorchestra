"""Tests for the handlers module (e005b-01).

Tests cover:
- Handler base class and NoOpHandler
- CallableHandler dispatch
- ComputeClient protocol and NoOpComputeClient
- ComputeHandler dispatch
- OrchestrationHandler
- HandlerRegistry dispatch and factory methods

Note: This test file has been updated for e005b-01 which replaced
DataPlaneHandler with CallableHandler for call.* ops.
"""

import pytest
from datetime import datetime, timezone

from lorchestra.handlers import (
    Handler,
    NoOpHandler,
    HandlerRegistry,
    CallableHandler,
    ComputeHandler,
    OrchestrationHandler,
)
from lorchestra.handlers.compute import ComputeClient, NoOpComputeClient
from lorchestra.schemas import StepManifest, Op
from lorchestra.callable import register_callable


# -----------------------------------------------------------------------------
# Test Fixtures
# -----------------------------------------------------------------------------


def _make_call_manifest() -> StepManifest:
    """Create a call.injest manifest for testing."""
    return StepManifest.from_op(
        run_id="01TEST00000000000000000000",
        step_id="test_call",
        op=Op.CALL_INJEST,
        resolved_params={
            "source_system": "test",
            "object_type": "test_obj",
        },
        idempotency_key="test:test_call",
    )


def _make_llm_manifest() -> StepManifest:
    """Create a compute.llm manifest for testing."""
    return StepManifest.from_op(
        run_id="01TEST00000000000000000000",
        step_id="test_llm",
        op=Op.COMPUTE_LLM,
        resolved_params={
            "prompt": "Hello world",
            "model": "test-model",
        },
        idempotency_key="test:test_llm",
    )


def _make_job_run_manifest() -> StepManifest:
    """Create a job.run manifest for testing."""
    return StepManifest.from_op(
        run_id="01TEST00000000000000000000",
        step_id="test_subjob",
        op=Op.JOB_RUN,
        resolved_params={
            "job_id": "sub_job",
            "ctx": {"key": "value"},
        },
        idempotency_key="test:test_subjob",
    )


# -----------------------------------------------------------------------------
# Base Handler Tests
# -----------------------------------------------------------------------------


class TestNoOpHandler:
    """Tests for NoOpHandler."""

    def test_noop_handler_returns_empty_result(self):
        """NoOpHandler returns status noop."""
        handler = NoOpHandler()
        manifest = _make_call_manifest()

        result = handler.execute(manifest)

        assert result["status"] == "noop"


# -----------------------------------------------------------------------------
# CallableHandler Tests
# -----------------------------------------------------------------------------


class TestCallableHandler:
    """Tests for CallableHandler dispatch."""

    def test_callable_handler_dispatches_call_op(self):
        """CallableHandler dispatches call.* ops to callables."""
        # Register a mock callable
        def mock_injest(params: dict) -> dict:
            return {
                "items": [{"id": 1, "data": params.get("source_system")}],
                "stats": {"count": 1},
            }

        register_callable(Op.CALL_INJEST, mock_injest)

        handler = CallableHandler()
        manifest = _make_call_manifest()

        result = handler.execute(manifest)

        assert "callable_result" in result
        assert result["callable_result"]["items_count"] == 1

    def test_callable_handler_rejects_non_call_ops(self):
        """CallableHandler rejects non-call.* ops."""
        handler = CallableHandler()
        manifest = _make_llm_manifest()

        with pytest.raises(ValueError, match="only handles call"):
            handler.execute(manifest)


# -----------------------------------------------------------------------------
# ComputeHandler Tests
# -----------------------------------------------------------------------------


class TestNoOpComputeClient:
    """Tests for NoOpComputeClient."""

    def test_noop_compute_returns_stub(self):
        """NoOpComputeClient returns stub response."""
        client = NoOpComputeClient()
        result = client.llm_invoke(prompt="test", model="test")

        assert result["model"] == "test"
        assert "response" in result


class TestComputeHandler:
    """Tests for ComputeHandler."""

    def test_compute_handler_dispatches_llm(self):
        """ComputeHandler dispatches compute.llm to client."""
        client = NoOpComputeClient()
        handler = ComputeHandler(client)
        manifest = _make_llm_manifest()

        result = handler.execute(manifest)

        assert "response" in result
        assert "model" in result


# -----------------------------------------------------------------------------
# OrchestrationHandler Tests
# -----------------------------------------------------------------------------


class TestOrchestrationHandler:
    """Tests for OrchestrationHandler."""

    def test_orchestration_handler_handles_job_run(self):
        """OrchestrationHandler handles job.run ops."""
        registry = HandlerRegistry.create_noop()
        handler = OrchestrationHandler(registry=registry, store=None)
        manifest = _make_job_run_manifest()

        result = handler.execute(manifest)

        # Without a store or real job, this just returns the params
        assert "job_id" in result or "status" in result


# -----------------------------------------------------------------------------
# HandlerRegistry Tests
# -----------------------------------------------------------------------------


class TestHandlerRegistry:
    """Tests for HandlerRegistry."""

    def test_registry_register_and_get(self):
        """Can register and retrieve handlers."""
        registry = HandlerRegistry()
        handler = NoOpHandler()

        registry.register("test_backend", handler)

        assert registry.get("test_backend") is handler
        assert registry.has("test_backend")
        assert not registry.has("unknown")

    def test_registry_get_missing_raises(self):
        """Getting unregistered backend raises KeyError."""
        registry = HandlerRegistry()

        with pytest.raises(KeyError, match="No handler registered"):
            registry.get("unknown")

    def test_registry_list_backends(self):
        """list_backends returns all registered names."""
        registry = HandlerRegistry()
        registry.register("a", NoOpHandler())
        registry.register("b", NoOpHandler())

        backends = registry.list_backends()

        assert set(backends) == {"a", "b"}

    def test_registry_dispatch(self):
        """dispatch routes to correct handler."""
        registry = HandlerRegistry()
        registry.register("callable", NoOpHandler())

        manifest = _make_call_manifest()
        result = registry.dispatch(manifest)

        assert result["status"] == "noop"

    def test_create_noop_registry(self):
        """create_noop creates registry with all NoOp handlers."""
        registry = HandlerRegistry.create_noop()

        assert registry.has("callable")
        assert registry.has("inferator")
        assert registry.has("orchestration")

        # All should return noop
        manifest = _make_call_manifest()
        result = registry.dispatch(manifest)
        assert result["status"] == "noop"

    def test_create_default_registry(self):
        """create_default creates registry with default handlers."""
        registry = HandlerRegistry.create_default()

        assert registry.has("callable")
        assert registry.has("inferator")
        assert registry.has("orchestration")


# -----------------------------------------------------------------------------
# Backend Routing Tests
# -----------------------------------------------------------------------------


class TestBackendRouting:
    """Tests for correct backend routing in registry."""

    def test_call_ops_route_to_callable(self):
        """call.* ops route to callable backend."""
        manifest = _make_call_manifest()
        assert manifest.backend == "callable"

    def test_compute_ops_route_to_inferator(self):
        """compute.* ops route to inferator backend."""
        manifest = _make_llm_manifest()
        assert manifest.backend == "inferator"

    def test_job_ops_route_to_orchestration(self):
        """job.* ops route to orchestration backend."""
        manifest = _make_job_run_manifest()
        assert manifest.backend == "orchestration"
