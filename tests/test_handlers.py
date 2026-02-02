"""Tests for the handlers module (e005b-05).

Tests cover:
- Handler base class and NoOpHandler
- ComputeClient protocol and NoOpComputeClient
- ComputeHandler dispatch
- OrchestrationHandler
- HandlerRegistry dispatch and factory methods

Note: CallableHandler was removed in e005b-05. Native ops (call, plan.build,
storacle.submit) are handled directly by the Executor.
"""

import pytest

from lorchestra.handlers import (
    NoOpHandler,
    HandlerRegistry,
    ComputeHandler,
    OrchestrationHandler,
)
from lorchestra.handlers.compute import NoOpComputeClient
from lorchestra.schemas import StepManifest, Op


# -----------------------------------------------------------------------------
# Test Fixtures
# -----------------------------------------------------------------------------


def _make_call_manifest() -> StepManifest:
    """Create a call manifest for testing."""
    return StepManifest.from_op(
        run_id="01TEST00000000000000000000",
        step_id="test_call",
        op=Op.CALL,
        resolved_params={
            "callable": "injest",
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
        manifest = _make_llm_manifest()

        result = handler.execute(manifest)

        assert result["status"] == "noop"


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
        registry.register("inferometer", NoOpHandler())

        manifest = _make_llm_manifest()
        result = registry.dispatch(manifest)

        assert result["status"] == "noop"

    def test_create_noop_registry(self):
        """create_noop creates registry with NoOp handlers for non-native backends."""
        registry = HandlerRegistry.create_noop()

        assert registry.has("inferometer")
        assert registry.has("orchestration")

        # All should return noop
        manifest = _make_llm_manifest()
        result = registry.dispatch(manifest)
        assert result["status"] == "noop"

    def test_create_default_registry(self):
        """create_default creates registry with default handlers."""
        registry = HandlerRegistry.create_default()

        assert registry.has("inferometer")
        assert registry.has("orchestration")


# -----------------------------------------------------------------------------
# Backend Routing Tests
# -----------------------------------------------------------------------------


class TestBackendRouting:
    """Tests for correct backend routing in registry."""

    def test_call_op_routes_to_callable(self):
        """call op routes to callable backend."""
        manifest = _make_call_manifest()
        assert manifest.backend == "callable"

    def test_compute_ops_route_to_inferometer(self):
        """compute.* ops route to inferometer backend."""
        manifest = _make_llm_manifest()
        assert manifest.backend == "inferometer"

    def test_job_ops_route_to_orchestration(self):
        """job.* ops route to orchestration backend."""
        manifest = _make_job_run_manifest()
        assert manifest.backend == "orchestration"

    def test_native_ops_route_to_native(self):
        """plan.build and storacle.submit route to native backend."""
        plan_manifest = StepManifest.from_op(
            run_id="01TEST00000000000000000000",
            step_id="test_plan",
            op=Op.PLAN_BUILD,
            resolved_params={"items": [], "method": "wal.append"},
            idempotency_key="test:test_plan",
        )
        assert plan_manifest.backend == "native"

        storacle_manifest = StepManifest.from_op(
            run_id="01TEST00000000000000000000",
            step_id="test_storacle",
            op=Op.STORACLE_SUBMIT,
            resolved_params={"plan": {}},
            idempotency_key="test:test_storacle",
        )
        assert storacle_manifest.backend == "native"
