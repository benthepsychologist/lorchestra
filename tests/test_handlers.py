"""Tests for the handlers module (e005-03).

Tests cover:
- Handler base class and NoOpHandler
- StoracleClient protocol and NoOpStoracleClient
- DataPlaneHandler dispatch
- ComputeClient protocol and NoOpComputeClient
- ComputeHandler dispatch
- OrchestrationHandler
- HandlerRegistry dispatch and factory methods
"""

import pytest
from datetime import datetime, timezone

from lorchestra.handlers import (
    Handler,
    NoOpHandler,
    HandlerRegistry,
    DataPlaneHandler,
    ComputeHandler,
    OrchestrationHandler,
    StoracleClient,
    NoOpStoracleClient,
)
from lorchestra.handlers.compute import ComputeClient, NoOpComputeClient
from lorchestra.handlers.data_plane import QUERY_OP_ALLOWLIST, MAX_QUERY_LIMIT
from lorchestra.schemas import StepManifest, Op


# -----------------------------------------------------------------------------
# Test Fixtures
# -----------------------------------------------------------------------------


def _make_query_manifest(limit: int = 100) -> StepManifest:
    """Create a query.raw_objects manifest for testing."""
    return StepManifest.from_op(
        run_id="01TEST00000000000000000000",
        step_id="test_query",
        op=Op.QUERY_RAW_OBJECTS,
        resolved_params={
            "source_system": "test",
            "object_type": "test_obj",
            "limit": limit,  # Required for bounded cost
        },
        idempotency_key="test:test_query",
    )


def _make_write_manifest() -> StepManifest:
    """Create a write.upsert manifest for testing."""
    return StepManifest.from_op(
        run_id="01TEST00000000000000000000",
        step_id="test_write",
        op=Op.WRITE_UPSERT,
        resolved_params={
            "table": "test_table",
            "objects": [{"id": 1}, {"id": 2}],
        },
        idempotency_key="test:test_write",
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
# NoOpHandler Tests
# -----------------------------------------------------------------------------


class TestNoOpHandler:
    """Tests for NoOpHandler."""

    def test_execute_returns_noop_status(self):
        """NoOpHandler should return noop status."""
        handler = NoOpHandler()
        manifest = _make_query_manifest()

        result = handler.execute(manifest)

        assert result["status"] == "noop"
        assert result["op"] == "query.raw_objects"
        assert result["backend"] == "data_plane"

    def test_execute_includes_params(self):
        """NoOpHandler should include resolved_params in result."""
        handler = NoOpHandler()
        manifest = _make_write_manifest()

        result = handler.execute(manifest)

        assert result["params"]["table"] == "test_table"
        assert len(result["params"]["objects"]) == 2


# -----------------------------------------------------------------------------
# NoOpStoracleClient Tests
# -----------------------------------------------------------------------------


class TestNoOpStoracleClient:
    """Tests for NoOpStoracleClient."""

    def test_query_raw_objects_returns_empty(self):
        """query_raw_objects should return empty iterator."""
        client = NoOpStoracleClient()

        results = list(client.query_raw_objects("test", "obj"))

        assert results == []

    def test_query_canonical_objects_returns_empty(self):
        """query_canonical_objects should return empty iterator."""
        client = NoOpStoracleClient()

        results = list(client.query_canonical_objects("schema"))

        assert results == []

    def test_query_last_sync_returns_none(self):
        """query_last_sync should return None."""
        client = NoOpStoracleClient()

        result = client.query_last_sync("test", "conn", "obj")

        assert result is None

    def test_upsert_returns_object_count(self):
        """upsert should return count of objects as inserted."""
        client = NoOpStoracleClient()
        objects = [{"id": 1}, {"id": 2}, {"id": 3}]

        result = client.upsert("table", objects)

        assert result["inserted"] == 3
        assert result["updated"] == 0

    def test_insert_returns_count(self):
        """insert should return count of objects."""
        client = NoOpStoracleClient()

        result = client.insert("table", [{"id": 1}])

        assert result == 1

    def test_delete_returns_zero(self):
        """delete should return 0 (no rows deleted in noop)."""
        client = NoOpStoracleClient()

        result = client.delete("table", {"id": 1})

        assert result == 0

    def test_assert_rows_passes(self):
        """assert_rows should always pass in noop mode."""
        client = NoOpStoracleClient()

        result = client.assert_rows("table", expected_count=100)

        assert result["passed"] is True

    def test_assert_schema_passes(self):
        """assert_schema should always pass in noop mode."""
        client = NoOpStoracleClient()

        result = client.assert_schema("table", [{"name": "id", "type": "INT"}])

        assert result["passed"] is True
        assert result["missing"] == []

    def test_assert_unique_passes(self):
        """assert_unique should always pass in noop mode."""
        client = NoOpStoracleClient()

        result = client.assert_unique("table", ["id"])

        assert result["passed"] is True


# -----------------------------------------------------------------------------
# DataPlaneHandler Tests
# -----------------------------------------------------------------------------


class TestDataPlaneHandler:
    """Tests for DataPlaneHandler."""

    def test_query_raw_objects(self):
        """Should dispatch query.raw_objects to client."""
        client = NoOpStoracleClient()
        handler = DataPlaneHandler(client)
        manifest = _make_query_manifest()

        result = handler.execute(manifest)

        assert result["rows"] == []
        assert result["count"] == 0

    def test_write_upsert(self):
        """Should dispatch write.upsert to client."""
        client = NoOpStoracleClient()
        handler = DataPlaneHandler(client)
        manifest = _make_write_manifest()

        result = handler.execute(manifest)

        assert result["inserted"] == 2
        assert result["updated"] == 0
        assert result["total"] == 2

    def test_assert_rows(self):
        """Should dispatch assert.rows to client."""
        client = NoOpStoracleClient()
        handler = DataPlaneHandler(client)
        manifest = StepManifest.from_op(
            run_id="01TEST00000000000000000000",
            step_id="assert_test",
            op=Op.ASSERT_ROWS,
            resolved_params={
                "table": "test_table",
                "expected_count": 10,
            },
            idempotency_key="test:assert",
        )

        result = handler.execute(manifest)

        assert result["passed"] is True

    def test_unsupported_op_raises(self):
        """Should raise ValueError for non-data_plane ops."""
        client = NoOpStoracleClient()
        handler = DataPlaneHandler(client)
        manifest = _make_llm_manifest()

        with pytest.raises(ValueError, match="Unsupported data_plane op"):
            handler.execute(manifest)

    def test_query_requires_limit(self):
        """Query operations (except query.last_sync) require a limit parameter."""
        client = NoOpStoracleClient()
        handler = DataPlaneHandler(client)
        # Create manifest without limit
        manifest = StepManifest.from_op(
            run_id="01TEST00000000000000000000",
            step_id="test_query_no_limit",
            op=Op.QUERY_RAW_OBJECTS,
            resolved_params={
                "source_system": "test",
                "object_type": "test_obj",
                # No limit specified
            },
            idempotency_key="test:no_limit",
        )

        with pytest.raises(ValueError, match="requires a 'limit' parameter"):
            handler.execute(manifest)

    def test_query_limit_max_enforced(self):
        """Query limit cannot exceed MAX_QUERY_LIMIT (1000)."""
        client = NoOpStoracleClient()
        handler = DataPlaneHandler(client)
        manifest = _make_query_manifest(limit=MAX_QUERY_LIMIT + 1)

        with pytest.raises(ValueError, match="exceeds maximum allowed"):
            handler.execute(manifest)

    def test_query_limit_at_max_allowed(self):
        """Query with limit at MAX_QUERY_LIMIT should succeed."""
        client = NoOpStoracleClient()
        handler = DataPlaneHandler(client)
        manifest = _make_query_manifest(limit=MAX_QUERY_LIMIT)

        # Should not raise
        result = handler.execute(manifest)
        assert result["count"] == 0  # NoOp returns empty

    def test_query_last_sync_no_limit_required(self):
        """query.last_sync does not require a limit parameter."""
        client = NoOpStoracleClient()
        handler = DataPlaneHandler(client)
        manifest = StepManifest.from_op(
            run_id="01TEST00000000000000000000",
            step_id="test_last_sync",
            op=Op.QUERY_LAST_SYNC,
            resolved_params={
                "source_system": "test",
                "connection_name": "conn",
                "object_type": "obj",
                # No limit needed
            },
            idempotency_key="test:last_sync",
        )

        # Should not raise
        result = handler.execute(manifest)
        assert result["last_sync"] is None  # NoOp returns None

    def test_query_allowlist_contains_expected_ops(self):
        """Verify the query allowlist contains the expected operations."""
        expected = {Op.QUERY_RAW_OBJECTS, Op.QUERY_CANONICAL_OBJECTS, Op.QUERY_LAST_SYNC}
        assert QUERY_OP_ALLOWLIST == expected

    def test_max_query_limit_is_1000(self):
        """Verify MAX_QUERY_LIMIT is 1000 per e005 spec."""
        assert MAX_QUERY_LIMIT == 1000


# -----------------------------------------------------------------------------
# NoOpComputeClient Tests
# -----------------------------------------------------------------------------


class TestNoOpComputeClient:
    """Tests for NoOpComputeClient."""

    def test_llm_invoke_returns_mock_response(self):
        """llm_invoke should return mock response."""
        client = NoOpComputeClient()

        result = client.llm_invoke("Hello", model="gpt-4")

        assert result["response"] == "[noop response]"
        assert result["model"] == "gpt-4"
        assert "usage" in result

    def test_transform_returns_input_unchanged(self):
        """transform should return input unchanged (identity)."""
        client = NoOpComputeClient()
        data = {"key": "value"}

        result = client.transform(data, "some_transform")

        assert result == data

    def test_extract_returns_mock_result(self):
        """extract should return mock extraction result."""
        client = NoOpComputeClient()

        result = client.extract("source text", "email_extractor")

        assert result["source"] == "source text"
        assert result["extractor"] == "email_extractor"

    def test_render_returns_mock_string(self):
        """render should return mock rendered string."""
        client = NoOpComputeClient()

        result = client.render("report_template", {"name": "Test"})

        assert "[noop render:" in result


# -----------------------------------------------------------------------------
# ComputeHandler Tests
# -----------------------------------------------------------------------------


class TestComputeHandler:
    """Tests for ComputeHandler."""

    def test_compute_llm(self):
        """Should dispatch compute.llm to client."""
        client = NoOpComputeClient()
        handler = ComputeHandler(client)
        manifest = _make_llm_manifest()

        result = handler.execute(manifest)

        assert result["response"] == "[noop response]"
        assert result["model"] == "test-model"

    def test_compute_transform(self):
        """Should dispatch compute.transform to client."""
        client = NoOpComputeClient()
        handler = ComputeHandler(client)
        manifest = StepManifest.from_op(
            run_id="01TEST00000000000000000000",
            step_id="transform_test",
            op=Op.COMPUTE_TRANSFORM,
            resolved_params={
                "input": {"data": "value"},
                "transform_ref": "email/to_jmap@1.0",
            },
            idempotency_key="test:transform",
        )

        result = handler.execute(manifest)

        assert result["output"] == {"data": "value"}

    def test_compute_extract(self):
        """Should dispatch compute.extract to client."""
        client = NoOpComputeClient()
        handler = ComputeHandler(client)
        manifest = StepManifest.from_op(
            run_id="01TEST00000000000000000000",
            step_id="extract_test",
            op=Op.COMPUTE_EXTRACT,
            resolved_params={
                "source": "email body text",
                "extractor_ref": "contact_info",
            },
            idempotency_key="test:extract",
        )

        result = handler.execute(manifest)

        assert result["source"] == "email body text"

    def test_compute_render(self):
        """Should dispatch compute.render to client."""
        client = NoOpComputeClient()
        handler = ComputeHandler(client)
        manifest = StepManifest.from_op(
            run_id="01TEST00000000000000000000",
            step_id="render_test",
            op=Op.COMPUTE_RENDER,
            resolved_params={
                "template_ref": "weekly_report",
                "context": {"week": 1},
            },
            idempotency_key="test:render",
        )

        result = handler.execute(manifest)

        assert "rendered" in result

    def test_unsupported_op_raises(self):
        """Should raise ValueError for non-compute ops."""
        client = NoOpComputeClient()
        handler = ComputeHandler(client)
        manifest = _make_query_manifest()

        with pytest.raises(ValueError, match="Unsupported compute op"):
            handler.execute(manifest)


# -----------------------------------------------------------------------------
# OrchestrationHandler Tests
# -----------------------------------------------------------------------------


class TestOrchestrationHandler:
    """Tests for OrchestrationHandler."""

    def test_job_run_without_registry_returns_noop(self):
        """job.run without registry should return noop status."""
        handler = OrchestrationHandler()
        manifest = _make_job_run_manifest()

        result = handler.execute(manifest)

        assert result["status"] == "noop"
        assert result["job_id"] == "sub_job"

    def test_unsupported_op_raises(self):
        """Should raise ValueError for non-orchestration ops."""
        handler = OrchestrationHandler()
        manifest = _make_query_manifest()

        with pytest.raises(ValueError, match="Unsupported orchestration op"):
            handler.execute(manifest)


# -----------------------------------------------------------------------------
# HandlerRegistry Tests
# -----------------------------------------------------------------------------


class TestHandlerRegistry:
    """Tests for HandlerRegistry."""

    def test_register_and_get(self):
        """Should register and retrieve handlers."""
        registry = HandlerRegistry()
        handler = NoOpHandler()

        registry.register("test", handler)
        retrieved = registry.get("test")

        assert retrieved is handler

    def test_get_unregistered_raises(self):
        """Should raise KeyError for unregistered backend."""
        registry = HandlerRegistry()

        with pytest.raises(KeyError, match="No handler registered"):
            registry.get("unknown")

    def test_has_returns_true_for_registered(self):
        """has() should return True for registered backends."""
        registry = HandlerRegistry()
        registry.register("test", NoOpHandler())

        assert registry.has("test") is True
        assert registry.has("unknown") is False

    def test_list_backends(self):
        """list_backends() should return registered backend names."""
        registry = HandlerRegistry()
        registry.register("a", NoOpHandler())
        registry.register("b", NoOpHandler())

        backends = registry.list_backends()

        assert set(backends) == {"a", "b"}

    def test_dispatch_routes_to_handler(self):
        """dispatch() should route manifest to correct handler."""
        registry = HandlerRegistry()
        registry.register("data_plane", DataPlaneHandler(NoOpStoracleClient()))
        registry.register("compute", ComputeHandler(NoOpComputeClient()))

        # Data plane manifest
        dp_result = registry.dispatch(_make_query_manifest())
        assert dp_result["count"] == 0

        # Compute manifest
        compute_result = registry.dispatch(_make_llm_manifest())
        assert compute_result["response"] == "[noop response]"

    def test_dispatch_unregistered_raises(self):
        """dispatch() should raise KeyError for unregistered backend."""
        registry = HandlerRegistry()

        with pytest.raises(KeyError):
            registry.dispatch(_make_query_manifest())

    def test_create_noop_registers_all_backends(self):
        """create_noop() should register all backend types."""
        registry = HandlerRegistry.create_noop()

        backends = set(registry.list_backends())

        assert backends == {"data_plane", "compute", "orchestration"}

    def test_create_default_without_clients(self):
        """create_default() without clients should use NoOpHandler."""
        registry = HandlerRegistry.create_default()

        # Should have all backends
        assert registry.has("data_plane")
        assert registry.has("compute")
        assert registry.has("orchestration")

        # Should be noop
        result = registry.dispatch(_make_query_manifest())
        assert result["status"] == "noop"

    def test_create_default_with_storacle_client(self):
        """create_default() with storacle_client should use DataPlaneHandler."""
        client = NoOpStoracleClient()
        registry = HandlerRegistry.create_default(storacle_client=client)

        # Should dispatch to DataPlaneHandler
        result = registry.dispatch(_make_query_manifest())
        assert "rows" in result  # DataPlaneHandler returns rows
        assert "status" not in result  # Not NoOpHandler

    def test_create_default_with_compute_client(self):
        """create_default() with compute_client should use ComputeHandler."""
        client = NoOpComputeClient()
        registry = HandlerRegistry.create_default(compute_client=client)

        # Should dispatch to ComputeHandler
        result = registry.dispatch(_make_llm_manifest())
        assert result["response"] == "[noop response]"
        assert "model" in result  # ComputeHandler includes model


# -----------------------------------------------------------------------------
# Protocol Compliance Tests
# -----------------------------------------------------------------------------


class TestProtocolCompliance:
    """Tests to verify protocol implementations are compliant."""

    def test_noop_storacle_client_is_storacle_client(self):
        """NoOpStoracleClient should be a StoracleClient."""
        client = NoOpStoracleClient()

        # Runtime check using isinstance with Protocol
        assert isinstance(client, StoracleClient)

    def test_noop_compute_client_is_compute_client(self):
        """NoOpComputeClient should be a ComputeClient."""
        client = NoOpComputeClient()

        # Runtime check using isinstance with Protocol
        assert isinstance(client, ComputeClient)

    def test_data_plane_handler_is_handler(self):
        """DataPlaneHandler should be a Handler."""
        handler = DataPlaneHandler(NoOpStoracleClient())

        assert isinstance(handler, Handler)

    def test_compute_handler_is_handler(self):
        """ComputeHandler should be a Handler."""
        handler = ComputeHandler(NoOpComputeClient())

        assert isinstance(handler, Handler)

    def test_orchestration_handler_is_handler(self):
        """OrchestrationHandler should be a Handler."""
        handler = OrchestrationHandler()

        assert isinstance(handler, Handler)

    def test_noop_handler_is_handler(self):
        """NoOpHandler should be a Handler."""
        handler = NoOpHandler()

        assert isinstance(handler, Handler)
