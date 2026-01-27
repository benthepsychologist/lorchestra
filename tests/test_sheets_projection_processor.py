"""Tests for ProjectionistProcessor (sheets projection via RPC).

NOTE: These tests require the projectionist and storacle packages to be installed.
They are skipped if those packages are not available.
"""

from unittest.mock import MagicMock, patch
from dataclasses import dataclass

import pytest

# Skip entire module if external dependencies aren't available
try:
    from projectionist.context import ProjectionContext
    from storacle.rpc import execute_plan
    PROJECTIONIST_AVAILABLE = True
except ImportError:
    PROJECTIONIST_AVAILABLE = False
    pytestmark = pytest.mark.skip(reason="projectionist or storacle packages not installed")

from lorchestra.processors.projectionist import (
    ProjectionistProcessor,
    BqQueryServiceAdapter,
    _resolve_placeholders,
    _get_build_plan_registry,
)


# =============================================================================
# FIXTURES
# =============================================================================


@dataclass
class MockConfig:
    """Mock LorchestraConfig for tests."""

    project: str = "test-project"
    dataset_canonical: str = "canonical"
    dataset_raw: str = "raw"
    dataset_derived: str = "derived"


@dataclass
class MockJobContext:
    """Mock JobContext for tests."""

    run_id: str = "test-run-123"
    dry_run: bool = False
    config: MockConfig = None

    def __post_init__(self):
        if self.config is None:
            self.config = MockConfig()


@pytest.fixture
def mock_storage_client():
    """Create a mock StorageClient."""
    client = MagicMock()
    # Default: return some test rows
    client.query_to_dataframe.return_value = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]
    return client


@pytest.fixture
def mock_event_client():
    """Create a mock EventClient."""
    return MagicMock()


@pytest.fixture
def job_spec():
    """Create a basic job spec (v3 format - no factory loading)."""
    return {
        "job_id": "test_sheets_job",
        "job_type": "projectionist",
        "projection_name": "sheets",
        "projection_config": {
            "query": "SELECT * FROM `${PROJECT}.${DATASET_CANONICAL}.test_view`",
            "spreadsheet_id": "abc123",
            "sheet_name": "test_sheet",
            "strategy": "replace",
            "account": "gdrive",
        },
    }


# =============================================================================
# PLACEHOLDER RESOLUTION TESTS
# =============================================================================


class TestResolvePlaceholders:
    """Tests for placeholder resolution."""

    def test_resolves_project(self):
        """Test ${PROJECT} is resolved."""
        config = MockConfig(project="my-project")
        result = _resolve_placeholders("SELECT FROM `${PROJECT}.ds.table`", config)
        assert result == "SELECT FROM `my-project.ds.table`"

    def test_resolves_dataset_canonical(self):
        """Test ${DATASET_CANONICAL} is resolved."""
        config = MockConfig(dataset_canonical="my_canonical")
        result = _resolve_placeholders("${DATASET_CANONICAL}", config)
        assert result == "my_canonical"

    def test_resolves_in_nested_dict(self):
        """Test placeholders in nested dict."""
        config = MockConfig(project="proj", dataset_canonical="can")
        value = {
            "query": "SELECT FROM `${PROJECT}.${DATASET_CANONICAL}.t`",
            "other": "no placeholders",
        }
        result = _resolve_placeholders(value, config)
        assert result["query"] == "SELECT FROM `proj.can.t`"
        assert result["other"] == "no placeholders"

    def test_resolves_in_list(self):
        """Test placeholders in list."""
        config = MockConfig(project="proj")
        value = ["${PROJECT}", "other"]
        result = _resolve_placeholders(value, config)
        assert result == ["proj", "other"]


# =============================================================================
# BQ QUERY SERVICE ADAPTER TESTS
# =============================================================================


class TestBqQueryServiceAdapter:
    """Tests for BqQueryServiceAdapter."""

    def test_query_calls_storage_client(self, mock_storage_client):
        """Test query delegates to storage_client.query_to_dataframe."""
        adapter = BqQueryServiceAdapter(mock_storage_client)

        result = list(adapter.query("SELECT 1"))

        mock_storage_client.query_to_dataframe.assert_called_once_with("SELECT 1")
        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_query_yields_rows(self, mock_storage_client):
        """Test query returns an iterator."""
        mock_storage_client.query_to_dataframe.return_value = [{"a": 1}, {"a": 2}]
        adapter = BqQueryServiceAdapter(mock_storage_client)

        result = adapter.query("SELECT 1")

        # Should be an iterator
        assert hasattr(result, "__iter__")
        rows = list(result)
        assert len(rows) == 2


# =============================================================================
# REGISTRY TESTS
# =============================================================================


class TestProjectionBuildPlanRegistry:
    """Tests for build_plan registry."""

    @pytest.mark.skipif(not PROJECTIONIST_AVAILABLE, reason="projectionist not installed")
    def test_sheets_build_plan_registered(self):
        """Test sheets build_plan is in the registry (when projectionist installed)."""
        registry = _get_build_plan_registry()
        assert "sheets" in registry

    @pytest.mark.skipif(not PROJECTIONIST_AVAILABLE, reason="projectionist not installed")
    def test_registry_value_is_callable(self):
        """Test registry values are callable."""
        registry = _get_build_plan_registry()
        for name, fn in registry.items():
            assert callable(fn), f"{name} is not callable"


# =============================================================================
# PROCESSOR TESTS - RPC-BASED FLOW
# =============================================================================


@pytest.mark.skipif(not PROJECTIONIST_AVAILABLE, reason="projectionist or storacle not installed")
class TestProjectionistProcessor:
    """Tests for ProjectionistProcessor with RPC-based flow."""

    @patch("lorchestra.processors.projectionist._import_storacle_rpc")
    @patch("lorchestra.processors.projectionist._get_build_plan_registry")
    @patch("lorchestra.processors.projectionist._import_projectionist")
    def test_calls_build_plan_then_execute_plan(
        self,
        mock_import_projectionist,
        mock_get_registry,
        mock_import_rpc,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that build_plan() is called before execute_plan()."""
        call_order = []

        mock_plan = {
            "plan_version": "storacle.plan/1.0.0",
            "plan_id": "sha256:abc123",
            "jsonrpc": "2.0",
            "meta": {"source": "projectionist"},
            "ops": [{"jsonrpc": "2.0", "id": "sheets-1", "method": "sheets.write_table", "params": {}}],
        }
        mock_responses = [
            {"jsonrpc": "2.0", "id": "sheets-1", "result": {"rows_written": 2}}
        ]

        mock_build_plan = MagicMock(
            side_effect=lambda *a, **k: (call_order.append("build_plan"), mock_plan)[1]
        )
        mock_get_registry.return_value = {"sheets": mock_build_plan}

        # Mock ProjectionContext class
        mock_projection_context = MagicMock()
        mock_import_projectionist.return_value = (mock_projection_context, None)

        mock_execute_plan = MagicMock(
            side_effect=lambda *a, **k: (call_order.append("execute_plan"), mock_responses)[1]
        )
        mock_import_rpc.return_value = mock_execute_plan

        processor = ProjectionistProcessor()
        context = MockJobContext()

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        assert call_order == ["build_plan", "execute_plan"]

    @patch("lorchestra.processors.projectionist._import_storacle_rpc")
    @patch("lorchestra.processors.projectionist._get_build_plan_registry")
    @patch("lorchestra.processors.projectionist._import_projectionist")
    def test_execute_plan_called_with_plan_and_dry_run(
        self,
        mock_import_projectionist,
        mock_get_registry,
        mock_import_rpc,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that execute_plan is called with plan dict and dry_run flag."""
        mock_plan = {
            "plan_version": "storacle.plan/1.0.0",
            "plan_id": "sha256:test-plan-id",
            "jsonrpc": "2.0",
            "meta": {},
            "ops": [],
        }
        mock_responses = []

        mock_build_plan = MagicMock(return_value=mock_plan)
        mock_get_registry.return_value = {"sheets": mock_build_plan}

        mock_projection_context = MagicMock()
        mock_import_projectionist.return_value = (mock_projection_context, None)

        mock_execute_plan = MagicMock(return_value=mock_responses)
        mock_import_rpc.return_value = mock_execute_plan

        processor = ProjectionistProcessor()
        context = MockJobContext(dry_run=True)

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        mock_execute_plan.assert_called_once_with(mock_plan, dry_run=True)

    @patch("lorchestra.processors.projectionist._import_storacle_rpc")
    @patch("lorchestra.processors.projectionist._get_build_plan_registry")
    @patch("lorchestra.processors.projectionist._import_projectionist")
    def test_events_logged_with_plan_id(
        self,
        mock_import_projectionist,
        mock_get_registry,
        mock_import_rpc,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that events include plan_id from the plan dict."""
        mock_plan = {
            "plan_version": "storacle.plan/1.0.0",
            "plan_id": "sha256:my-unique-plan-id",
            "jsonrpc": "2.0",
            "meta": {},
            "ops": [],
        }
        mock_responses = [{"jsonrpc": "2.0", "id": "op-1", "result": {"rows_written": 5}}]

        mock_build_plan = MagicMock(return_value=mock_plan)
        mock_get_registry.return_value = {"sheets": mock_build_plan}

        mock_projection_context = MagicMock()
        mock_import_projectionist.return_value = (mock_projection_context, None)

        mock_execute_plan = MagicMock(return_value=mock_responses)
        mock_import_rpc.return_value = mock_execute_plan

        processor = ProjectionistProcessor()
        context = MockJobContext()

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        # Check started event includes plan_id
        started_call = mock_event_client.log_event.call_args_list[0]
        assert started_call.kwargs["event_type"] == "projection.started"
        assert started_call.kwargs["payload"]["plan_id"] == "sha256:my-unique-plan-id"

        # Check completed event includes plan_id
        completed_call = mock_event_client.log_event.call_args_list[1]
        assert completed_call.kwargs["event_type"] == "projection.completed"
        assert completed_call.kwargs["payload"]["plan_id"] == "sha256:my-unique-plan-id"

    @patch("lorchestra.processors.projectionist._import_storacle_rpc")
    @patch("lorchestra.processors.projectionist._get_build_plan_registry")
    @patch("lorchestra.processors.projectionist._import_projectionist")
    def test_completed_event_has_ops_count_not_rows(
        self,
        mock_import_projectionist,
        mock_get_registry,
        mock_import_rpc,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that completed event has ops_count, not rows_affected (opaque handling)."""
        mock_plan = {
            "plan_version": "storacle.plan/1.0.0",
            "plan_id": "sha256:abc",
            "jsonrpc": "2.0",
            "meta": {},
            "ops": [],
        }
        mock_responses = [
            {"jsonrpc": "2.0", "id": "op-1", "result": {"rows_written": 10}},
            {"jsonrpc": "2.0", "id": "op-2", "result": {"rows_written": 5}},
        ]

        mock_build_plan = MagicMock(return_value=mock_plan)
        mock_get_registry.return_value = {"sheets": mock_build_plan}

        mock_projection_context = MagicMock()
        mock_import_projectionist.return_value = (mock_projection_context, None)

        mock_execute_plan = MagicMock(return_value=mock_responses)
        mock_import_rpc.return_value = mock_execute_plan

        processor = ProjectionistProcessor()
        context = MockJobContext()

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        completed_call = mock_event_client.log_event.call_args_list[1]
        # Should have ops_count (opaque)
        assert completed_call.kwargs["payload"]["ops_count"] == 2
        # Should NOT have rows_affected (that would require inspecting result schema)
        assert "rows_affected" not in completed_call.kwargs["payload"]

    @patch("lorchestra.processors.projectionist._import_storacle_rpc")
    @patch("lorchestra.processors.projectionist._get_build_plan_registry")
    @patch("lorchestra.processors.projectionist._import_projectionist")
    def test_completed_event_passes_responses_through(
        self,
        mock_import_projectionist,
        mock_get_registry,
        mock_import_rpc,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that completed event includes responses passed through opaquely."""
        mock_plan = {"plan_version": "storacle.plan/1.0.0", "plan_id": "x", "jsonrpc": "2.0", "meta": {}, "ops": []}
        mock_responses = [{"jsonrpc": "2.0", "id": "op-1", "result": {"some": "data"}}]

        mock_build_plan = MagicMock(return_value=mock_plan)
        mock_get_registry.return_value = {"sheets": mock_build_plan}

        mock_projection_context = MagicMock()
        mock_import_projectionist.return_value = (mock_projection_context, None)

        mock_execute_plan = MagicMock(return_value=mock_responses)
        mock_import_rpc.return_value = mock_execute_plan

        processor = ProjectionistProcessor()
        context = MockJobContext()

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        completed_call = mock_event_client.log_event.call_args_list[1]
        # Responses should be passed through as-is
        assert completed_call.kwargs["payload"]["responses"] == mock_responses

    @patch("lorchestra.processors.projectionist._import_storacle_rpc")
    @patch("lorchestra.processors.projectionist._get_build_plan_registry")
    @patch("lorchestra.processors.projectionist._import_projectionist")
    def test_dry_run_passed_to_execute_plan(
        self,
        mock_import_projectionist,
        mock_get_registry,
        mock_import_rpc,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that dry_run=True is passed to execute_plan."""
        mock_plan = {"plan_version": "storacle.plan/1.0.0", "plan_id": "x", "jsonrpc": "2.0", "meta": {}, "ops": []}
        mock_responses = []

        mock_build_plan = MagicMock(return_value=mock_plan)
        mock_get_registry.return_value = {"sheets": mock_build_plan}

        mock_projection_context = MagicMock()
        mock_import_projectionist.return_value = (mock_projection_context, None)

        mock_execute_plan = MagicMock(return_value=mock_responses)
        mock_import_rpc.return_value = mock_execute_plan

        processor = ProjectionistProcessor()
        context = MockJobContext(dry_run=True)

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        mock_execute_plan.assert_called_once()
        call_kwargs = mock_execute_plan.call_args.kwargs
        assert call_kwargs["dry_run"] is True

    @patch("lorchestra.processors.projectionist._import_storacle_rpc")
    @patch("lorchestra.processors.projectionist._get_build_plan_registry")
    @patch("lorchestra.processors.projectionist._import_projectionist")
    def test_placeholder_resolution_in_config(
        self,
        mock_import_projectionist,
        mock_get_registry,
        mock_import_rpc,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that ${PROJECT} etc. in config are resolved."""
        mock_plan = {"plan_version": "storacle.plan/1.0.0", "plan_id": "x", "jsonrpc": "2.0", "meta": {}, "ops": []}

        mock_build_plan = MagicMock(return_value=mock_plan)
        mock_get_registry.return_value = {"sheets": mock_build_plan}

        mock_projection_context = MagicMock()
        mock_import_projectionist.return_value = (mock_projection_context, None)

        mock_execute_plan = MagicMock(return_value=[])
        mock_import_rpc.return_value = mock_execute_plan

        processor = ProjectionistProcessor()
        context = MockJobContext(
            config=MockConfig(project="real-project", dataset_canonical="real_can")
        )

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        # Check that build_plan was called with resolved config
        build_call = mock_build_plan.call_args
        config_arg = build_call.args[1]  # Second positional arg is config

        assert "real-project" in config_arg["query"]
        assert "real_can" in config_arg["query"]
        assert "${PROJECT}" not in config_arg["query"]

    def test_unknown_projection_raises(
        self, mock_storage_client, mock_event_client
    ):
        """Test that unknown projection_name raises ValueError."""
        job_spec = {
            "job_id": "test",
            "job_type": "projectionist",
            "projection_name": "unknown_projection",
            "projection_config": {},
        }

        processor = ProjectionistProcessor()
        context = MockJobContext()

        with pytest.raises(ValueError) as exc_info:
            processor.run(job_spec, context, mock_storage_client, mock_event_client)

        assert "Unknown projection: unknown_projection" in str(exc_info.value)


@pytest.mark.skipif(not PROJECTIONIST_AVAILABLE, reason="projectionist or storacle not installed")
class TestProjectionistProcessorFailure:
    """Tests for failure handling with RPC responses."""

    @patch("lorchestra.processors.projectionist._import_storacle_rpc")
    @patch("lorchestra.processors.projectionist._get_build_plan_registry")
    @patch("lorchestra.processors.projectionist._import_projectionist")
    def test_failed_event_logged_on_rpc_error(
        self,
        mock_import_projectionist,
        mock_get_registry,
        mock_import_rpc,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that projection.failed is logged on RPC error response."""
        mock_plan = {
            "plan_version": "storacle.plan/1.0.0",
            "plan_id": "sha256:fail-plan",
            "jsonrpc": "2.0",
            "meta": {},
            "ops": [],
        }
        # Return an error response
        mock_responses = [
            {
                "jsonrpc": "2.0",
                "id": "op-1",
                "error": {"code": -32000, "message": "Sheets API error"},
            }
        ]

        mock_build_plan = MagicMock(return_value=mock_plan)
        mock_get_registry.return_value = {"sheets": mock_build_plan}

        mock_projection_context = MagicMock()
        mock_import_projectionist.return_value = (mock_projection_context, None)

        mock_execute_plan = MagicMock(return_value=mock_responses)
        mock_import_rpc.return_value = mock_execute_plan

        processor = ProjectionistProcessor()
        context = MockJobContext()

        with pytest.raises(RuntimeError) as exc_info:
            processor.run(job_spec, context, mock_storage_client, mock_event_client)

        # Error message should be opaque (count only, not error details)
        assert "1/1 ops returned errors" in str(exc_info.value)

        # Check failed event was logged
        failed_call = [
            c
            for c in mock_event_client.log_event.call_args_list
            if c.kwargs.get("event_type") == "projection.failed"
        ]
        assert len(failed_call) == 1
        assert failed_call[0].kwargs["status"] == "failed"
        assert failed_call[0].kwargs["payload"]["plan_id"] == "sha256:fail-plan"

    @patch("lorchestra.processors.projectionist._import_storacle_rpc")
    @patch("lorchestra.processors.projectionist._get_build_plan_registry")
    @patch("lorchestra.processors.projectionist._import_projectionist")
    def test_failed_event_logged_on_execute_plan_exception(
        self,
        mock_import_projectionist,
        mock_get_registry,
        mock_import_rpc,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that projection.failed is logged on execute_plan exception."""
        mock_plan = {
            "plan_version": "storacle.plan/1.0.0",
            "plan_id": "sha256:exception-plan",
            "jsonrpc": "2.0",
            "meta": {},
            "ops": [],
        }

        mock_build_plan = MagicMock(return_value=mock_plan)
        mock_get_registry.return_value = {"sheets": mock_build_plan}

        mock_projection_context = MagicMock()
        mock_import_projectionist.return_value = (mock_projection_context, None)

        mock_execute_plan = MagicMock(side_effect=RuntimeError("Connection error"))
        mock_import_rpc.return_value = mock_execute_plan

        processor = ProjectionistProcessor()
        context = MockJobContext()

        with pytest.raises(RuntimeError):
            processor.run(job_spec, context, mock_storage_client, mock_event_client)

        failed_call = [
            c
            for c in mock_event_client.log_event.call_args_list
            if c.kwargs.get("event_type") == "projection.failed"
        ]
        assert len(failed_call) == 1
        assert "Connection error" in failed_call[0].kwargs["error_message"]


# =============================================================================
# NO FACTORY LOADING TESTS
# =============================================================================


class TestNoFactoryLoading:
    """Tests to verify factory loading is not used."""

    def test_processor_does_not_import_load_service_factory(self):
        """Verify the processor module does not import load_service_factory."""
        import lorchestra.processors.projectionist as module

        # Check that load_service_factory is not in the module's namespace
        assert not hasattr(module, "load_service_factory")


# =============================================================================
# LAYER BOUNDARY TESTS
# =============================================================================


class TestLayerBoundaries:
    """Tests to verify layer boundaries are respected."""

    def test_processor_does_not_import_sheets_write_service(self):
        """Verify processor does not import SheetsWriteService."""
        import lorchestra.processors.projectionist as module

        # Check that SheetsWriteService is not in the module's namespace
        assert not hasattr(module, "SheetsWriteService")

        # Check imports in the source (exclude docstrings)
        with open(module.__file__) as f:
            source = f.read()

        # Look for actual imports, not docstring mentions
        assert "from storacle.clients.sheets import" not in source
        assert "import storacle.clients.sheets" not in source

    def test_processor_does_not_branch_on_method(self):
        """Verify processor does not branch on ops[*].method at runtime."""
        import lorchestra.processors.projectionist as module
        import ast

        # Parse the source and check for method comparisons in actual code
        with open(module.__file__) as f:
            source = f.read()

        tree = ast.parse(source)

        # Check for Compare nodes that compare with 'method' attribute
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare):
                # Check if left side is an attribute access for 'method'
                if (
                    isinstance(node.left, ast.Subscript)
                    and isinstance(node.left.slice, ast.Constant)
                    and node.left.slice.value == "method"
                ):
                    raise AssertionError("Found branching on ops[*].method in code")

    def test_processor_uses_lazy_import_for_storacle_rpc(self):
        """Verify storacle.rpc is imported lazily via _import_storacle_rpc."""
        import lorchestra.processors.projectionist as module

        with open(module.__file__) as f:
            source = f.read()

        # Should have lazy import helper
        assert "_import_storacle_rpc" in source

        # Should NOT have top-level direct import (except in TYPE_CHECKING block)
        lines = source.split("\n")
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("from storacle.rpc import"):
                # Check if it's inside a function or TYPE_CHECKING block
                # Look for preceding if TYPE_CHECKING or def
                context_lines = "\n".join(lines[max(0, i-10):i])
                if "if TYPE_CHECKING" not in context_lines and "def _import" not in context_lines:
                    raise AssertionError(f"Found top-level direct import at line {i+1}")

    def test_processor_does_not_inspect_result_schema(self):
        """Verify processor does not access rows_written or similar result fields."""
        import lorchestra.processors.projectionist as module
        import ast

        with open(module.__file__) as f:
            source = f.read()

        tree = ast.parse(source)

        # Check for subscript access to "result" or "rows_written" in actual code
        # (not in docstrings or comments)
        for node in ast.walk(tree):
            if isinstance(node, ast.Subscript):
                if isinstance(node.slice, ast.Constant):
                    key = node.slice.value
                    if key in ("rows_written", "result"):
                        raise AssertionError(
                            f"Found code accessing [{key!r}] - processor should not inspect result schema"
                        )
