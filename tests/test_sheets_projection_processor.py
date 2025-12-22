"""Tests for ProjectionistProcessor (sheets projection)."""

from unittest.mock import MagicMock, patch, call
from dataclasses import dataclass

import pytest

from lorchestra.processors.projectionist import (
    ProjectionistProcessor,
    BqQueryServiceAdapter,
    ProjectionServicesImpl,
    _resolve_placeholders,
    PROJECTION_REGISTRY,
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
    """Create a basic job spec."""
    return {
        "job_id": "test_sheets_job",
        "job_type": "projectionist",
        "projection_name": "sheets",
        "sheets_service_factory": "gorch.sheets.factories:build_sheets_write_service",
        "sheets_service_factory_args": {"account": "gdrive"},
        "projection_config": {
            "query": "SELECT * FROM `${PROJECT}.${DATASET_CANONICAL}.test_view`",
            "spreadsheet_id": "abc123",
            "sheet_name": "test_sheet",
            "strategy": "replace",
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
# PROCESSOR TESTS
# =============================================================================


class TestProjectionistProcessor:
    """Tests for ProjectionistProcessor."""

    @patch("lorchestra.processors.projectionist.load_service_factory")
    @patch("lorchestra.processors.projectionist.PROJECTION_REGISTRY")
    def test_plan_called_before_apply(
        self,
        mock_registry,
        mock_load_factory,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that plan() is called before apply()."""
        mock_sheets_service = MagicMock()
        mock_factory = MagicMock(return_value=mock_sheets_service)
        mock_load_factory.return_value = mock_factory

        # Track call order
        call_order = []

        mock_projection_cls = MagicMock()
        mock_projection = MagicMock()
        mock_projection_cls.return_value = mock_projection
        mock_registry.get.return_value = mock_projection_cls

        mock_plan = MagicMock()
        mock_plan.plan_id = "plan-123"

        mock_result = MagicMock()
        mock_result.projection_name = "sheets"
        mock_result.plan_id = "plan-123"
        mock_result.rows_affected = 2
        mock_result.duration_seconds = 0.5
        mock_result.dry_run = False

        mock_projection.plan.side_effect = lambda *a, **k: (
            call_order.append("plan"),
            mock_plan,
        )[1]
        mock_projection.apply.side_effect = lambda *a, **k: (
            call_order.append("apply"),
            mock_result,
        )[1]

        processor = ProjectionistProcessor()
        context = MockJobContext()

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        assert call_order == ["plan", "apply"]

    @patch("lorchestra.processors.projectionist.load_service_factory")
    @patch("lorchestra.processors.projectionist.PROJECTION_REGISTRY")
    def test_events_logged_with_plan_id(
        self,
        mock_registry,
        mock_load_factory,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that events include plan_id."""
        mock_sheets_service = MagicMock()
        mock_factory = MagicMock(return_value=mock_sheets_service)
        mock_load_factory.return_value = mock_factory

        mock_projection_cls = MagicMock()
        mock_projection = MagicMock()
        mock_projection_cls.return_value = mock_projection
        mock_registry.get.return_value = mock_projection_cls

        mock_plan = MagicMock()
        mock_plan.plan_id = "my-plan-id-abc"

        mock_result = MagicMock()
        mock_result.projection_name = "sheets"
        mock_result.plan_id = "my-plan-id-abc"
        mock_result.rows_affected = 5
        mock_result.duration_seconds = 1.0
        mock_result.dry_run = False

        mock_projection.plan.return_value = mock_plan
        mock_projection.apply.return_value = mock_result

        processor = ProjectionistProcessor()
        context = MockJobContext()

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        # Check started event includes plan_id
        started_call = mock_event_client.log_event.call_args_list[0]
        assert started_call.kwargs["event_type"] == "sheets_projection.started"
        assert started_call.kwargs["payload"]["plan_id"] == "my-plan-id-abc"

        # Check completed event includes plan_id
        completed_call = mock_event_client.log_event.call_args_list[1]
        assert completed_call.kwargs["event_type"] == "sheets_projection.completed"
        assert completed_call.kwargs["payload"]["plan_id"] == "my-plan-id-abc"

    @patch("lorchestra.processors.projectionist.load_service_factory")
    @patch("lorchestra.processors.projectionist.PROJECTION_REGISTRY")
    def test_dry_run_still_generates_plan(
        self,
        mock_registry,
        mock_load_factory,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that dry_run still calls plan() and produces a plan_id."""
        mock_sheets_service = MagicMock()
        mock_factory = MagicMock(return_value=mock_sheets_service)
        mock_load_factory.return_value = mock_factory

        mock_projection_cls = MagicMock()
        mock_projection = MagicMock()
        mock_projection_cls.return_value = mock_projection
        mock_registry.get.return_value = mock_projection_cls

        mock_plan = MagicMock()
        mock_plan.plan_id = "dry-run-plan"

        mock_result = MagicMock()
        mock_result.projection_name = "sheets"
        mock_result.plan_id = "dry-run-plan"
        mock_result.rows_affected = 0  # Dry run should have 0 rows
        mock_result.duration_seconds = 0.1
        mock_result.dry_run = True

        mock_projection.plan.return_value = mock_plan
        mock_projection.apply.return_value = mock_result

        processor = ProjectionistProcessor()
        context = MockJobContext(dry_run=True)

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        # plan() should still be called
        mock_projection.plan.assert_called_once()

        # apply() should be called (projectionist handles dry_run internally)
        mock_projection.apply.assert_called_once()

        # Check that dry_run was passed in context
        plan_call = mock_projection.plan.call_args
        ctx_arg = plan_call[0][1]  # Second positional arg is ctx
        assert ctx_arg.dry_run is True

    @patch("lorchestra.processors.projectionist.load_service_factory")
    @patch("lorchestra.processors.projectionist.PROJECTION_REGISTRY")
    def test_placeholder_resolution_in_query(
        self,
        mock_registry,
        mock_load_factory,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that ${PROJECT} etc. in query are resolved."""
        mock_sheets_service = MagicMock()
        mock_factory = MagicMock(return_value=mock_sheets_service)
        mock_load_factory.return_value = mock_factory

        mock_projection_cls = MagicMock()
        mock_projection = MagicMock()
        mock_projection_cls.return_value = mock_projection
        mock_registry.get.return_value = mock_projection_cls

        mock_plan = MagicMock()
        mock_plan.plan_id = "plan-123"

        mock_result = MagicMock()
        mock_result.projection_name = "sheets"
        mock_result.plan_id = "plan-123"
        mock_result.rows_affected = 0
        mock_result.duration_seconds = 0.1
        mock_result.dry_run = False

        mock_projection.plan.return_value = mock_plan
        mock_projection.apply.return_value = mock_result

        processor = ProjectionistProcessor()
        context = MockJobContext(
            config=MockConfig(project="real-project", dataset_canonical="real_can")
        )

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        # Check that plan was called with resolved query
        plan_call = mock_projection.plan.call_args
        ctx_arg = plan_call[0][1]
        resolved_query = ctx_arg.config["query"]

        assert "real-project" in resolved_query
        assert "real_can" in resolved_query
        assert "${PROJECT}" not in resolved_query
        assert "${DATASET_CANONICAL}" not in resolved_query

    @patch("lorchestra.processors.projectionist.load_service_factory")
    def test_unknown_projection_raises(
        self, mock_load_factory, mock_storage_client, mock_event_client
    ):
        """Test that unknown projection_name raises ValueError."""
        job_spec = {
            "job_id": "test",
            "job_type": "projectionist",
            "projection_name": "unknown_projection",
            "sheets_service_factory": "gorch.sheets.factories:build_sheets_write_service",
            "sheets_service_factory_args": {},
            "projection_config": {},
        }

        processor = ProjectionistProcessor()
        context = MockJobContext()

        with pytest.raises(ValueError) as exc_info:
            processor.run(job_spec, context, mock_storage_client, mock_event_client)

        assert "Unknown projection: unknown_projection" in str(exc_info.value)


class TestProjectionistProcessorFailure:
    """Tests for failure handling."""

    @patch("lorchestra.processors.projectionist.load_service_factory")
    @patch("lorchestra.processors.projectionist.PROJECTION_REGISTRY")
    def test_failed_event_logged_on_apply_error(
        self,
        mock_registry,
        mock_load_factory,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that sheets_projection.failed is logged on apply() error."""
        mock_sheets_service = MagicMock()
        mock_factory = MagicMock(return_value=mock_sheets_service)
        mock_load_factory.return_value = mock_factory

        mock_projection_cls = MagicMock()
        mock_projection = MagicMock()
        mock_projection_cls.return_value = mock_projection
        mock_registry.get.return_value = mock_projection_cls

        mock_plan = MagicMock()
        mock_plan.plan_id = "fail-plan"

        mock_projection.plan.return_value = mock_plan
        mock_projection.apply.side_effect = RuntimeError("Sheets API error")

        processor = ProjectionistProcessor()
        context = MockJobContext()

        with pytest.raises(RuntimeError):
            processor.run(job_spec, context, mock_storage_client, mock_event_client)

        # Check failed event was logged
        failed_call = [
            c
            for c in mock_event_client.log_event.call_args_list
            if c.kwargs.get("event_type") == "sheets_projection.failed"
        ]
        assert len(failed_call) == 1
        assert failed_call[0].kwargs["status"] == "failed"
        assert "Sheets API error" in failed_call[0].kwargs["error_message"]
        assert failed_call[0].kwargs["payload"]["plan_id"] == "fail-plan"


# =============================================================================
# REGISTRY TESTS
# =============================================================================


class TestProjectionRegistry:
    """Tests for projection registry."""

    def test_sheets_projection_registered(self):
        """Test SheetsProjection is in the registry."""
        assert "sheets" in PROJECTION_REGISTRY


# =============================================================================
# SERVICE FACTORY LOADING TESTS
# =============================================================================


class TestServiceFactoryLoading:
    """Tests for service factory loading in processor."""

    @patch("lorchestra.processors.projectionist.load_service_factory")
    @patch("lorchestra.processors.projectionist.PROJECTION_REGISTRY")
    def test_factory_loaded_with_path_from_job_spec(
        self,
        mock_registry,
        mock_load_factory,
        mock_storage_client,
        mock_event_client,
        job_spec,
    ):
        """Test that factory is loaded using path from job spec."""
        mock_sheets_service = MagicMock()
        mock_factory = MagicMock(return_value=mock_sheets_service)
        mock_load_factory.return_value = mock_factory

        mock_projection_cls = MagicMock()
        mock_projection = MagicMock()
        mock_projection_cls.return_value = mock_projection
        mock_registry.get.return_value = mock_projection_cls

        mock_plan = MagicMock()
        mock_plan.plan_id = "plan-123"
        mock_result = MagicMock()
        mock_result.projection_name = "sheets"
        mock_result.plan_id = "plan-123"
        mock_result.rows_affected = 0
        mock_result.duration_seconds = 0.1
        mock_result.dry_run = False
        mock_projection.plan.return_value = mock_plan
        mock_projection.apply.return_value = mock_result

        processor = ProjectionistProcessor()
        context = MockJobContext()

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        # Verify load_service_factory was called with the factory path from job spec
        mock_load_factory.assert_called_once_with(
            "gorch.sheets.factories:build_sheets_write_service"
        )
        # Verify factory was called with args from job spec
        mock_factory.assert_called_once_with(account="gdrive")
