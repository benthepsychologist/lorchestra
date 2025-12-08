import pytest
from lorchestra.config import LorchestraConfig

@pytest.fixture
def test_config():
    return LorchestraConfig(
        project="test-project",
        dataset_raw="test_events",
        dataset_canonical="test_canonical",
        dataset_derived="test_derived",
        sqlite_path="/tmp/test.db",
        local_views_root="/tmp/views",
        canonizer_registry_root="/tmp/canonizer",
        formation_registry_root="/tmp/formation",
    )

from unittest.mock import patch

@pytest.fixture(autouse=True)
def mock_load_config(request, test_config):
    # Don't patch for config tests
    if "test_config" in request.module.__name__ or "test_cli_config" in request.module.__name__:
        yield
        return

    with patch("lorchestra.config.load_config", return_value=test_config):
        yield
