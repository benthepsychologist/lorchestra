import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from lorchestra.config import LorchestraConfig

# Ensure repo root is on sys.path so `import lorchestra` works in tests.
repo_root = Path(__file__).parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# Avoid import collisions with dependency packages.
#
# Pytest can end up adding the `tests/` directory itself to `sys.path` depending
# on import mode. This repo contains `tests/storacle/`, which can accidentally
# shadow the real `storacle` dependency and break imports like `from storacle.rpc
# import execute_plan`.
tests_dir = Path(__file__).parent


@pytest.fixture(autouse=True)
def _prefer_sibling_repo_dependencies():
    """Prefer sibling-repo deps over same-named test folders.

    Pytest can insert `/workspace/lorchestra/tests` on sys.path, and this repo
    contains `tests/storacle/` which can shadow the real `storacle` dependency.
    Ensure `/workspace/storacle/src` is always before `/workspace/lorchestra/tests`.
    """

    storacle_src = "/workspace/storacle/src"
    if Path(storacle_src).exists():
        if storacle_src in sys.path:
            sys.path.remove(storacle_src)
        sys.path.insert(0, storacle_src)

    # If something already imported a shadowed `storacle`, evict it.
    if "storacle" in sys.modules:
        storacle_mod = sys.modules["storacle"]
        storacle_file = getattr(storacle_mod, "__file__", None) or ""
        storacle_paths = [str(p) for p in getattr(storacle_mod, "__path__", [])]
        if "/lorchestra/tests/storacle" in storacle_file or any(
            "/lorchestra/tests/storacle" in p for p in storacle_paths
        ):
            for name in list(sys.modules):
                if name == "storacle" or name.startswith("storacle."):
                    del sys.modules[name]

    yield

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

@pytest.fixture(autouse=True)
def mock_load_config(request, test_config):
    # Don't patch for config tests
    if "test_config" in request.module.__name__ or "test_cli_config" in request.module.__name__:
        yield
        return

    with patch("lorchestra.config.load_config", return_value=test_config):
        yield
