"""Tests for lorchestra.registry module.

Tests JobRegistry loading, caching, extras capture, and .yaml path resolution.
"""

import json
import pytest
from pathlib import Path

import yaml

from lorchestra.schemas import JobDef, Op
from lorchestra.registry import JobRegistry, JobNotFoundError, JobValidationError


@pytest.fixture
def tmp_defs(tmp_path):
    """Create a temporary definitions directory."""
    return tmp_path


def _write_yaml(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(data, f)


class TestExtrasCapture:
    """Tests that extra YAML fields are captured in JobDef.extras."""

    def test_extras_from_dict(self):
        data = {
            "job_id": "test",
            "version": "2.0",
            "steps": [],
            "config": {"key": "value"},
            "description": "A test job",
        }
        job_def = JobDef.from_dict(data)
        assert job_def.extras == {"config": {"key": "value"}, "description": "A test job"}

    def test_no_extras(self):
        data = {"job_id": "test", "version": "2.0", "steps": []}
        job_def = JobDef.from_dict(data)
        assert job_def.extras == {}

    def test_extras_in_to_dict(self):
        job_def = JobDef(
            job_id="test",
            version="2.0",
            extras={"config": {"spreadsheet_id": "abc"}},
        )
        d = job_def.to_dict()
        assert d["config"] == {"spreadsheet_id": "abc"}
        assert "extras" not in d  # spread at top level, not nested

    def test_extras_round_trip(self):
        original = JobDef(
            job_id="test",
            version="2.0",
            extras={"description": "hello", "config": {"a": 1}},
        )
        restored = JobDef.from_dict(original.to_dict())
        assert restored.extras == original.extras


class TestYamlPathLoading:
    """Tests that .yaml file paths in extras get loaded by the registry."""

    def test_yaml_path_resolved(self, tmp_defs):
        # Create a config file
        config_dir = tmp_defs / "config"
        config_dir.mkdir()
        _write_yaml(config_dir / "sheets.yaml", {
            "spreadsheet_id": "abc123",
            "account": "acct1",
        })

        # Create a job that references it
        _write_yaml(tmp_defs / "test_job.yaml", {
            "job_id": "test_job",
            "version": "2.0",
            "config": "config/sheets.yaml",
            "steps": [],
        })

        registry = JobRegistry(tmp_defs)
        job_def = registry.load("test_job")

        assert job_def.extras["config"] == {
            "spreadsheet_id": "abc123",
            "account": "acct1",
        }

    def test_yml_extension_resolved(self, tmp_defs):
        config_dir = tmp_defs / "config"
        config_dir.mkdir()
        _write_yaml(config_dir / "settings.yml", {"key": "value"})

        _write_yaml(tmp_defs / "test_job.yaml", {
            "job_id": "test_job",
            "version": "2.0",
            "settings": "config/settings.yml",
            "steps": [],
        })

        registry = JobRegistry(tmp_defs)
        job_def = registry.load("test_job")
        assert job_def.extras["settings"] == {"key": "value"}

    def test_non_yaml_string_not_loaded(self, tmp_defs):
        _write_yaml(tmp_defs / "test_job.yaml", {
            "job_id": "test_job",
            "version": "2.0",
            "description": "This is a plain string",
            "steps": [],
        })

        registry = JobRegistry(tmp_defs)
        job_def = registry.load("test_job")
        assert job_def.extras["description"] == "This is a plain string"

    def test_missing_yaml_path_raises(self, tmp_defs):
        _write_yaml(tmp_defs / "test_job.yaml", {
            "job_id": "test_job",
            "version": "2.0",
            "config": "config/nonexistent.yaml",
            "steps": [],
        })

        registry = JobRegistry(tmp_defs)
        with pytest.raises(JobValidationError, match="Failed to load config file"):
            registry.load("test_job")

    def test_known_keys_not_scanned(self, tmp_defs):
        """job_id, version, steps are not scanned for .yaml paths."""
        _write_yaml(tmp_defs / "test_job.yaml", {
            "job_id": "test_job",
            "version": "2.0",
            "steps": [],
        })

        registry = JobRegistry(tmp_defs)
        job_def = registry.load("test_job")
        # No error — known keys are skipped
        assert job_def.job_id == "test_job"

    def test_nested_job_in_subdir(self, tmp_defs):
        """Job in subdirectory resolves config paths relative to definitions_dir."""
        config_dir = tmp_defs / "config"
        config_dir.mkdir()
        _write_yaml(config_dir / "pm.yaml", {"sheet_id": "xyz"})

        sub_dir = tmp_defs / "projection" / "sync"
        sub_dir.mkdir(parents=True)
        _write_yaml(sub_dir / "my_job.yaml", {
            "job_id": "my_job",
            "version": "2.0",
            "config": "config/pm.yaml",
            "steps": [],
        })

        registry = JobRegistry(tmp_defs)
        job_def = registry.load("my_job")
        assert job_def.extras["config"] == {"sheet_id": "xyz"}
