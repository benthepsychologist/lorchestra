import os
import pytest
import yaml
from pathlib import Path
from lorchestra.config import load_config, get_lorchestra_home, LorchestraConfig

def test_get_lorchestra_home_default(monkeypatch):
    monkeypatch.delenv("LORCHESTRA_HOME", raising=False)
    home = get_lorchestra_home()
    assert home == Path("~/.config/lorchestra").expanduser()

def test_get_lorchestra_home_env_var(monkeypatch, tmp_path):
    custom_home = tmp_path / "custom_home"
    monkeypatch.setenv("LORCHESTRA_HOME", str(custom_home))
    assert get_lorchestra_home() == custom_home

def test_load_config_missing_file(monkeypatch, tmp_path):
    monkeypatch.setenv("LORCHESTRA_HOME", str(tmp_path))
    with pytest.raises(FileNotFoundError, match="lorchestra config.yaml not found"):
        load_config()

def test_load_config_valid(monkeypatch, tmp_path):
    monkeypatch.setenv("LORCHESTRA_HOME", str(tmp_path))
    config_path = tmp_path / "config.yaml"
    
    config_data = {
        "project": "test-project",
        "dataset_raw": "raw_db",
        "dataset_canonical": "canon_db",
        "dataset_derived": "derived_db",
        "sqlite_path": "~/test.db",
        "local_views_root": "~/views",
        "canonizer_registry_root": "/tmp/canon",
        "formation_registry_root": "/tmp/form"
    }
    config_path.write_text(yaml.dump(config_data))
    
    cfg = load_config()
    assert isinstance(cfg, LorchestraConfig)
    assert cfg.project == "test-project"
    assert cfg.dataset_raw == "raw_db"
    assert cfg.canonizer_registry_root == "/tmp/canon"

def test_load_config_with_env_file(monkeypatch, tmp_path):
    monkeypatch.setenv("LORCHESTRA_HOME", str(tmp_path))
    config_path = tmp_path / "config.yaml"
    env_file = tmp_path / ".env.test"
    
    env_file.write_text("TEST_VAR=loaded_from_env")
    
    config_data = {
        "project": "test",
        "dataset_raw": "r",
        "dataset_canonical": "c",
        "dataset_derived": "d",
        "sqlite_path": "s",
        "local_views_root": "l",
        "env_file": str(env_file)
    }
    config_path.write_text(yaml.dump(config_data))
    
    # Pre-clean env var
    monkeypatch.delenv("TEST_VAR", raising=False)
    
    load_config()
    assert os.environ.get("TEST_VAR") == "loaded_from_env"
