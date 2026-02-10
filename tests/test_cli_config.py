import os
import shutil
import pytest
import yaml
from click.testing import CliRunner
from lorchestra.cli import main
from lorchestra.config import LorchestraConfig

@pytest.fixture
def runner():
    return CliRunner()

def test_init_command_creates_files(runner, tmp_path, monkeypatch):
    home = tmp_path / "custom_home"
    monkeypatch.setenv("LORCHESTRA_HOME", str(home))
    
    result = runner.invoke(main, ["init"])
    assert result.exit_code == 0
    assert "Initialized lorchestra config" in result.output
    
    assert (home / "config.yaml").exists()
    assert (home / ".env").exists()
    
    cfg = yaml.safe_load((home / "config.yaml").read_text())
    assert cfg["project"] == "lifeos-dev"

def test_init_does_not_overwrite_without_force(runner, tmp_path, monkeypatch):
    home = tmp_path / "custom_home"
    monkeypatch.setenv("LORCHESTRA_HOME", str(home))
    home.mkdir(parents=True)
    
    (home / "config.yaml").write_text("existing: true")
    
    result = runner.invoke(main, ["init"])
    assert result.exit_code == 1
    assert "Config already exists" in result.output
    
    assert (home / "config.yaml").read_text() == "existing: true"

def test_init_force_overwrites(runner, tmp_path, monkeypatch):
    home = tmp_path / "custom_home"
    monkeypatch.setenv("LORCHESTRA_HOME", str(home))
    home.mkdir(parents=True)
    
    (home / "config.yaml").write_text("existing: true")
    
    result = runner.invoke(main, ["init", "--force"])
    assert result.exit_code == 0
    
    cfg = yaml.safe_load((home / "config.yaml").read_text())
    assert "project" in cfg

