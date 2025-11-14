"""
Configuration management for lorchestra pipeline.

Loads and validates pipeline.yaml configuration file.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


class ConfigError(Exception):
    """Configuration validation error."""
    pass


class StageConfig:
    """Configuration for a single pipeline stage."""

    def __init__(self, name: str, data: Dict[str, Any]):
        self.name = name
        self.type = data.get("type")
        self.enabled = data.get("enabled", True)
        self.repo_path = Path(data.get("repo_path", ""))
        self.venv_path = Path(data.get("venv_path", ""))
        self.input_dir = Path(data.get("input_dir", "")) if "input_dir" in data else None
        self.output_dir = Path(data.get("output_dir", ""))
        self.retry = data.get("retry", {})
        self.validation = data.get("validation", {})

        # Stage-specific config
        self.extra = {k: v for k, v in data.items() if k not in [
            "type", "enabled", "repo_path", "venv_path", "input_dir",
            "output_dir", "retry", "validation"
        ]}

    def get(self, key: str, default: Any = None) -> Any:
        """Get extra configuration value."""
        return self.extra.get(key, default)

    def validate(self) -> None:
        """Validate stage configuration."""
        if not self.type:
            raise ConfigError(f"Stage {self.name}: missing 'type'")

        if not self.repo_path.exists():
            raise ConfigError(
                f"Stage {self.name}: repo_path does not exist: {self.repo_path}"
            )

        if self.venv_path and not self.venv_path.exists():
            # Only warn for vector-projector (not yet installed)
            if self.type != "vector-projector":
                raise ConfigError(
                    f"Stage {self.name}: venv_path does not exist: {self.venv_path}"
                )

        if not self.output_dir.parent.exists():
            raise ConfigError(
                f"Stage {self.name}: output_dir parent does not exist: {self.output_dir.parent}"
            )

    def __repr__(self) -> str:
        return f"StageConfig(name={self.name}, type={self.type}, enabled={self.enabled})"


class PipelineConfig:
    """Complete pipeline configuration."""

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.raw_config = self._load_yaml()

        # Pipeline metadata
        pipeline = self.raw_config.get("pipeline", {})
        self.name = pipeline.get("name", "unnamed-pipeline")
        self.version = pipeline.get("version", "0.0.0")
        self.description = pipeline.get("description", "")

        # Stages
        stages_data = self.raw_config.get("stages", {})
        self.stages: Dict[str, StageConfig] = {}
        for stage_name, stage_data in stages_data.items():
            self.stages[stage_name] = StageConfig(stage_name, stage_data)

        # Logging
        self.logging = self.raw_config.get("logging", {})

        # Security
        self.security = self.raw_config.get("security", {})

        # Behavior
        self.behavior = self.raw_config.get("behavior", {})

    def _load_yaml(self) -> Dict[str, Any]:
        """Load and parse YAML configuration file."""
        if not self.config_path.exists():
            raise ConfigError(f"Configuration file not found: {self.config_path}")

        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)
                if not config:
                    raise ConfigError("Configuration file is empty")
                return config
        except yaml.YAMLError as e:
            raise ConfigError(f"Invalid YAML syntax: {e}")

    def get_stage(self, name: str) -> Optional[StageConfig]:
        """Get stage configuration by name."""
        return self.stages.get(name)

    def get_enabled_stages(self) -> List[StageConfig]:
        """Get list of enabled stages in order."""
        stage_order = ["extract", "canonize", "index"]
        enabled = []
        for stage_name in stage_order:
            stage = self.stages.get(stage_name)
            if stage and stage.enabled:
                enabled.append(stage)
        return enabled

    def get_log_file_path(self) -> Path:
        """Get log file path with date interpolation."""
        log_output = self.logging.get("output", "logs/pipeline-{date}.log")
        log_output = log_output.replace("{date}", datetime.now().strftime("%Y-%m-%d"))
        return Path(log_output)

    def get_log_level(self) -> str:
        """Get logging level."""
        return self.logging.get("level", "INFO").upper()

    def get_log_format(self) -> str:
        """Get log format (structured or pretty)."""
        return self.logging.get("format", "structured")

    def should_log_to_console(self) -> bool:
        """Check if console logging is enabled."""
        return self.logging.get("console", True)

    def get_phi_directories(self) -> List[Path]:
        """Get list of PHI directories to validate."""
        phi_dirs = self.security.get("phi_directories", [])
        return [Path(d) for d in phi_dirs]

    def should_enforce_permissions(self) -> bool:
        """Check if permission enforcement is enabled."""
        return self.security.get("enforce_permissions", True)

    def get_required_permissions(self) -> str:
        """Get required permission mode for PHI directories."""
        return self.security.get("required_perms", "700")

    def should_fail_fast(self) -> bool:
        """Check if pipeline should stop on first error."""
        return self.behavior.get("fail_fast", False)

    def validate(self) -> None:
        """Validate entire configuration."""
        # Validate pipeline metadata
        if not self.name:
            raise ConfigError("Pipeline name is required")

        # Validate all stages
        for stage_name, stage in self.stages.items():
            try:
                stage.validate()
            except ConfigError as e:
                raise ConfigError(f"Stage '{stage_name}' validation failed: {e}")

        # Validate PHI directories exist
        for phi_dir in self.get_phi_directories():
            if not phi_dir.parent.exists():
                # Create parent if needed
                phi_dir.mkdir(parents=True, exist_ok=True)

        # Validate log directory
        log_file = self.get_log_file_path()
        log_dir = log_file.parent
        if not log_dir.exists():
            log_dir.mkdir(parents=True, exist_ok=True)

    def __repr__(self) -> str:
        return f"PipelineConfig(name={self.name}, version={self.version}, stages={len(self.stages)})"


def load_config(config_path: Optional[Path] = None) -> PipelineConfig:
    """
    Load pipeline configuration from YAML file.

    Args:
        config_path: Path to config file. Defaults to config/pipeline.yaml

    Returns:
        PipelineConfig instance

    Raises:
        ConfigError: If config is invalid or missing
    """
    if config_path is None:
        # Default to config/pipeline.yaml relative to this file
        config_path = Path(__file__).parent.parent / "config" / "pipeline.yaml"

    return PipelineConfig(config_path)
