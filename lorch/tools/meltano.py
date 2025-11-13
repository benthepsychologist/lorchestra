"""Meltano tool adapter for lorch."""

import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from lorch.tools.base import ToolAdapter


class MeltanoAdapter(ToolAdapter):
    """
    Adapter for meltano extract/load tool.

    This adapter provides validation, configuration sync, and execution
    capabilities for meltano-based data pipelines.
    """

    def __init__(
        self,
        meltano_dir: Path,
        config_cache: Optional[Path] = None,
    ):
        """
        Initialize MeltanoAdapter.

        Args:
            meltano_dir: Path to meltano project directory (e.g., /home/user/meltano-ingest)
            config_cache: Path to cached config file (defaults to config/tools/meltano.yaml)
        """
        self.meltano_dir = Path(meltano_dir)
        self.meltano_yml = self.meltano_dir / "meltano.yml"

        if config_cache is None:
            config_cache = Path("config/tools/meltano.yaml")

        super().__init__(config_cache)

    def load_config(self) -> Dict[str, Any]:
        """
        Load tool configuration from the cached config file.

        Returns:
            Dictionary containing extractors, loaders, and jobs configuration

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file is invalid
        """
        if not self.config_path.exists():
            return {
                "extractors": {},
                "loaders": {},
                "jobs": [],
                "environments": {},
            }

        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)
                return config or {}
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {self.config_path}: {e}")

    def sync_config(self) -> None:
        """
        Sync configuration from meltano.yml to lorch's cached config.

        Parses meltano.yml and extracts:
        - extractors configuration
        - loaders configuration
        - jobs configuration
        - environments configuration

        Raises:
            FileNotFoundError: If meltano.yml doesn't exist
            ValueError: If meltano.yml is invalid
        """
        if not self.meltano_yml.exists():
            raise FileNotFoundError(
                f"meltano.yml not found at {self.meltano_yml}"
            )

        try:
            with open(self.meltano_yml, "r") as f:
                meltano_config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {self.meltano_yml}: {e}")

        # Extract relevant configuration
        plugins = meltano_config.get("plugins", {})
        extractors_list = plugins.get("extractors", [])
        loaders_list = plugins.get("loaders", [])

        # Convert lists to dicts keyed by name for easier lookup
        extractors = {e["name"]: e for e in extractors_list}
        loaders = {l["name"]: l for l in loaders_list}

        # Extract jobs configuration
        jobs = meltano_config.get("jobs", [])

        # Extract environments
        environments = {
            env["name"]: env for env in meltano_config.get("environments", [])
        }

        # Build cached config
        cached_config = {
            "extractors": extractors,
            "loaders": loaders,
            "jobs": jobs,
            "environments": environments,
            "meltano_dir": str(self.meltano_dir),
            "synced_at": None,  # Will be set by CLI command
        }

        # Write to cache
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w") as f:
            yaml.safe_dump(cached_config, f, default_flow_style=False, sort_keys=False)

        # Reload config
        self.config = self.load_config()

    def validate(self) -> Dict[str, Any]:
        """
        Validate meltano configuration and setup.

        Returns:
            Dictionary with keys:
                - 'valid': bool indicating if validation passed
                - 'errors': list of error messages
                - 'warnings': list of warning messages
        """
        errors = []
        warnings = []

        # Check if meltano directory exists
        if not self.meltano_dir.exists():
            errors.append(f"Meltano directory not found: {self.meltano_dir}")

        # Check if meltano.yml exists
        if not self.meltano_yml.exists():
            errors.append(f"meltano.yml not found: {self.meltano_yml}")

        # Check if config has been synced
        if not self.config or not self.config.get("extractors"):
            warnings.append(
                "Config cache is empty or not synced. Run 'lorch config sync meltano' first."
            )

        # Check if meltano executable exists
        meltano_bin = self.meltano_dir / ".venv" / "bin" / "meltano"
        if not meltano_bin.exists():
            errors.append(f"Meltano executable not found: {meltano_bin}")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
        }

    def validate_task(self, tap: str, target: str) -> Dict[str, Any]:
        """
        Validate a specific tap-target pair before execution.

        Args:
            tap: Name of the extractor (e.g., 'tap-gmail--acct1-personal')
            target: Name of the loader (e.g., 'target-jsonl-chunked--gmail-ben-mensio')

        Returns:
            Dictionary with keys:
                - 'valid': bool indicating if validation passed
                - 'errors': list of error messages
                - 'warnings': list of warning messages
        """
        errors = []
        warnings = []

        # Check tap exists
        if tap not in self.config.get("extractors", {}):
            errors.append(f"Tap '{tap}' not found in meltano configuration")
        else:
            # Gmail-specific validation
            if "gmail" in tap.lower():
                tap_config = self.config["extractors"][tap]

                # Check for message_list stream issue
                select = tap_config.get("select", [])
                config = tap_config.get("config", {})

                # If messages.q filter is set, message_list must NOT be excluded
                if config.get("messages.q"):
                    if "!message_list.*.*" in select:
                        errors.append(
                            f"Tap '{tap}' has messages.q filter but message_list stream is excluded. "
                            "Filter will not work. Either remove messages.q or enable message_list stream."
                        )

                # Check for base64 bloat issue
                if "!messages.raw" not in select:
                    warnings.append(
                        f"Tap '{tap}' does not exclude 'messages.raw' field. "
                        "This may result in large file sizes due to base64-encoded email content."
                    )

        # Check target exists
        if target not in self.config.get("loaders", {}):
            errors.append(f"Target '{target}' not found in meltano configuration")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
        }

    def run_task(
        self,
        tap: str,
        target: str,
        validate: bool = True,
        **kwargs,
    ) -> subprocess.CompletedProcess:
        """
        Execute meltano run <tap> <target>.

        Args:
            tap: Name of the extractor
            target: Name of the loader
            validate: If True, validate task before execution (default: True)
            **kwargs: Additional arguments passed to subprocess.run

        Returns:
            subprocess.CompletedProcess result

        Raises:
            ValueError: If task validation fails
            RuntimeError: If meltano command fails
        """
        # Validate task if requested
        if validate:
            validation = self.validate_task(tap, target)
            if not validation["valid"]:
                raise ValueError(
                    f"Task validation failed: {', '.join(validation['errors'])}"
                )

            # Log warnings but don't fail
            for warning in validation.get("warnings", []):
                print(f"WARNING: {warning}")

        # Build meltano command
        meltano_bin = self.meltano_dir / ".venv" / "bin" / "meltano"
        cmd = [str(meltano_bin), "run", tap, target]

        # Execute
        result = subprocess.run(
            cmd,
            cwd=self.meltano_dir,
            **kwargs,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Meltano command failed with exit code {result.returncode}"
            )

        return result

    def execute(self, *args, **kwargs) -> subprocess.CompletedProcess:
        """
        Execute a meltano command.

        This is a generic execution method. For running tap-target pairs,
        prefer using run_task() which includes validation.

        Args:
            *args: Command arguments (e.g., 'run', 'tap-gmail', 'target-jsonl')
            **kwargs: Additional arguments passed to subprocess.run

        Returns:
            subprocess.CompletedProcess result
        """
        meltano_bin = self.meltano_dir / ".venv" / "bin" / "meltano"
        cmd = [str(meltano_bin)] + list(args)

        return subprocess.run(
            cmd,
            cwd=self.meltano_dir,
            **kwargs,
        )
