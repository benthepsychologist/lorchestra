"""Base class for tool adapters."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict


class ToolAdapter(ABC):
    """
    Base class for tool adapters.

    Tool adapters provide a standardized interface for lorchestra to interact with
    external tools (meltano, canonizer, vector-projector, etc.). Each adapter
    handles configuration sync, validation, and execution for its specific tool.
    """

    def __init__(self, config_path: Path):
        """
        Initialize the tool adapter.

        Args:
            config_path: Path to the tool's configuration file in lorchestra's cache
        """
        self.config_path = config_path
        self.config = self.load_config()

    @abstractmethod
    def load_config(self) -> Dict[str, Any]:
        """
        Load tool configuration from the cached config file.

        Returns:
            Dictionary containing the tool's configuration

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file is invalid
        """
        pass

    @abstractmethod
    def sync_config(self) -> None:
        """
        Sync configuration from the tool's native source to lorchestra's cache.

        This is typically a one-way sync from the tool's config file
        (e.g., meltano.yml) to lorch's cached version (e.g., config/tools/meltano.yaml).

        Raises:
            FileNotFoundError: If source config doesn't exist
            ValueError: If source config is invalid
        """
        pass

    @abstractmethod
    def validate(self) -> Dict[str, Any]:
        """
        Validate the tool's configuration and setup.

        Returns:
            Dictionary with keys:
                - 'valid': bool indicating if validation passed
                - 'errors': list of error messages (empty if valid)
                - 'warnings': list of warning messages (optional)
        """
        pass

    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        """
        Execute a tool command.

        Args:
            *args: Positional arguments for the tool command
            **kwargs: Keyword arguments for the tool command

        Returns:
            Tool-specific return value (typically subprocess.CompletedProcess)

        Raises:
            ValueError: If command arguments are invalid
            RuntimeError: If command execution fails
        """
        pass
