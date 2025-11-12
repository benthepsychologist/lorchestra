"""
Base classes for pipeline stages.

All stages inherit from Stage and return StageResult.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from lorch.config import StageConfig


@dataclass
class StageResult:
    """Result of stage execution."""

    stage_name: str
    success: bool
    duration_seconds: float
    records_processed: int = 0
    output_files: List[Path] = field(default_factory=list)
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "stage_name": self.stage_name,
            "success": self.success,
            "duration_seconds": self.duration_seconds,
            "records_processed": self.records_processed,
            "output_files": [str(f) for f in self.output_files],
            "error_message": self.error_message,
            "metadata": self.metadata,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
        }


class Stage(ABC):
    """
    Abstract base class for pipeline stages.

    Each stage must implement:
    - validate(): Check prerequisites before execution
    - execute(): Run the stage
    - cleanup(): Clean up resources after execution
    """

    def __init__(self, config: StageConfig, logger: logging.Logger):
        """
        Initialize stage.

        Args:
            config: Stage configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger
        self.name = config.name

    @abstractmethod
    def validate(self) -> None:
        """
        Validate stage prerequisites.

        Raises:
            Exception: If validation fails
        """
        pass

    @abstractmethod
    def execute(self) -> StageResult:
        """
        Execute the stage.

        Returns:
            StageResult with execution details

        Raises:
            Exception: If execution fails
        """
        pass

    def cleanup(self) -> None:
        """
        Clean up resources after stage execution.

        Override if stage needs cleanup.
        """
        pass

    def run(self) -> StageResult:
        """
        Run the complete stage lifecycle.

        Returns:
            StageResult with execution details
        """
        import time

        self.logger.info(
            f"Starting stage: {self.name}",
            extra={"stage": self.name, "event": "stage_started"},
        )

        started_at = datetime.utcnow()
        start_time = time.time()

        try:
            # Validate
            self.validate()
            self.logger.info(
                f"Stage {self.name} validation passed",
                extra={"stage": self.name, "event": "validation_passed"},
            )

            # Execute
            result = self.execute()
            result.started_at = started_at
            result.ended_at = datetime.utcnow()
            result.duration_seconds = time.time() - start_time

            if result.success:
                self.logger.info(
                    f"Stage {self.name} completed successfully",
                    extra={
                        "stage": self.name,
                        "event": "stage_completed",
                        "metadata": {
                            "duration_seconds": result.duration_seconds,
                            "records_processed": result.records_processed,
                            "output_files_count": len(result.output_files),
                        },
                    },
                )
            else:
                self.logger.error(
                    f"Stage {self.name} failed: {result.error_message}",
                    extra={
                        "stage": self.name,
                        "event": "stage_failed",
                        "metadata": {"error": result.error_message},
                    },
                )

            return result

        except Exception as e:
            duration = time.time() - start_time

            self.logger.error(
                f"Stage {self.name} failed with exception: {e}",
                extra={
                    "stage": self.name,
                    "event": "stage_exception",
                    "metadata": {"exception": str(e)},
                },
                exc_info=True,
            )

            return StageResult(
                stage_name=self.name,
                success=False,
                duration_seconds=duration,
                error_message=str(e),
                started_at=started_at,
                ended_at=datetime.utcnow(),
            )

        finally:
            # Cleanup
            try:
                self.cleanup()
            except Exception as e:
                self.logger.warning(
                    f"Stage {self.name} cleanup failed: {e}",
                    extra={"stage": self.name, "event": "cleanup_failed"},
                )

    def _validate_output_dir(self) -> None:
        """Validate output directory exists and is writable."""
        output_dir = self.config.output_dir

        if not output_dir.parent.exists():
            raise FileNotFoundError(
                f"Output directory parent does not exist: {output_dir.parent}"
            )

        # Create output dir if needed
        output_dir.mkdir(parents=True, exist_ok=True)

        # Test writability
        test_file = output_dir / ".lorch_write_test"
        try:
            test_file.write_text("test")
            test_file.unlink()
        except Exception as e:
            raise PermissionError(
                f"Output directory {output_dir} is not writable: {e}"
            )

    def _validate_input_files(self, patterns: List[str] = None) -> List[Path]:
        """
        Validate input files exist.

        Args:
            patterns: Optional glob patterns to match

        Returns:
            List of input file paths

        Raises:
            FileNotFoundError: If no input files found
        """
        if not self.config.input_dir:
            return []

        input_dir = self.config.input_dir

        if not input_dir.exists():
            raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

        if patterns:
            input_files = []
            for pattern in patterns:
                input_files.extend(input_dir.glob(pattern))
        else:
            # Default: all .jsonl files
            input_files = list(input_dir.glob("*.jsonl"))

        if not input_files:
            raise FileNotFoundError(
                f"No input files found in {input_dir} matching patterns: {patterns}"
            )

        return input_files

    def _get_retry_config(self) -> Dict[str, Any]:
        """Get retry configuration for this stage."""
        retry_config = self.config.retry
        return {
            "enabled": retry_config.get("enabled", False),
            "max_attempts": retry_config.get("max_attempts", 3),
            "backoff_seconds": retry_config.get("backoff_seconds", 60),
            "backoff_multiplier": retry_config.get("backoff_multiplier", 2.0),
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, type={self.config.type})"
