"""
Pipeline orchestrator for lorch.

Coordinates execution of extract → canonize → index stages.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from lorch.config import PipelineConfig, load_config
from lorch.stages.base import Stage, StageResult
from lorch.stages.canonize import CanonizeStage
from lorch.stages.extract import ExtractStage
from lorch.stages.index import IndexStage
from lorch.utils import (
    format_duration,
    print_banner,
    print_error,
    print_info,
    print_success,
    print_warning,
    setup_logging,
    validate_phi_permissions,
)


@dataclass
class PipelineResult:
    """Result of complete pipeline execution."""

    success: bool
    started_at: datetime
    ended_at: datetime
    duration_seconds: float
    stages: Dict[str, StageResult] = field(default_factory=dict)
    error_message: Optional[str] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "stages": {name: result.to_dict() for name, result in self.stages.items()},
            "error_message": self.error_message,
        }


class Pipeline:
    """
    Main pipeline orchestrator.

    Coordinates execution of all pipeline stages with validation,
    error handling, and logging.
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        """
        Initialize pipeline.

        Args:
            config: Pipeline configuration (defaults to config/pipeline.yaml)
        """
        self.config = config or load_config()
        self.logger: Optional[logging.Logger] = None

    def validate(self, skip_permissions: bool = False) -> None:
        """
        Validate pipeline configuration and prerequisites.

        Args:
            skip_permissions: Skip PHI directory permission checks

        Raises:
            Exception: If validation fails
        """
        print_banner("Pipeline Validation")

        # Validate configuration
        print_info("Validating configuration...")
        try:
            self.config.validate()
            print_success("Configuration valid")
        except Exception as e:
            print_error(f"Configuration invalid: {e}")
            raise

        # Validate PHI directory permissions
        if not skip_permissions and self.config.should_enforce_permissions():
            print_info("Validating PHI directory permissions...")
            phi_dirs = self.config.get_phi_directories()
            required_perms = self.config.get_required_permissions()

            for phi_dir in phi_dirs:
                try:
                    if phi_dir.exists():
                        validate_phi_permissions(phi_dir, required_perms)
                        print_success(f"  {phi_dir}: {required_perms} ✓")
                    else:
                        print_warning(f"  {phi_dir}: does not exist (will be created)")
                except PermissionError as e:
                    print_error(f"  {phi_dir}: {e}")
                    raise

        # Validate each enabled stage
        print_info("Validating pipeline stages...")
        enabled_stages = self.config.get_enabled_stages()

        for stage_config in enabled_stages:
            try:
                stage = self._create_stage(stage_config)
                stage.validate()
                print_success(f"  {stage_config.name}: valid")
            except Exception as e:
                print_error(f"  {stage_config.name}: {e}")
                raise

        print_success(f"Pipeline validation complete ({len(enabled_stages)} stages)")

    def run(
        self,
        stages: Optional[List[str]] = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> PipelineResult:
        """
        Run pipeline stages.

        Args:
            stages: List of stage names to run (default: all enabled)
            dry_run: Validate only, don't execute
            verbose: Enable debug logging

        Returns:
            PipelineResult with execution details
        """
        import time

        started_at = datetime.utcnow()
        start_time = time.time()

        # Setup logging
        log_level = "DEBUG" if verbose else self.config.get_log_level()
        log_file = self.config.get_log_file_path()
        log_format = self.config.get_log_format()
        console_output = self.config.should_log_to_console()

        self.logger = setup_logging(log_file, log_level, log_format, console_output)

        self.logger.info(
            f"Starting pipeline: {self.config.name} v{self.config.version}",
            extra={
                "event": "pipeline_started",
                "metadata": {
                    "name": self.config.name,
                    "version": self.config.version,
                    "dry_run": dry_run,
                },
            },
        )

        print_banner(f"{self.config.name} v{self.config.version}")

        try:
            # Validate
            self.validate()

            if dry_run:
                print_info("Dry run mode - validation complete, skipping execution")
                return PipelineResult(
                    success=True,
                    started_at=started_at,
                    ended_at=datetime.utcnow(),
                    duration_seconds=time.time() - start_time,
                )

            # Determine stages to run
            if stages:
                stage_configs = [self.config.get_stage(name) for name in stages]
                stage_configs = [s for s in stage_configs if s and s.enabled]
            else:
                stage_configs = self.config.get_enabled_stages()

            if not stage_configs:
                raise ValueError("No stages to execute")

            print_info(f"Executing {len(stage_configs)} stages...")

            # Execute stages
            stage_results = {}
            fail_fast = self.config.should_fail_fast()

            for stage_config in stage_configs:
                stage = self._create_stage(stage_config)

                self.logger.info(
                    f"Executing stage: {stage.name}",
                    extra={"event": "stage_starting", "stage": stage.name},
                )

                result = stage.run()
                stage_results[stage.name] = result

                if result.success:
                    print_success(
                        f"{stage.name}: {result.records_processed} records, "
                        f"{len(result.output_files)} files, "
                        f"{format_duration(result.duration_seconds)}"
                    )
                else:
                    print_error(f"{stage.name}: {result.error_message}")

                    if fail_fast:
                        self.logger.error(
                            f"Pipeline failed at stage {stage.name} (fail_fast=true)",
                            extra={
                                "event": "pipeline_failed",
                                "stage": stage.name,
                                "error": result.error_message,
                            },
                        )

                        return PipelineResult(
                            success=False,
                            started_at=started_at,
                            ended_at=datetime.utcnow(),
                            duration_seconds=time.time() - start_time,
                            stages=stage_results,
                            error_message=f"Stage {stage.name} failed: {result.error_message}",
                        )

            # Check if all stages succeeded
            all_success = all(result.success for result in stage_results.values())

            ended_at = datetime.utcnow()
            duration = time.time() - start_time

            if all_success:
                print_success(f"Pipeline completed successfully in {format_duration(duration)}")
                self.logger.info(
                    "Pipeline completed successfully",
                    extra={
                        "event": "pipeline_completed",
                        "metadata": {"duration_seconds": duration},
                    },
                )
            else:
                failed_stages = [
                    name for name, result in stage_results.items() if not result.success
                ]
                print_warning(
                    f"Pipeline completed with failures: {', '.join(failed_stages)}"
                )
                self.logger.warning(
                    f"Pipeline completed with failures: {', '.join(failed_stages)}",
                    extra={
                        "event": "pipeline_completed_with_failures",
                        "metadata": {"failed_stages": failed_stages},
                    },
                )

            # Save pipeline state
            self._save_state(
                PipelineResult(
                    success=all_success,
                    started_at=started_at,
                    ended_at=ended_at,
                    duration_seconds=duration,
                    stages=stage_results,
                )
            )

            return PipelineResult(
                success=all_success,
                started_at=started_at,
                ended_at=ended_at,
                duration_seconds=duration,
                stages=stage_results,
            )

        except Exception as e:
            ended_at = datetime.utcnow()
            duration = time.time() - start_time

            print_error(f"Pipeline failed: {e}")

            if self.logger:
                self.logger.error(
                    f"Pipeline failed with exception: {e}",
                    extra={"event": "pipeline_exception", "metadata": {"exception": str(e)}},
                    exc_info=True,
                )

            return PipelineResult(
                success=False,
                started_at=started_at,
                ended_at=ended_at,
                duration_seconds=duration,
                error_message=str(e),
            )

    def status(self) -> Optional[PipelineResult]:
        """
        Get status of last pipeline run.

        Returns:
            PipelineResult from last run, or None if no previous run
        """
        state_file = self._get_state_file()

        if not state_file.exists():
            return None

        try:
            with open(state_file, "r") as f:
                state_data = json.load(f)

            # Reconstruct PipelineResult
            return PipelineResult(
                success=state_data["success"],
                started_at=datetime.fromisoformat(state_data["started_at"]),
                ended_at=datetime.fromisoformat(state_data["ended_at"]),
                duration_seconds=state_data["duration_seconds"],
                error_message=state_data.get("error_message"),
            )

        except Exception as e:
            if self.logger:
                self.logger.warning(f"Could not load pipeline state: {e}")
            return None

    def _create_stage(self, stage_config) -> Stage:
        """
        Create stage instance from configuration.

        Args:
            stage_config: Stage configuration

        Returns:
            Stage instance

        Raises:
            ValueError: If stage type is unknown
        """
        # Get or create logger
        if not self.logger:
            log_file = self.config.get_log_file_path()
            self.logger = setup_logging(log_file, "INFO", "structured", True)

        stage_type = stage_config.type

        if stage_type == "meltano":
            return ExtractStage(stage_config, self.logger)
        elif stage_type == "canonizer":
            return CanonizeStage(stage_config, self.logger)
        elif stage_type == "vector-projector":
            return IndexStage(stage_config, self.logger)
        else:
            raise ValueError(f"Unknown stage type: {stage_type}")

    def _save_state(self, result: PipelineResult) -> None:
        """
        Save pipeline state to disk.

        Args:
            result: Pipeline result to save
        """
        state_file = self._get_state_file()
        state_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(state_file, "w") as f:
                json.dump(result.to_dict(), f, indent=2)

            if self.logger:
                self.logger.debug(
                    f"Saved pipeline state to {state_file}",
                    extra={"event": "state_saved", "metadata": {"file": str(state_file)}},
                )

        except Exception as e:
            if self.logger:
                self.logger.warning(
                    f"Could not save pipeline state: {e}",
                    extra={"event": "state_save_failed", "metadata": {"error": str(e)}},
                )

    def _get_state_file(self) -> Path:
        """Get path to pipeline state file."""
        log_file = self.config.get_log_file_path()
        state_file = log_file.parent / "state.json"
        return state_file
