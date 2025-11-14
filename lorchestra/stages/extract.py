"""
Extract stage: Run Meltano extraction jobs.

Executes Meltano CLI to pull data from configured sources.
"""

import subprocess
from pathlib import Path
from typing import List

from lorchestra.config import StageConfig
from lorchestra.stages.base import Stage, StageResult
from lorchestra.tools.meltano import MeltanoAdapter
from lorchestra.utils import count_jsonl_records, retry_with_backoff


class ExtractStage(Stage):
    """
    Extract stage using Meltano.

    Runs Meltano job to extract data from sources and write to JSONL files.
    """

    def validate(self) -> None:
        """Validate Meltano installation and configuration using adapter."""
        # Create adapter for validation
        adapter = MeltanoAdapter(
            meltano_dir=self.config.repo_path,
            config_cache=Path("config/tools/meltano.yaml"),
        )

        # Use adapter validation
        validation = adapter.validate()

        # Check for errors
        if not validation["valid"]:
            errors = validation["errors"]
            raise ValueError(
                f"Meltano validation failed: {'; '.join(errors)}"
            )

        # Log warnings
        for warning in validation.get("warnings", []):
            self.logger.warning(
                f"Meltano validation warning: {warning}",
                extra={
                    "stage": self.name,
                    "event": "validation_warning",
                    "metadata": {"warning": warning},
                },
            )

        # Validate output directory
        self._validate_output_dir()

    def execute(self) -> StageResult:
        """Execute Meltano extraction job."""
        job_name = self.config.get("job", "ingest-all-accounts")

        # Validate using adapter before execution
        adapter = MeltanoAdapter(
            meltano_dir=self.config.repo_path,
            config_cache=Path("config/tools/meltano.yaml"),
        )

        validation = adapter.validate()

        if not validation["valid"]:
            error_msg = f"Pre-execution validation failed: {'; '.join(validation['errors'])}"
            self.logger.error(
                error_msg,
                extra={
                    "stage": self.name,
                    "event": "validation_failed",
                    "metadata": {"errors": validation["errors"]},
                },
            )
            raise ValueError(error_msg)

        # Log any warnings
        for warning in validation.get("warnings", []):
            self.logger.warning(
                f"Validation warning: {warning}",
                extra={
                    "stage": self.name,
                    "event": "validation_warning",
                    "metadata": {"warning": warning},
                },
            )

        self.logger.info(
            f"Running Meltano job: {job_name}",
            extra={
                "stage": self.name,
                "event": "meltano_job_started",
                "metadata": {"job": job_name},
            },
        )

        # Build command
        meltano_bin = self.config.venv_path / "bin" / "meltano"
        command = [str(meltano_bin), "run", job_name]

        # Execute with retry if configured
        retry_config = self._get_retry_config()

        if retry_config["enabled"]:
            self._run_meltano_with_retry(command, retry_config)
        else:
            self._run_meltano(command)

        # Collect output files
        output_files = self._get_output_files()

        # Count total records
        total_records = 0
        for output_file in output_files:
            try:
                records = count_jsonl_records(output_file)
                total_records += records
                self.logger.info(
                    f"Extracted {records} records to {output_file.name}",
                    extra={
                        "stage": self.name,
                        "event": "file_written",
                        "metadata": {
                            "file": str(output_file),
                            "records": records,
                        },
                    },
                )
            except Exception as e:
                self.logger.warning(
                    f"Could not count records in {output_file}: {e}"
                )

        # Validate minimum records if configured
        min_records = self.config.validation.get("min_records", 0)
        if min_records > 0 and total_records < min_records:
            return StageResult(
                stage_name=self.name,
                success=False,
                duration_seconds=0,
                error_message=f"Extracted {total_records} records, expected at least {min_records}",
            )

        return StageResult(
            stage_name=self.name,
            success=True,
            duration_seconds=0,  # Will be set by base class
            records_processed=total_records,
            output_files=output_files,
            metadata={"job": job_name, "meltano_repo": str(self.config.repo_path)},
        )

    def _run_meltano(self, command: List[str]) -> None:
        """
        Run Meltano command.

        Args:
            command: Command to execute

        Raises:
            subprocess.CalledProcessError: If command fails
        """
        self.logger.debug(f"Executing: {' '.join(command)}")

        result = subprocess.run(
            command,
            cwd=self.config.repo_path,
            capture_output=True,
            text=True,
            check=False,  # Don't raise, we'll handle errors
        )

        if result.returncode != 0:
            error_msg = f"Meltano command failed with exit code {result.returncode}"
            if result.stderr:
                error_msg += f": {result.stderr[:500]}"

            self.logger.error(
                error_msg,
                extra={
                    "stage": self.name,
                    "event": "meltano_error",
                    "metadata": {
                        "exit_code": result.returncode,
                        "stderr": result.stderr[:1000] if result.stderr else None,
                    },
                },
            )

            raise subprocess.CalledProcessError(
                result.returncode, command, output=result.stdout, stderr=result.stderr
            )

        # Log stdout at debug level (may contain sensitive info)
        if result.stdout:
            self.logger.debug(f"Meltano output: {result.stdout[:500]}")

    def _run_meltano_with_retry(self, command: List[str], retry_config: dict) -> None:
        """
        Run Meltano command with retry logic.

        Args:
            command: Command to execute
            retry_config: Retry configuration
        """
        retry_with_backoff(
            func=lambda: self._run_meltano(command),
            max_attempts=retry_config["max_attempts"],
            backoff_seconds=retry_config["backoff_seconds"],
            backoff_multiplier=retry_config["backoff_multiplier"],
            logger=self.logger,
        )

    def _get_output_files(self) -> List[Path]:
        """
        Get list of output files from extraction.

        Returns:
            List of JSONL files in output directory
        """
        output_dir = self.config.output_dir

        # Find all .jsonl files
        output_files = list(output_dir.glob("*.jsonl"))

        # Sort by modification time (most recent first)
        output_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

        return output_files
