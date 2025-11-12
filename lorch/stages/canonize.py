"""
Canonize stage: Transform source data to canonical format.

Uses canonizer CLI to apply JSONata transforms.
"""

import json
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple

from lorch.config import StageConfig
from lorch.stages.base import Stage, StageResult
from lorch.utils import count_jsonl_records, validate_file_is_jsonl


class CanonizeStage(Stage):
    """
    Canonize stage using canonizer.

    Transforms source JSONL files to canonical format using JSONata transforms.
    """

    def validate(self) -> None:
        """Validate canonizer installation and transform registry."""
        # Check canonizer repo exists
        if not self.config.repo_path.exists():
            raise FileNotFoundError(
                f"Canonizer repo not found: {self.config.repo_path}"
            )

        # Check venv exists
        if not self.config.venv_path.exists():
            raise FileNotFoundError(
                f"Canonizer venv not found: {self.config.venv_path}"
            )

        # Check canonizer executable
        can_bin = self.config.venv_path / "bin" / "can"
        if not can_bin.exists():
            raise FileNotFoundError(
                f"Canonizer executable not found: {can_bin}"
            )

        # Check transform registry exists
        transform_registry = Path(self.config.get("transform_registry"))
        if not transform_registry.exists():
            raise FileNotFoundError(
                f"Transform registry not found: {transform_registry}"
            )

        # Validate input files exist
        self._validate_input_files(patterns=["*.jsonl"])

        # Validate output directory
        self._validate_output_dir()

        # Validate mappings are configured
        mappings = self.config.get("mappings", [])
        if not mappings:
            raise ValueError("No transform mappings configured")

    def execute(self) -> StageResult:
        """Execute canonization transforms."""
        mappings = self.config.get("mappings", [])
        input_dir = self.config.input_dir
        output_dir = self.config.output_dir
        transform_registry = Path(self.config.get("transform_registry"))

        self.logger.info(
            f"Starting canonization with {len(mappings)} mappings",
            extra={
                "stage": self.name,
                "event": "canonize_started",
                "metadata": {"mappings_count": len(mappings)},
            },
        )

        total_records = 0
        output_files = []
        errors = []

        # Process each mapping
        for mapping in mappings:
            source_pattern = mapping["source_pattern"]
            transform_name = mapping["transform"]
            output_name = mapping.get("output_name", "canonical")

            # Find matching input files
            input_files = list(input_dir.glob(source_pattern))

            if not input_files:
                self.logger.warning(
                    f"No files match pattern: {source_pattern}",
                    extra={
                        "stage": self.name,
                        "event": "no_files_matched",
                        "metadata": {"pattern": source_pattern},
                    },
                )
                continue

            self.logger.info(
                f"Processing {len(input_files)} files with transform: {transform_name}",
                extra={
                    "stage": self.name,
                    "event": "transform_started",
                    "metadata": {
                        "transform": transform_name,
                        "file_count": len(input_files),
                    },
                },
            )

            # Process each input file
            for input_file in input_files:
                try:
                    records_processed = self._transform_file(
                        input_file=input_file,
                        transform_name=transform_name,
                        transform_registry=transform_registry,
                        output_dir=output_dir,
                        output_name=output_name,
                    )

                    total_records += records_processed
                    self.logger.info(
                        f"Transformed {records_processed} records from {input_file.name}",
                        extra={
                            "stage": self.name,
                            "event": "file_transformed",
                            "metadata": {
                                "input_file": str(input_file),
                                "records": records_processed,
                            },
                        },
                    )

                except Exception as e:
                    error_msg = f"Failed to transform {input_file.name}: {e}"
                    errors.append(error_msg)
                    self.logger.error(
                        error_msg,
                        extra={
                            "stage": self.name,
                            "event": "transform_error",
                            "metadata": {"input_file": str(input_file), "error": str(e)},
                        },
                    )

        # Collect output files
        output_files = list(output_dir.glob("*.jsonl"))

        # Check if we had errors
        if errors and not output_files:
            # All transforms failed
            return StageResult(
                stage_name=self.name,
                success=False,
                duration_seconds=0,
                error_message=f"{len(errors)} transform(s) failed: {errors[0]}",
            )

        return StageResult(
            stage_name=self.name,
            success=True,
            duration_seconds=0,  # Will be set by base class
            records_processed=total_records,
            output_files=output_files,
            metadata={
                "transform_registry": str(transform_registry),
                "mappings_applied": len(mappings),
                "errors": len(errors),
            },
        )

    def _transform_file(
        self,
        input_file: Path,
        transform_name: str,
        transform_registry: Path,
        output_dir: Path,
        output_name: str,
    ) -> int:
        """
        Transform a single JSONL file.

        Args:
            input_file: Input JSONL file
            transform_name: Transform name (e.g., "email/gmail_to_canonical_v1")
            transform_registry: Transform registry directory
            output_dir: Output directory
            output_name: Output file base name

        Returns:
            Number of records processed

        Raises:
            Exception: If transformation fails
        """
        # Build transform metadata path
        transform_meta = transform_registry / f"{transform_name}.meta.yaml"

        if not transform_meta.exists():
            raise FileNotFoundError(f"Transform metadata not found: {transform_meta}")

        # Build output file path
        from datetime import datetime
        date_str = datetime.now().strftime("%Y%m%d")
        output_file = output_dir / f"{output_name}-{date_str}.jsonl"

        # Build command
        can_bin = self.config.venv_path / "bin" / "can"
        command = [
            str(can_bin),
            "transform",
            "run",
            "--meta",
            str(transform_meta),
            "--input",
            str(input_file),
        ]

        self.logger.debug(f"Executing: {' '.join(command)}")

        # Execute canonizer
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            error_msg = f"Canonizer failed with exit code {result.returncode}"
            if result.stderr:
                error_msg += f": {result.stderr[:500]}"
            raise RuntimeError(error_msg)

        # Write output to file (append mode for multiple inputs)
        with open(output_file, "a") as f:
            f.write(result.stdout)

        # Count records in output
        records = result.stdout.count("\n")

        # Validate output is valid JSONL
        if not validate_file_is_jsonl(output_file):
            self.logger.warning(
                f"Output file {output_file} contains invalid JSONL",
                extra={
                    "stage": self.name,
                    "event": "invalid_jsonl",
                    "metadata": {"file": str(output_file)},
                },
            )

        return records

    def cleanup(self) -> None:
        """Clean up temporary files if needed."""
        # No cleanup needed for canonize stage
        pass
