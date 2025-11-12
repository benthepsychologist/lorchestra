"""
Index stage: Store canonical data in local document store.

STUB IMPLEMENTATION: Currently just copies files to destination.
Future: Will call vector-projector to index in SQLite + inode storage.
"""

import shutil
from datetime import datetime
from pathlib import Path
from typing import List

from lorch.config import StageConfig
from lorch.stages.base import Stage, StageResult
from lorch.utils import count_jsonl_records


class IndexStage(Stage):
    """
    Index stage using vector-projector (stub).

    Current: Copies canonical JSON files to destination directory.
    Future: Will index in SQLite with inode-style file storage.
    """

    def validate(self) -> None:
        """Validate index stage prerequisites."""
        # Validate output directory
        self._validate_output_dir()

        # Check mode
        mode = self.config.get("mode", "stub")
        if mode != "stub":
            self.logger.warning(
                f"Index mode '{mode}' not supported yet, using 'stub'",
                extra={
                    "stage": self.name,
                    "event": "unsupported_mode",
                    "metadata": {"mode": mode},
                },
            )

        # Note: We don't validate input files here since they may not exist yet
        # (they're created by the canonize stage)

    def execute(self) -> StageResult:
        """Execute index stub (copy files)."""
        input_dir = self.config.input_dir
        output_dir = self.config.output_dir

        self.logger.info(
            "Starting index stage (stub mode - copying files)",
            extra={
                "stage": self.name,
                "event": "index_started",
                "metadata": {"mode": "stub"},
            },
        )

        # Get input files
        input_files = list(input_dir.glob("*.jsonl"))

        if not input_files:
            self.logger.warning(
                "No input files found for indexing",
                extra={
                    "stage": self.name,
                    "event": "no_input_files",
                },
            )
            return StageResult(
                stage_name=self.name,
                success=True,
                duration_seconds=0,
                records_processed=0,
                metadata={"mode": "stub", "files_processed": 0},
            )

        # Copy each file to output directory
        total_records = 0
        output_files = []

        for input_file in input_files:
            try:
                # Generate output filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                output_file = output_dir / f"{input_file.stem}-{timestamp}{input_file.suffix}"

                # Copy file
                shutil.copy2(input_file, output_file)
                output_files.append(output_file)

                # Count records
                records = count_jsonl_records(output_file)
                total_records += records

                self.logger.info(
                    f"Indexed {records} records from {input_file.name}",
                    extra={
                        "stage": self.name,
                        "event": "file_indexed",
                        "metadata": {
                            "input_file": str(input_file),
                            "output_file": str(output_file),
                            "records": records,
                        },
                    },
                )

            except Exception as e:
                self.logger.error(
                    f"Failed to index {input_file.name}: {e}",
                    extra={
                        "stage": self.name,
                        "event": "index_error",
                        "metadata": {"input_file": str(input_file), "error": str(e)},
                    },
                )

        # Create manifest file
        manifest_file = output_dir / "manifest.json"
        self._create_manifest(manifest_file, output_files, total_records)

        self.logger.info(
            f"Index stage completed: {len(output_files)} files, {total_records} records",
            extra={
                "stage": self.name,
                "event": "index_completed",
                "metadata": {
                    "files": len(output_files),
                    "records": total_records,
                },
            },
        )

        return StageResult(
            stage_name=self.name,
            success=True,
            duration_seconds=0,  # Will be set by base class
            records_processed=total_records,
            output_files=output_files,
            metadata={
                "mode": "stub",
                "files_processed": len(output_files),
                "manifest": str(manifest_file),
            },
        )

    def _create_manifest(
        self, manifest_file: Path, output_files: List[Path], total_records: int
    ) -> None:
        """
        Create manifest file with indexing metadata.

        Args:
            manifest_file: Path to manifest file
            output_files: List of indexed files
            total_records: Total record count
        """
        import json

        manifest = {
            "created_at": datetime.utcnow().isoformat() + "Z",
            "stage": self.name,
            "mode": "stub",
            "total_records": total_records,
            "files": [
                {
                    "path": str(f.relative_to(self.config.output_dir)),
                    "size_bytes": f.stat().st_size,
                    "records": count_jsonl_records(f),
                }
                for f in output_files
            ],
        }

        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)

        self.logger.debug(
            f"Created manifest: {manifest_file}",
            extra={
                "stage": self.name,
                "event": "manifest_created",
                "metadata": {"manifest": str(manifest_file)},
            },
        )

    def cleanup(self) -> None:
        """Clean up temporary files if needed."""
        # No cleanup needed for stub mode
        pass
