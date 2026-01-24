"""
RunStore - Persist execution artifacts for runs.

The RunStore manages:
- RunRecords (created when execution starts)
- StepManifests (created for each step execution)
- AttemptRecords (tracking execution attempts and outcomes)
- Step outputs (results from backend execution)

Storage backends:
- In-memory (for testing)
- File-based (for development)
- Future: BigQuery/GCS for production
"""

import json
import os
import time
import random
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from lorchestra.schemas import (
    RunRecord,
    StepManifest,
    AttemptRecord,
    StepOutcome,
    StepStatus,
    JobInstance,
)


def generate_ulid() -> str:
    """
    Generate a ULID (Universally Unique Lexicographically Sortable Identifier).

    ULIDs are 26 characters, encoding:
    - 48 bits of timestamp (milliseconds since Unix epoch)
    - 80 bits of randomness

    This is a simplified implementation for portability.
    For production, consider using the `ulid` package.
    """
    # Crockford's Base32 alphabet (excludes I, L, O, U)
    ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

    # Timestamp component (48 bits = 10 chars in base32)
    timestamp_ms = int(time.time() * 1000)
    timestamp_chars = []
    for _ in range(10):
        timestamp_chars.append(ALPHABET[timestamp_ms & 0x1F])
        timestamp_ms >>= 5
    timestamp_part = "".join(reversed(timestamp_chars))

    # Random component (80 bits = 16 chars in base32)
    random_part = "".join(random.choice(ALPHABET) for _ in range(16))

    return timestamp_part + random_part


class RunStore(ABC):
    """
    Abstract base class for run artifact storage.

    Implementations must provide methods to:
    - Create and retrieve RunRecords
    - Store and retrieve StepManifests
    - Store and retrieve AttemptRecords
    - Store and retrieve step outputs
    """

    @abstractmethod
    def create_run(self, instance: JobInstance, envelope: dict[str, Any]) -> RunRecord:
        """
        Create a new run record.

        Args:
            instance: The JobInstance being executed
            envelope: Runtime context/payload for the run

        Returns:
            The created RunRecord with a new ULID
        """
        pass

    @abstractmethod
    def get_run(self, run_id: str) -> Optional[RunRecord]:
        """
        Retrieve a run record by ID.

        Args:
            run_id: The ULID of the run

        Returns:
            The RunRecord if found, None otherwise
        """
        pass

    @abstractmethod
    def store_manifest(self, manifest: StepManifest) -> str:
        """
        Store a step manifest.

        Args:
            manifest: The StepManifest to store

        Returns:
            A reference string for retrieving the manifest
        """
        pass

    @abstractmethod
    def get_manifest(self, manifest_ref: str) -> Optional[StepManifest]:
        """
        Retrieve a step manifest by reference.

        Args:
            manifest_ref: The manifest reference string

        Returns:
            The StepManifest if found, None otherwise
        """
        pass

    @abstractmethod
    def store_output(self, run_id: str, step_id: str, output: Any) -> str:
        """
        Store step output.

        Args:
            run_id: The run ULID
            step_id: The step identifier
            output: The output data to store

        Returns:
            A reference string for retrieving the output
        """
        pass

    @abstractmethod
    def get_output(self, output_ref: str) -> Optional[Any]:
        """
        Retrieve step output by reference.

        Args:
            output_ref: The output reference string

        Returns:
            The output data if found, None otherwise
        """
        pass

    @abstractmethod
    def store_attempt(self, attempt: AttemptRecord) -> None:
        """
        Store or update an attempt record.

        Args:
            attempt: The AttemptRecord to store
        """
        pass

    @abstractmethod
    def get_attempt(self, run_id: str, attempt_n: int) -> Optional[AttemptRecord]:
        """
        Retrieve an attempt record.

        Args:
            run_id: The run ULID
            attempt_n: The attempt number (1-indexed)

        Returns:
            The AttemptRecord if found, None otherwise
        """
        pass

    @abstractmethod
    def get_latest_attempt(self, run_id: str) -> Optional[AttemptRecord]:
        """
        Get the latest attempt for a run.

        Args:
            run_id: The run ULID

        Returns:
            The most recent AttemptRecord, or None if no attempts exist
        """
        pass


class InMemoryRunStore(RunStore):
    """
    In-memory implementation of RunStore for testing.

    All data is lost when the instance is garbage collected.
    """

    def __init__(self):
        self._runs: dict[str, RunRecord] = {}
        self._manifests: dict[str, StepManifest] = {}
        self._outputs: dict[str, Any] = {}
        self._attempts: dict[str, dict[int, AttemptRecord]] = {}  # run_id -> attempt_n -> record

    def create_run(self, instance: JobInstance, envelope: dict[str, Any]) -> RunRecord:
        run_id = generate_ulid()
        run = RunRecord(
            run_id=run_id,
            job_id=instance.job_id,
            job_def_sha256=instance.job_def_sha256,
            envelope=envelope,
            started_at=datetime.now(timezone.utc),
        )
        self._runs[run_id] = run
        self._attempts[run_id] = {}
        return run

    def get_run(self, run_id: str) -> Optional[RunRecord]:
        return self._runs.get(run_id)

    def store_manifest(self, manifest: StepManifest) -> str:
        ref = f"mem://{manifest.run_id}/{manifest.step_id}"
        self._manifests[ref] = manifest
        return ref

    def get_manifest(self, manifest_ref: str) -> Optional[StepManifest]:
        return self._manifests.get(manifest_ref)

    def store_output(self, run_id: str, step_id: str, output: Any) -> str:
        ref = f"mem://{run_id}/{step_id}/output"
        self._outputs[ref] = output
        return ref

    def get_output(self, output_ref: str) -> Optional[Any]:
        return self._outputs.get(output_ref)

    def store_attempt(self, attempt: AttemptRecord) -> None:
        if attempt.run_id not in self._attempts:
            self._attempts[attempt.run_id] = {}
        self._attempts[attempt.run_id][attempt.attempt_n] = attempt

    def get_attempt(self, run_id: str, attempt_n: int) -> Optional[AttemptRecord]:
        run_attempts = self._attempts.get(run_id, {})
        return run_attempts.get(attempt_n)

    def get_latest_attempt(self, run_id: str) -> Optional[AttemptRecord]:
        run_attempts = self._attempts.get(run_id, {})
        if not run_attempts:
            return None
        max_n = max(run_attempts.keys())
        return run_attempts[max_n]

    def clear(self) -> None:
        """Clear all stored data (for testing)."""
        self._runs.clear()
        self._manifests.clear()
        self._outputs.clear()
        self._attempts.clear()


class FileRunStore(RunStore):
    """
    File-based implementation of RunStore for development.

    Stores artifacts as JSON files in a directory tree:
        store_dir/
            runs/
                {run_id}.json
            manifests/
                {run_id}/
                    {step_id}.json
            outputs/
                {run_id}/
                    {step_id}.json
            attempts/
                {run_id}/
                    {attempt_n}.json
    """

    def __init__(self, store_dir: Path | str):
        self._store_dir = Path(store_dir)
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        """Create the directory structure if needed."""
        for subdir in ["runs", "manifests", "outputs", "attempts"]:
            (self._store_dir / subdir).mkdir(parents=True, exist_ok=True)

    def create_run(self, instance: JobInstance, envelope: dict[str, Any]) -> RunRecord:
        run_id = generate_ulid()
        run = RunRecord(
            run_id=run_id,
            job_id=instance.job_id,
            job_def_sha256=instance.job_def_sha256,
            envelope=envelope,
            started_at=datetime.now(timezone.utc),
        )

        # Store the run record
        run_path = self._store_dir / "runs" / f"{run_id}.json"
        with open(run_path, "w") as f:
            json.dump(run.to_dict(), f, indent=2)

        # Create subdirectories for this run
        for subdir in ["manifests", "outputs", "attempts"]:
            (self._store_dir / subdir / run_id).mkdir(parents=True, exist_ok=True)

        return run

    def get_run(self, run_id: str) -> Optional[RunRecord]:
        run_path = self._store_dir / "runs" / f"{run_id}.json"
        if not run_path.exists():
            return None
        with open(run_path) as f:
            data = json.load(f)
        return RunRecord.from_dict(data)

    def store_manifest(self, manifest: StepManifest) -> str:
        manifest_dir = self._store_dir / "manifests" / manifest.run_id
        manifest_dir.mkdir(parents=True, exist_ok=True)

        manifest_path = manifest_dir / f"{manifest.step_id}.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest.to_dict(), f, indent=2)

        return f"file://{manifest_path}"

    def get_manifest(self, manifest_ref: str) -> Optional[StepManifest]:
        if manifest_ref.startswith("file://"):
            path = Path(manifest_ref[7:])
        else:
            return None

        if not path.exists():
            return None

        with open(path) as f:
            data = json.load(f)
        return StepManifest.from_dict(data)

    def store_output(self, run_id: str, step_id: str, output: Any) -> str:
        output_dir = self._store_dir / "outputs" / run_id
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / f"{step_id}.json"
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)

        return f"file://{output_path}"

    def get_output(self, output_ref: str) -> Optional[Any]:
        if output_ref.startswith("file://"):
            path = Path(output_ref[7:])
        else:
            return None

        if not path.exists():
            return None

        with open(path) as f:
            return json.load(f)

    def store_attempt(self, attempt: AttemptRecord) -> None:
        attempt_dir = self._store_dir / "attempts" / attempt.run_id
        attempt_dir.mkdir(parents=True, exist_ok=True)

        attempt_path = attempt_dir / f"{attempt.attempt_n}.json"
        with open(attempt_path, "w") as f:
            json.dump(attempt.to_dict(), f, indent=2)

    def get_attempt(self, run_id: str, attempt_n: int) -> Optional[AttemptRecord]:
        attempt_path = self._store_dir / "attempts" / run_id / f"{attempt_n}.json"
        if not attempt_path.exists():
            return None
        with open(attempt_path) as f:
            data = json.load(f)
        return AttemptRecord.from_dict(data)

    def get_latest_attempt(self, run_id: str) -> Optional[AttemptRecord]:
        attempt_dir = self._store_dir / "attempts" / run_id
        if not attempt_dir.exists():
            return None

        attempt_files = list(attempt_dir.glob("*.json"))
        if not attempt_files:
            return None

        # Parse attempt numbers from filenames
        max_n = max(int(f.stem) for f in attempt_files)
        return self.get_attempt(run_id, max_n)
