"""
JobRegistry - Load and validate JobDefs from storage.

The registry provides:
- Loading JobDefs from YAML or JSON files in a definitions directory
- Version support (optional, default="latest")
- Caching loaded definitions
- Validation of JobDef structure
- Content-addressable lookup via SHA256 hash
"""

import hashlib
import json
from pathlib import Path
from typing import Optional

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

from lorchestra.schemas import JobDef


class JobNotFoundError(Exception):
    """Raised when a job definition is not found."""
    pass


class JobValidationError(Exception):
    """Raised when a job definition fails validation."""
    pass


class JobRegistry:
    """
    Registry for loading and caching JobDefs.

    Loads job definitions from JSON files organized in a directory tree.
    Supports both flat and nested directory structures.

    Example directory structure:
        definitions/
            ingest/
                ingest_gmail.json
            canonize/
                canonize_gmail.json
            pipeline/
                daily_pipeline.json
    """

    def __init__(self, definitions_dir: Path | str):
        """
        Initialize the registry.

        Args:
            definitions_dir: Path to directory containing job definition JSON files
        """
        self._definitions_dir = Path(definitions_dir)
        self._cache: dict[str, JobDef] = {}
        self._hash_index: dict[str, str] = {}  # sha256 -> job_id

    @property
    def definitions_dir(self) -> Path:
        """Get the definitions directory path."""
        return self._definitions_dir

    def load(self, job_id: str, version: Optional[str] = None) -> JobDef:
        """
        Load a JobDef by ID and optional version.

        Searches for {job_id}.yaml or {job_id}.json in the definitions directory tree.
        YAML files are preferred over JSON when both exist.
        Results are cached for subsequent calls.

        Args:
            job_id: The job identifier (filename without extension)
            version: Optional version string. If provided and not "latest",
                     validates that the loaded JobDef has this version.

        Returns:
            The loaded JobDef

        Raises:
            JobNotFoundError: If the job definition file doesn't exist
                             or if version doesn't match
            JobValidationError: If the job definition is invalid
        """
        # Check cache first (only for unversioned or "latest" requests)
        if version is None or version == "latest":
            if job_id in self._cache:
                return self._cache[job_id]

        # Find the definition file
        def_path = self._find_definition(job_id)
        if def_path is None:
            raise JobNotFoundError(f"Job definition not found: {job_id}")

        # Load and parse based on file extension
        try:
            data = self._load_file(def_path)
        except Exception as e:
            raise JobValidationError(f"Failed to load {def_path}: {e}")

        # Resolve .yaml/.yml file paths in top-level extra fields
        known_keys = {"job_id", "version", "steps"}
        for key, value in data.items():
            if key not in known_keys and isinstance(value, str) and (
                value.endswith(".yaml") or value.endswith(".yml")
            ):
                yaml_path = self._definitions_dir / value
                try:
                    data[key] = self._load_file(yaml_path)
                except Exception as e:
                    raise JobValidationError(
                        f"Failed to load config file '{value}' for key '{key}': {e}"
                    )

        # Validate and create JobDef
        try:
            job_def = JobDef.from_dict(data)
        except Exception as e:
            raise JobValidationError(f"Invalid JobDef in {def_path}: {e}")

        # Verify job_id matches
        if job_def.job_id != job_id:
            raise JobValidationError(
                f"Job ID mismatch: file is '{job_id}' but job_id is '{job_def.job_id}'"
            )

        # Version validation
        if version is not None and version != "latest":
            if job_def.version != version:
                raise JobNotFoundError(
                    f"Version mismatch for {job_id}: requested '{version}', found '{job_def.version}'"
                )

        # Cache and index
        self._cache[job_id] = job_def
        sha256 = self.compute_hash(job_def)
        self._hash_index[sha256] = job_id

        return job_def

    def _load_file(self, path: Path) -> dict:
        """
        Load a definition file (YAML or JSON).

        Args:
            path: Path to the file

        Returns:
            Parsed dictionary

        Raises:
            ValueError: If file format is unsupported or parsing fails
        """
        suffix = path.suffix.lower()

        with open(path) as f:
            if suffix in (".yaml", ".yml"):
                if not HAS_YAML:
                    raise ValueError(
                        f"Cannot load YAML file {path}: PyYAML not installed. "
                        "Install with: pip install pyyaml"
                    )
                return yaml.safe_load(f)
            elif suffix == ".json":
                return json.load(f)
            else:
                raise ValueError(f"Unsupported file format: {suffix}")

    def load_by_hash(self, sha256: str) -> Optional[JobDef]:
        """
        Load a JobDef by its content hash.

        Args:
            sha256: The SHA256 hash of the JobDef

        Returns:
            The JobDef if found in cache, None otherwise
        """
        job_id = self._hash_index.get(sha256)
        if job_id is None:
            return None
        return self._cache.get(job_id)

    def list_jobs(self) -> list[str]:
        """
        List all available job IDs.

        Returns:
            Sorted list of job IDs found in the definitions directory
        """
        if not self._definitions_dir.exists():
            return []

        job_ids = set()
        for ext in ["*.yaml", "*.yml", "*.json"]:
            for f in self._definitions_dir.glob(f"**/{ext}"):
                if "_deprecated" not in str(f):
                    job_ids.add(f.stem)

        return sorted(job_ids)

    def _find_definition(self, job_id: str) -> Optional[Path]:
        """
        Find the definition file for a job ID.

        Searches the definitions directory recursively.
        YAML files are preferred over JSON.

        Args:
            job_id: The job identifier

        Returns:
            Path to the definition file, or None if not found
        """
        # Try extensions in order of preference: YAML first, then JSON
        for ext in [".yaml", ".yml", ".json"]:
            filename = f"{job_id}{ext}"

            # Check root directory first
            root_path = self._definitions_dir / filename
            if root_path.exists():
                return root_path

            # Search subdirectories
            matches = list(self._definitions_dir.glob(f"**/{filename}"))
            if matches:
                return matches[0]

        return None

    @staticmethod
    def compute_hash(job_def: JobDef) -> str:
        """
        Compute SHA256 hash of a JobDef for content addressing.

        Uses canonical JSON serialization (sorted keys, no whitespace)
        to ensure consistent hashing.

        Args:
            job_def: The job definition to hash

        Returns:
            Hexadecimal SHA256 hash string
        """
        # Canonical JSON: sorted keys, compact encoding
        canonical = json.dumps(job_def.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()

    def clear_cache(self) -> None:
        """Clear the definition cache."""
        self._cache.clear()
        self._hash_index.clear()

    def preload_all(self) -> int:
        """
        Preload all job definitions into cache.

        Useful for startup validation or when you need fast access
        to all definitions.

        Returns:
            Number of jobs loaded

        Raises:
            JobValidationError: If any job definition is invalid
        """
        count = 0
        for job_id in self.list_jobs():
            self.load(job_id)
            count += 1
        return count
