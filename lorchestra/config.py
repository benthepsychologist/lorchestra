import os
import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv(*_args, **_kwargs) -> bool:
        return False

@dataclass
class LorchestraConfig:
    project: str
    dataset_raw: str
    dataset_canonical: str
    dataset_derived: str
    sqlite_path: str
    local_views_root: str

    dataset_wal: str = "wal"

    canonizer_registry_root: Optional[str] = None
    formation_registry_root: Optional[str] = None
    env_file: Optional[str] = None
    google_application_credentials: Optional[str] = None

def get_lorchestra_home() -> Path:
    home = os.environ.get("LORCHESTRA_HOME", "~/.config/lorchestra")
    return Path(home).expanduser()

def get_config_path() -> Path:
    return get_lorchestra_home() / "config.yaml"

def load_config() -> LorchestraConfig:
    path = get_config_path()
    if not path.exists():
        raise FileNotFoundError(
            f"lorchestra config.yaml not found at {path}. "
            "Run `lorchestra init` to create one."
        )

    try:
        data = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse config file at {path}: {e}")

    # For dataclass, we need to ensure required fields.
    # Simple filtering or just passing strict.
    # If using pydantic, validation is free. With dataclasses, we do manual check or just kwargs.
    # Let's trust yaml matches fields for now, but handle potential mismatch.
    try:
        cfg = LorchestraConfig(**data)
    except TypeError as e:
        # Provide a more helpful error if missing fields
        raise ValueError(f"Invalid configuration in {path}: {e}")

    # Optional: if cfg.env_file, load dotenv
    if cfg.env_file:
        env_path = Path(cfg.env_file).expanduser()
        if env_path.exists():
            # Override=False ensures we don't clobber real env vars if user set them
            load_dotenv(env_path, override=False)

    # Set Google Credentials if provided and not already set in env
    if cfg.google_application_credentials and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        cred_path = Path(cfg.google_application_credentials).expanduser()
        if cred_path.exists():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(cred_path)

    return cfg
