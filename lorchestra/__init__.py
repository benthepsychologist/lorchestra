"""
lorchestra - Lightweight job orchestrator

Discovers and runs jobs from installed packages via entrypoints.
Jobs emit events to BigQuery for tracking and observability.
"""

__version__ = "0.1.0"
__author__ = "Local Pipeline Team"


__all__ = ["LorchestraConfig", "load_config", "get_lorchestra_home"]

from .config import LorchestraConfig, load_config, get_lorchestra_home

