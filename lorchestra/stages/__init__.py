"""
Pipeline stages for lorchestra orchestrator.

Each stage is responsible for one part of the pipeline:
- extract: Run Meltano jobs to pull data from sources
- canonize: Transform source data to canonical format
- index: Store canonical data in local document store
"""

from .base import Stage, StageResult

__all__ = ["Stage", "StageResult"]
