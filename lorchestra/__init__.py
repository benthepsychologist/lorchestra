"""
lorchestra - Local Orchestrator for PHI Data Pipeline

Coordinates the three-stage local data pipeline:
1. Extract (Meltano) - Pull data from sources
2. Canonize (Canonizer) - Transform to canonical format
3. Index (Vector-projector) - Store in local document store

Security: Handles PHI data with appropriate permissions and logging.
"""

__version__ = "0.1.0"
__author__ = "Local Pipeline Team"

from .pipeline import Pipeline, PipelineResult

__all__ = ["Pipeline", "PipelineResult"]
