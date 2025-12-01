"""Lorchestra processors package.

Provides typed processors for job execution:
- IngestProcessor: Wraps injest library for data ingestion
- CanonizeProcessor: Wraps canonizer library for validation/transformation
- FinalFormProcessor: Wraps final-form library for clinical instruments

All processors implement the Processor protocol and are registered
with the global registry for dispatch by job_type.
"""

from lorchestra.processors.base import (
    EventClient,
    JobContext,
    Processor,
    ProcessorRegistry,
    StorageClient,
    UpsertResult,
    registry,
)

__all__ = [
    "EventClient",
    "JobContext",
    "Processor",
    "ProcessorRegistry",
    "StorageClient",
    "UpsertResult",
    "registry",
]
