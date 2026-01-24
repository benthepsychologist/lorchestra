"""
Handlers module for lorchestra execution backends.

This module provides the handler abstraction layer that enforces clean boundaries:
- lorchestra orchestrates (JobDef → JobInstance → RunRecord → StepManifest)
- handlers dispatch to appropriate backends
- storacle handles data-plane operations (query.*, write.*, assert.*)
- compute handles external IO (compute.llm, compute.transform, compute.extract, compute.render)

Usage:
    from lorchestra.handlers import HandlerRegistry, DataPlaneHandler, ComputeHandler

    # Create registry with configured handlers
    registry = HandlerRegistry()
    registry.register("data_plane", DataPlaneHandler(storacle_client))
    registry.register("compute", ComputeHandler(compute_client))

    # Or use factory with defaults
    registry = HandlerRegistry.create_default()
"""

from lorchestra.handlers.base import Handler, NoOpHandler
from lorchestra.handlers.registry import HandlerRegistry
from lorchestra.handlers.data_plane import DataPlaneHandler
from lorchestra.handlers.compute import ComputeHandler, ComputeClient
from lorchestra.handlers.orchestration import OrchestrationHandler
from lorchestra.handlers.storacle_client import StoracleClient, NoOpStoracleClient

__all__ = [
    "Handler",
    "NoOpHandler",
    "HandlerRegistry",
    "DataPlaneHandler",
    "ComputeHandler",
    "ComputeClient",
    "OrchestrationHandler",
    "StoracleClient",
    "NoOpStoracleClient",
]
