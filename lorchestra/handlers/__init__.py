"""
Handlers module for lorchestra execution backends.

This module provides the handler abstraction layer that enforces clean boundaries:
- lorchestra orchestrates (JobDef -> JobInstance -> RunRecord -> StepManifest)
- handlers dispatch to appropriate backends for non-native ops

Native ops (call, plan.build, storacle.submit) are handled directly by the
Executor. Handlers are used for:
- inferometer: compute.* ops dispatched to LLM service
- orchestration: job.* ops handled by lorchestra itself

Usage:
    from lorchestra.handlers import HandlerRegistry, ComputeHandler

    # Create registry with configured handlers
    registry = HandlerRegistry()
    registry.register("inferometer", ComputeHandler(compute_client))

    # Or use factory with defaults
    registry = HandlerRegistry.create_default()
"""

from lorchestra.handlers.base import Handler, NoOpHandler
from lorchestra.handlers.registry import HandlerRegistry
from lorchestra.handlers.compute import ComputeHandler, ComputeClient
from lorchestra.handlers.orchestration import OrchestrationHandler

__all__ = [
    "Handler",
    "NoOpHandler",
    "HandlerRegistry",
    "ComputeHandler",
    "ComputeClient",
    "OrchestrationHandler",
]
