"""
Handler Registry for dispatching operations to appropriate handlers.

The registry maps backend types to their corresponding Handler implementations,
providing a central dispatch mechanism for the Executor.

Native ops (call, plan.build, storacle.submit) are handled directly by the
Executor and do NOT go through the registry. The registry handles:
- inferometer: compute.* ops dispatched to LLM service
- orchestration: job.* ops handled by lorchestra itself
"""

from typing import Any, TYPE_CHECKING

from lorchestra.handlers.base import Handler, NoOpHandler
from lorchestra.schemas import StepManifest

if TYPE_CHECKING:
    from lorchestra.handlers.compute import ComputeClient
    from lorchestra.run_store import RunStore


class HandlerRegistry:
    """
    Registry for handler dispatch by backend type.

    Maps backend names (inferometer, orchestration) to Handler instances.
    Native ops (call, plan.build, storacle.submit) are handled directly
    by the Executor and do not go through the registry.

    Usage:
        registry = HandlerRegistry()
        registry.register("inferometer", ComputeHandler(compute_client))

        # Dispatch a manifest
        result = registry.dispatch(manifest)

        # Or use factory with defaults
        registry = HandlerRegistry.create_default()
    """

    def __init__(self) -> None:
        """Initialize an empty handler registry."""
        self._handlers: dict[str, Handler] = {}

    def register(self, backend: str, handler: Handler) -> None:
        """
        Register a handler for a backend type.

        Args:
            backend: Backend type name (e.g. inferometer, orchestration)
            handler: Handler instance for this backend
        """
        self._handlers[backend] = handler

    def get(self, backend: str) -> Handler:
        """
        Get handler for a backend type.

        Args:
            backend: Backend type name

        Returns:
            Handler instance for this backend

        Raises:
            KeyError: If no handler registered for this backend
        """
        if backend not in self._handlers:
            registered = list(self._handlers.keys())
            raise KeyError(
                f"No handler registered for backend: {backend}. "
                f"Registered: {registered}"
            )
        return self._handlers[backend]

    def has(self, backend: str) -> bool:
        """
        Check if a handler is registered for a backend.

        Args:
            backend: Backend type name

        Returns:
            True if handler is registered
        """
        return backend in self._handlers

    def list_backends(self) -> list[str]:
        """
        List all registered backend names.

        Returns:
            List of registered backend names
        """
        return list(self._handlers.keys())

    def dispatch(self, manifest: StepManifest) -> dict[str, Any]:
        """
        Dispatch a manifest to the appropriate handler.

        Args:
            manifest: The StepManifest to execute

        Returns:
            The execution result from the handler

        Raises:
            KeyError: If no handler registered for the manifest's backend
        """
        backend = manifest.backend
        handler = self.get(backend)
        return handler.execute(manifest)

    @classmethod
    def create_default(
        cls,
        compute_client: "ComputeClient | None" = None,
        store: "RunStore | None" = None,
    ) -> "HandlerRegistry":
        """
        Create a registry with default handlers.

        Native ops (call, plan.build, storacle.submit) are handled by the
        Executor directly and are NOT registered here.

        If clients are not provided, NoOpHandler will be used.

        Args:
            compute_client: ComputeClient for inferometer operations
            store: RunStore for orchestration sub-job artifacts

        Returns:
            Configured HandlerRegistry
        """
        registry = cls()

        # Inferometer handler (LLM service)
        if compute_client is not None:
            from lorchestra.handlers.compute import ComputeHandler
            registry.register("inferometer", ComputeHandler(compute_client))
        else:
            registry.register("inferometer", NoOpHandler())

        # Orchestration handler
        from lorchestra.handlers.orchestration import OrchestrationHandler
        registry.register(
            "orchestration",
            OrchestrationHandler(registry=registry, store=store),
        )

        return registry

    @classmethod
    def create_noop(cls) -> "HandlerRegistry":
        """
        Create a registry with all NoOp handlers.

        Useful for testing and dry-run mode.
        Native ops are handled by the Executor, not the registry.

        Returns:
            HandlerRegistry with NoOp handlers for non-native backends
        """
        registry = cls()
        registry.register("inferometer", NoOpHandler())
        registry.register("orchestration", NoOpHandler())
        return registry
