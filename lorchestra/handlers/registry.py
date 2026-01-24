"""
Handler Registry for dispatching operations to appropriate handlers.

The registry maps backend types (data_plane, compute, orchestration) to
their corresponding Handler implementations, providing a central dispatch
mechanism for the Executor.
"""

from typing import Any, TYPE_CHECKING

from lorchestra.handlers.base import Handler, NoOpHandler
from lorchestra.schemas import StepManifest

if TYPE_CHECKING:
    from lorchestra.handlers.storacle_client import StoracleClient
    from lorchestra.handlers.compute import ComputeClient
    from lorchestra.run_store import RunStore


class HandlerRegistry:
    """
    Registry for handler dispatch by backend type.

    Maps backend names (data_plane, compute, orchestration) to Handler instances.

    Usage:
        registry = HandlerRegistry()
        registry.register("data_plane", DataPlaneHandler(storacle_client))
        registry.register("compute", ComputeHandler(compute_client))

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
            backend: Backend type name (data_plane, compute, orchestration)
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
        storacle_client: "StoracleClient | None" = None,
        compute_client: "ComputeClient | None" = None,
        store: "RunStore | None" = None,
    ) -> "HandlerRegistry":
        """
        Create a registry with default handlers.

        If clients are not provided, NoOpHandler will be used.

        Args:
            storacle_client: StoracleClient for data_plane operations
            compute_client: ComputeClient for compute operations
            store: RunStore for orchestration sub-job artifacts

        Returns:
            Configured HandlerRegistry
        """
        registry = cls()

        # Data plane handler
        if storacle_client is not None:
            from lorchestra.handlers.data_plane import DataPlaneHandler

            registry.register("data_plane", DataPlaneHandler(storacle_client))
        else:
            registry.register("data_plane", NoOpHandler())

        # Compute handler
        if compute_client is not None:
            from lorchestra.handlers.compute import ComputeHandler

            registry.register("compute", ComputeHandler(compute_client))
        else:
            registry.register("compute", NoOpHandler())

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

        Returns:
            HandlerRegistry with NoOp handlers for all backends
        """
        registry = cls()
        registry.register("data_plane", NoOpHandler())
        registry.register("compute", NoOpHandler())
        registry.register("orchestration", NoOpHandler())
        return registry
