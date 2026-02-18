"""
Callable dispatch - In-proc dispatch to callables.

This module provides the dispatch layer for the generic `call` operation:
1. Maps callable name string to callable function via explicit registry
2. Invokes callable with params
3. Returns CallableResult

Error handling contract:
- Callables raise TransientError (safe to retry) or PermanentError (fail fast)
- dispatch_callable propagates exceptions unchanged
- Classification happens at the source (callables) or executor boundary
- CallableResult is success-only; errors are exceptions, not values

Note: The CALLABLES registry imports are lazy to avoid import cycles and
allow graceful handling when callable modules are not installed.
"""

from typing import Callable

from lorchestra.callable.result import CallableResult


# Type alias for callable functions
CallableFn = Callable[[dict], dict]


def _try_import_callable(module_name: str) -> CallableFn | None:
    """
    Try to import a callable module and return its execute function.

    Returns None if import fails or module doesn't have execute function.
    """
    try:
        import importlib
        module = importlib.import_module(module_name)
        if hasattr(module, 'execute') and callable(module.execute):
            return module.execute
    except ImportError:
        pass
    return None


def _get_callables() -> dict[str, CallableFn]:
    """
    Build the callable registry with lazy imports.

    This function is called once and cached. It attempts to import
    each callable module and only registers those that are available.

    External callables are top-level packages (injest, canonizer, etc.).
    Internal callables live in lorchestra.callable.* and are registered
    under their short name (e.g., "view_creator").
    """
    # External packages (top-level imports)
    external = [
        "injest",
        "canonizer",
        "finalform",
        "projectionist",
        "workman",
        "inferometer",
    ]
    # Internal callables (lorchestra.callable.*)
    internal = [
        "view_creator",
        "molt_projector",
        "bq_reader",
        "file_renderer",
        "egret_builder",
        "render",
        "inferometer_adapter",
    ]

    callables: dict[str, CallableFn] = {}

    for name in external:
        fn = _try_import_callable(name)
        if fn is not None:
            callables[name] = fn
        else:
            callables[name] = _stub_callable(name)

    for name in internal:
        fn = _try_import_callable(f"lorchestra.callable.{name}")
        if fn is not None:
            callables[name] = fn
        else:
            callables[name] = _stub_callable(name)

    # Aliases: map friendly names to internal callable modules
    # "egret" -> egret_builder (for use in job definitions as callable: egret)
    if "egret_builder" in callables:
        callables["egret"] = callables["egret_builder"]
    
    # "inferometer" can be used from either the external package or internal adapter
    # Prefer internal adapter if available (more flexible for lorchestra integration)
    if "inferometer_adapter" in callables and callables["inferometer_adapter"].__name__ != "stub":
        callables["inferometer"] = callables["inferometer_adapter"]

    return callables


def _stub_callable(name: str) -> CallableFn:
    """Create a stub callable that raises NotImplementedError."""
    def stub(params: dict) -> dict:
        raise NotImplementedError(
            f"Callable '{name}' not installed. "
            f"Install the {name} package or implement {name}.execute(params)."
        )
    return stub


# Lazy-initialized callable registry
_CALLABLES: dict[str, CallableFn] | None = None


def get_callables() -> dict[str, CallableFn]:
    """Get the callable registry, initializing if needed."""
    global _CALLABLES
    if _CALLABLES is None:
        _CALLABLES = _get_callables()
    return _CALLABLES


def dispatch_callable(name: str, params: dict) -> CallableResult:
    """
    Dispatch to in-proc callable by name and return CallableResult.

    Callables raise TransientError or PermanentError on failure.
    This function does NOT classify - it just propagates.
    Classification happens at the source (callables) or executor boundary.

    Args:
        name: The callable module name (e.g., "injest", "workman")
        params: Parameters to pass to the callable

    Returns:
        CallableResult with items or items_ref

    Raises:
        ValueError: If callable name is not registered
        TransientError: Propagated from callable (safe to retry)
        PermanentError: Propagated from callable (do not retry)
        NotImplementedError: If callable is not installed
    """
    callables = get_callables()
    fn = callables.get(name)

    if fn is None:
        raise ValueError(f"Unknown callable: {name}")

    # Invoke callable - exceptions propagate unchanged
    result_dict = fn(params)

    # Wrap in CallableResult
    return CallableResult(**result_dict)


def register_callable(name: str, fn: CallableFn) -> None:
    """
    Register a callable function by name.

    This is primarily for testing - allows injecting mock callables.

    Args:
        name: The callable name (e.g., "injest", "workman")
        fn: Function that takes params dict and returns result dict
    """
    callables = get_callables()
    callables[name] = fn
