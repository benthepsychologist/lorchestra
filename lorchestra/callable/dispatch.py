"""
Callable dispatch - In-proc dispatch to callables.

This module provides the dispatch layer for call.* operations:
1. Maps Op to callable function via explicit registry
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
from lorchestra.schemas.ops import Op


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


def _get_callables() -> dict[Op, CallableFn]:
    """
    Build the callable registry with lazy imports.

    This function is called once and cached. It attempts to import
    each callable module and only registers those that are available.

    For v0, we use stub implementations. As the actual callable
    libraries are implemented, replace stubs with real imports:

        import injest
        CALL_INJEST: injest.execute,
    """
    callables: dict[Op, CallableFn] = {}

    # Map of Op to module name
    op_modules = {
        Op.CALL_INJEST: "injest",
        Op.CALL_CANONIZER: "canonizer",
        Op.CALL_FINALFORM: "finalform",
        Op.CALL_PROJECTIONIST: "projectionist",
        Op.CALL_WORKMAN: "workman",
    }

    # Try to import each callable module
    for op, module_name in op_modules.items():
        fn = _try_import_callable(module_name)
        if fn is not None:
            callables[op] = fn
        else:
            callables[op] = _stub_callable(module_name)

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
_CALLABLES: dict[Op, CallableFn] | None = None


def get_callables() -> dict[Op, CallableFn]:
    """Get the callable registry, initializing if needed."""
    global _CALLABLES
    if _CALLABLES is None:
        _CALLABLES = _get_callables()
    return _CALLABLES


def dispatch_callable(op: Op, params: dict) -> CallableResult:
    """
    Dispatch to in-proc callable and return CallableResult.

    Callables raise TransientError or PermanentError on failure.
    This function does NOT classify - it just propagates.
    Classification happens at the source (callables) or executor boundary.

    Args:
        op: The call.* operation to dispatch
        params: Parameters to pass to the callable

    Returns:
        CallableResult with items or items_ref

    Raises:
        ValueError: If op is not a call.* operation
        TransientError: Propagated from callable (safe to retry)
        PermanentError: Propagated from callable (do not retry)
        NotImplementedError: If callable is not installed
    """
    if not op.value.startswith("call."):
        raise ValueError(f"dispatch_callable only handles call.* ops, got: {op}")

    callables = get_callables()
    fn = callables.get(op)

    if fn is None:
        raise ValueError(f"Unknown callable op: {op}")

    # Invoke callable - exceptions propagate unchanged
    result_dict = fn(params)

    # Wrap in CallableResult
    return CallableResult(**result_dict)


def register_callable(op: Op, fn: CallableFn) -> None:
    """
    Register a callable function for an op.

    This is primarily for testing - allows injecting mock callables.

    Args:
        op: The call.* operation
        fn: Function that takes params dict and returns result dict
    """
    if not op.value.startswith("call."):
        raise ValueError(f"Can only register call.* ops, got: {op}")

    callables = get_callables()
    callables[op] = fn
