"""
Callable dispatch module for lorchestra.

This module provides the callable dispatch layer that:
1. Dispatches call.* ops to in-proc callables
2. Returns normalized CallableResult from all callables
3. Propagates TransientError/PermanentError for retry classification
"""

from lorchestra.callable.result import CallableResult
from lorchestra.callable.dispatch import dispatch_callable, register_callable

__all__ = [
    "CallableResult",
    "dispatch_callable",
    "register_callable",
]
