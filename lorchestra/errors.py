"""
Error classes for lorchestra execution.

These error types enable retry classification at execution boundaries:
- TransientError: Safe to retry (rate limits, network issues, temporary failures)
- PermanentError: Do not retry (invalid input, schema errors, missing resources)

Callables raise these errors to signal retry behavior.
The executor catches at the boundary for retry/backoff and run recording.

Error handling contract:
- CallableResult is success-only
- Errors are exceptions, not values
- Don't mix "errors as values" with exceptions
"""


class LorchestraError(Exception):
    """Base exception for lorchestra."""
    pass


class TransientError(LorchestraError):
    """
    Transient error - safe to retry.

    Examples:
    - Rate limit exceeded
    - Network timeout
    - Service temporarily unavailable
    - Connection reset

    The executor will retry operations that raise TransientError
    according to the configured retry policy.
    """
    pass


class PermanentError(LorchestraError):
    """
    Permanent error - do not retry.

    Examples:
    - Invalid input/parameters
    - Schema validation error
    - Resource not found (404)
    - Authorization failed (403)
    - Data integrity violation

    The executor will immediately fail the step without retry
    when PermanentError is raised.
    """
    pass
