"""InJest configuration for lorchestra.

This module configures the InJest ConnectionStore with identity mappings
for all email accounts. Call configure_injest() once at process startup.

Uses AuthctlConnectionStore exclusively - all credentials are managed by authctl.
AUTHCTL_HOME must be set to point to the authctl home directory.
"""

import os

from injest.auth.store import configure_store, AuthctlConnectionStore


_configured = False


def configure_injest() -> None:
    """Configure InJest with AuthctlConnectionStore.

    Safe to call multiple times - only configures on first call.

    Raises:
        RuntimeError: If AUTHCTL_HOME is not set
    """
    global _configured
    if _configured:
        return

    # Require AUTHCTL_HOME - all credentials are managed by authctl
    if not os.environ.get("AUTHCTL_HOME"):
        raise RuntimeError(
            "AUTHCTL_HOME environment variable is required. "
            "Set it to the authctl home directory (e.g., /workspace/.authctl)"
        )

    configure_store(AuthctlConnectionStore())
    _configured = True
