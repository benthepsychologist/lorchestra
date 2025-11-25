"""InJest configuration for lorchestra.

This module configures the InJest ConnectionStore with identity mappings
for all email accounts. Call configure_injest() once at process startup.
"""

import os
from typing import Mapping, Any

from injest.auth.store import configure_store
from injest.config import ACCOUNTS, get_token_cache_path

JSONDict = Mapping[str, Any]


class HybridConnectionStore:
    """ConnectionStore that handles both env vars and literal values.

    For Gmail: resolves client_id, client_secret, refresh_token from env vars
    For Exchange: resolves tenant_id, client_id from env vars; derives token_cache_path
    """

    def __init__(self):
        pass

    def load(self, identity: str) -> JSONDict:
        """Load config for identity, resolving env vars and deriving paths."""
        if identity not in ACCOUNTS:
            raise KeyError(f"Unknown identity: {identity}")

        cfg = ACCOUNTS[identity]

        if cfg["provider"] == "gmail":
            return {
                "client_id": os.environ[cfg["client_id_env"]],
                "client_secret": os.environ[cfg["client_secret_env"]],
                "refresh_token": os.environ[cfg["refresh_token_env"]],
            }

        if cfg["provider"] == "exchange":
            return {
                "tenant_id": os.environ[cfg["tenant_id_env"]],
                "client_id": os.environ[cfg["client_id_env"]],
                "token_cache_path": str(get_token_cache_path(identity)),
                "user_email": cfg["user_email"],
            }

        if cfg["provider"] == "dataverse":
            from injest.config import INJEST_ROOT
            return {
                "tenant_id": os.environ[cfg["tenant_id_env"]],
                "client_id": os.environ[cfg["client_id_env"]],
                "dataverse_url": os.environ[cfg["dataverse_url_env"]],
                "token_cache_path": str(INJEST_ROOT / cfg["token_cache_path"]),
            }

        raise KeyError(f"Unknown provider: {cfg['provider']}")

    def save(self, identity: str, data: JSONDict) -> None:
        """No-op for v1."""
        pass


_configured = False


def configure_injest() -> None:
    """Configure InJest with the global ConnectionStore.

    Safe to call multiple times - only configures on first call.
    """
    global _configured
    if _configured:
        return

    store = HybridConnectionStore()
    configure_store(store)
    _configured = True
