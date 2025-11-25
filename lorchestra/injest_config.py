"""InJest configuration for lorchestra.

This module configures the InJest ConnectionStore with identity mappings
for all email accounts. Call configure_injest() once at process startup.
"""

import os
from typing import Mapping, Any

from injest.auth.store import configure_store

JSONDict = Mapping[str, Any]

# Gmail identity -> env var mappings (values are env var names)
GMAIL_IDENTITY_MAPPING = {
    "gmail:acct1": {
        "client_id": "TAP_GMAIL_ACCT1_PERSONAL_CLIENT_ID",
        "client_secret": "TAP_GMAIL_ACCT1_PERSONAL_CLIENT_SECRET",
        "refresh_token": "TAP_GMAIL_ACCT1_PERSONAL_REFRESH_TOKEN",
    },
    "gmail:acct2": {
        "client_id": "TAP_GMAIL_ACCT2_BUSINESS1_CLIENT_ID",
        "client_secret": "TAP_GMAIL_ACCT2_BUSINESS1_CLIENT_SECRET",
        "refresh_token": "TAP_GMAIL_ACCT2_BUSINESS1_REFRESH_TOKEN",
    },
    "gmail:acct3": {
        "client_id": "TAP_GMAIL_ACCT3_BFARMSTRONG_CLIENT_ID",
        "client_secret": "TAP_GMAIL_ACCT3_BFARMSTRONG_CLIENT_SECRET",
        "refresh_token": "TAP_GMAIL_ACCT3_BFARMSTRONG_REFRESH_TOKEN",
    },
}

# Exchange identity -> config (mix of env vars and literal values)
# tenant_id, client_id are env vars; token_cache_path and user_email are literals
EXCHANGE_IDENTITY_MAPPING = {
    "exchange:ben-mensio": {
        "tenant_id": "TAP_MSGRAPH_BEN_MENSIO_TENANT_ID",
        "client_id": "TAP_MSGRAPH_BEN_MENSIO_CLIENT_ID",
        "token_cache_path": "/workspace/ingestor/.tokens/graph_token_cache_ben_mensio.bin",
        "user_email": "ben@mensiomentalhealth.com",
    },
    "exchange:booking-mensio": {
        "tenant_id": "TAP_MSGRAPH_BOOKING_MENSIO_TENANT_ID",
        "client_id": "TAP_MSGRAPH_BOOKING_MENSIO_CLIENT_ID",
        "token_cache_path": "/workspace/ingestor/.tokens/graph_token_cache_booking_mensio.bin",
        "user_email": "booking@mensiomentalhealth.com",
    },
    "exchange:info-mensio": {
        "tenant_id": "TAP_MSGRAPH_INFO_MENSIO_TENANT_ID",
        "client_id": "TAP_MSGRAPH_INFO_MENSIO_CLIENT_ID",
        "token_cache_path": "/workspace/ingestor/.tokens/graph_token_cache_info_mensio.bin",
        "user_email": "info@mensiomentalhealth.com",
    },
    "exchange:ben-efs": {
        "tenant_id": "TAP_MSGRAPH_BEN_EFS_TENANT_ID",
        "client_id": "TAP_MSGRAPH_BEN_EFS_CLIENT_ID",
        "token_cache_path": "/workspace/ingestor/.tokens/graph_token_cache_ben_efs.bin",
        "user_email": "ben@ethicalfootprintsolutions.com",
    },
}


class HybridConnectionStore:
    """ConnectionStore that handles both env vars and literal values.

    For Gmail: all values are env var names
    For Exchange: tenant_id/client_id are env vars, others are literals
    """

    # Keys that are always env var names (not literal values)
    ENV_VAR_KEYS = {"client_id", "client_secret", "refresh_token", "tenant_id"}

    def __init__(self, gmail_mapping: dict, exchange_mapping: dict):
        self._gmail = gmail_mapping
        self._exchange = exchange_mapping

    def load(self, identity: str) -> JSONDict:
        """Load config for identity, resolving env vars where needed."""
        if identity in self._gmail:
            # Gmail: all values are env var names
            env_mapping = self._gmail[identity]
            return {k: os.environ[v] for k, v in env_mapping.items()}

        if identity in self._exchange:
            # Exchange: some are env vars, some are literals
            cfg = self._exchange[identity]
            result = {}
            for k, v in cfg.items():
                if k in self.ENV_VAR_KEYS:
                    result[k] = os.environ[v]
                else:
                    result[k] = v
            return result

        raise KeyError(f"Unknown identity: {identity}")

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

    store = HybridConnectionStore(GMAIL_IDENTITY_MAPPING, EXCHANGE_IDENTITY_MAPPING)
    configure_store(store)
    _configured = True
