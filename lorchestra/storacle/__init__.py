"""
Storacle client module for lorchestra.

This module provides the client boundary for submitting StoraclePlan
to storacle. Currently implements in-proc v0; RPC transport to follow.
"""

from lorchestra.storacle.client import (
    RpcMeta,
    submit_plan,
)

__all__ = [
    "RpcMeta",
    "submit_plan",
]
