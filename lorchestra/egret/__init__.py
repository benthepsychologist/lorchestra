"""
Egret client module for lorchestra.

This module provides the client boundary for submitting EgretPlan
to egret. Currently implements in-proc v0; RPC transport to follow.
"""

from lorchestra.egret.client import (
    RpcMeta,
    submit_plan,
)

__all__ = [
    "RpcMeta",
    "submit_plan",
]
