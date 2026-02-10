"""
egret_builder callable — Build EgretPlan from step params.

This callable constructs an EgretPlan (egret.plan/1.0.0 schema) from
the params provided in a job step. It supports both single sends and
batch sends.

In job definitions, this is invoked as:
  op: call
  params:
    callable: egret
    method: msg.send
    to: "@payload.to"
    ...

The callable returns the plan at the top level for simpler @run.build.plan
access in subsequent steps.
"""

import hashlib
import json
from typing import Any


PLAN_VERSION = "egret.plan/1.0.0"


def execute(params: dict[str, Any]) -> dict[str, Any]:
    """
    Build an EgretPlan from params.

    Single send params:
        method: str (e.g., "msg.send")
        to: str
        subject: str
        body: str
        is_html: bool (optional, default False)
        channel: str (default "email")
        provider: str
        account: str
        idempotency_key: str (optional, derived if not provided)

    Batch send params:
        method: str (e.g., "msg.send")
        items: list[{to, subject, body, is_html?, idempotency_key?}]
        channel: str (default "email")
        provider: str
        account: str

    Returns:
        Dict with schema_version, items, stats, and plan at top level
        for @run.build.plan access.
    """
    method = params.get("method", "msg.send")
    items = params.get("items")

    if items:
        # Batch: one msg.send op per item
        ops = [_build_msg_send_op(i, item, params) for i, item in enumerate(items)]
    else:
        # Single: one msg.send op
        ops = [_build_msg_send_op(0, params, params)]

    plan = {
        "plan_version": PLAN_VERSION,
        "jsonrpc": "2.0",
        "ops": ops,
    }

    # Return plan in items - access via @run.build.items[0].plan
    return {
        "schema_version": "1.0",
        "items": [{"plan": plan}],
        "stats": {"ops_count": len(ops)},
    }


def _build_msg_send_op(index: int, item: dict[str, Any], common: dict[str, Any]) -> dict[str, Any]:
    """
    Build a single msg.send operation.

    Args:
        index: Operation index (for id generation)
        item: Item-specific params (to, subject, body, etc.)
        common: Common params (channel, provider, account)

    Returns:
        JSON-RPC 2.0 operation dict
    """
    idempotency_key = item.get("idempotency_key") or _derive_idempotency_key(item, common)
    return {
        "jsonrpc": "2.0",
        "id": f"send-{index}",
        "method": "msg.send",
        "params": {
            "to": item["to"],
            "subject": item["subject"],
            "body": item["body"],
            "is_html": item.get("is_html", False),
            "channel": common.get("channel", "email"),
            "provider": common["provider"],
            "account": common["account"],
            "idempotency_key": idempotency_key,
        },
    }


def _derive_idempotency_key(item: dict[str, Any], common: dict[str, Any]) -> str:
    """
    Derive deterministic idempotency key for single sends.

    The key is derived from a hash of the canonical send parameters,
    making it safe to retry the same send without duplication.

    Args:
        item: Item-specific params
        common: Common params

    Returns:
        Idempotency key string in format "email.send:{hash}"
    """
    canonical = {
        "to": item["to"],
        "subject": item["subject"],
        "body": item["body"],
        "is_html": item.get("is_html", False),
        "channel": common.get("channel", "email"),
        "provider": common["provider"],
        "account": common["account"],
    }
    h = hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()[:16]
    return f"email.send:{h}"
