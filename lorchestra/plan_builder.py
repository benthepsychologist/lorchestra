"""
Plan Builder - Convert CallableResult to StoraclePlan.

This module converts the normalized CallableResult from callables
into a StoraclePlan that can be submitted to storacle.

StoraclePlan contract (storacle.plan/1.0.0):
{
  "plan_version": "storacle.plan/1.0.0",
  "plan_id": "sha256:...",
  "jsonrpc": "2.0",
  "meta": { "correlation_id": "..." },
  "ops": [
    {
      "jsonrpc": "2.0",
      "id": "uuid",
      "method": "wal.append",
      "params": { ... }
    }
  ]
}

Idempotency key computation:
- Hash the smallest stable identity fields, not the entire blob
- This avoids non-determinism from float precision, timestamps, etc.
- Items SHOULD include stable identity fields (stream_id, event_id, etc.)
"""

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

from lorchestra.callable.result import CallableResult


PLAN_VERSION = "storacle.plan/1.0.0"


@dataclass
class StoracleOp:
    """A single JSON-RPC 2.0 operation in a StoraclePlan."""
    op_id: str
    method: str
    params: dict
    idempotency_key: str | None = None

    def to_dict(self) -> dict:
        """Convert to JSON-RPC 2.0 request object."""
        d: dict = {
            "jsonrpc": "2.0",
            "id": self.op_id,
            "method": self.method,
            "params": dict(self.params),
        }
        if self.idempotency_key:
            d["params"]["idempotency_key"] = self.idempotency_key
        return d


@dataclass
class StoraclePlan:
    """
    Plan for storacle to execute.

    Conforms to storacle.plan/1.0.0 contract:
    - plan_version: "storacle.plan/1.0.0"
    - plan_id: deterministic hash of ops
    - jsonrpc: "2.0"
    - ops: list of JSON-RPC 2.0 request objects
    """
    correlation_id: str = ""
    ops: list[StoracleOp] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to storacle.plan/1.0.0 contract dict."""
        ops_list = [op.to_dict() for op in self.ops]
        plan = {
            "plan_version": PLAN_VERSION,
            "plan_id": _hash_canonical({"ops": ops_list}),
            "jsonrpc": "2.0",
            "meta": {
                "correlation_id": self.correlation_id,
            },
            "ops": ops_list,
        }
        return plan

    @classmethod
    def _from_dict(cls, d: dict) -> "StoraclePlan":
        """Reconstruct from a serialized plan dict (round-trip support)."""
        correlation_id = d.get("meta", {}).get("correlation_id", "")
        ops = []
        for op_data in d.get("ops", []):
            params = dict(op_data.get("params", {}))
            idem_key = params.pop("idempotency_key", None)
            ops.append(StoracleOp(
                op_id=op_data.get("id", str(uuid.uuid4())),
                method=op_data["method"],
                params=params,
                idempotency_key=idem_key,
            ))
        return cls(correlation_id=correlation_id, ops=ops)


def _hash_canonical(data: dict) -> str:
    """
    Compute canonical hash of a dictionary.

    Uses JSON serialization with sorted keys for determinism.

    Args:
        data: Dictionary to hash

    Returns:
        SHA256 hash prefixed with "sha256:"
    """
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    return f"sha256:{digest}"


def _compute_idempotency_key(item: dict, method: str) -> str:
    """
    Compute deterministic idempotency key from stable identity fields.

    IMPORTANT: Hash the smallest stable identity fields, not the entire blob.
    This avoids non-determinism from:
    - Float precision differences
    - Timestamp variations
    - Innocuous field changes breaking idempotency

    Items SHOULD include stable identity fields like:
    - stream_id + event_id (for events)
    - entity_id + version (for entities)
    - idem_key (if already computed)

    Args:
        item: The item to compute key for
        method: The storacle method (e.g., "wal.append")

    Returns:
        Idempotency key in format "sha256:..."
    """
    # Extract stable identity fields if present
    identity: dict = {}
    for key in ("idem_key", "idempotency_key", "stream_id", "event_id", "entity_id", "version", "id"):
        if key in item:
            identity[key] = item[key]

    # Fall back to full item hash if no identity fields (less safe)
    if not identity:
        identity = item

    return _hash_canonical({"method": method, **identity})


def build_plan(
    result: CallableResult,
    correlation_id: str,
    method: str = "wal.append",
) -> StoraclePlan:
    """
    Convert CallableResult to StoraclePlan.

    Each item in the result becomes a storacle op.

    v0 note: items_ref is allowed in CallableResult but NOT executed.
    If items_ref is set, this function raises - caller must dereference first
    or the callable should return items inline for now.

    Args:
        result: CallableResult from callable dispatch
        correlation_id: Correlation ID for tracing
        method: Storacle method to use (default: "wal.append")

    Returns:
        StoraclePlan ready for submission to storacle

    Raises:
        NotImplementedError: If items_ref is set (v0 limitation)
    """
    if result.items_ref is not None:
        raise NotImplementedError(
            "items_ref not supported in v0 - callables must return inline items"
        )

    ops: list[StoracleOp] = []
    items = result.items or []

    for item in items:
        op = StoracleOp(
            op_id=str(uuid.uuid4()),
            method=method,
            params=item,
            idempotency_key=_compute_idempotency_key(item, method) if method == "wal.append" else None,
        )
        ops.append(op)

    return StoraclePlan(
        correlation_id=correlation_id,
        ops=ops,
    )


def _apply_field_params(
    items: list[dict],
    fields: list[str] | None = None,
    field_map: dict[str, str] | None = None,
    field_defaults: dict[str, Any] | None = None,
) -> list[dict]:
    """
    Apply field filtering, mapping, and defaults to items.

    Processing order:
    1. field_map: rename keys (new_key -> old_key mapping)
    2. field_defaults: backfill missing keys with defaults
    3. fields: filter to allowlist (fail fast on missing required fields)

    Args:
        items: List of item dicts to process
        fields: Allowlist of keys to keep (items missing these keys raise ValueError)
        field_map: Rename mapping {new_key: old_key}
        field_defaults: Default values for missing keys

    Returns:
        Processed items list

    Raises:
        ValueError: If fields specified and an item is missing a required field
    """
    if not fields and not field_map and not field_defaults:
        return items

    result = []
    for item in items:
        processed = dict(item)

        # 1. Apply field_map (rename keys)
        if field_map:
            for new_key, old_key in field_map.items():
                if old_key in processed:
                    processed[new_key] = processed.pop(old_key)

        # 2. Apply field_defaults (backfill missing keys)
        if field_defaults:
            for key, default in field_defaults.items():
                if key not in processed:
                    processed[key] = default

        # 3. Apply fields filter (allowlist)
        if fields:
            missing = [f for f in fields if f not in processed]
            if missing:
                raise ValueError(
                    f"Item missing required field(s): {missing}. "
                    f"Available: {list(processed.keys())}"
                )
            processed = {k: v for k, v in processed.items() if k in fields}

        result.append(processed)
    return result


def build_plan_from_items(
    items: list[dict],
    correlation_id: str,
    method: str = "wal.append",
    fields: list[str] | None = None,
    field_map: dict[str, str] | None = None,
    field_defaults: dict[str, Any] | None = None,
) -> StoraclePlan:
    """
    Build StoraclePlan from raw items. Used by plan.build native op.

    This is the explicit plan building entry point — items come from
    a previous step's output (via @run.step_id.items), not from a
    CallableResult directly.

    Args:
        items: List of item dicts to convert to storacle ops
        correlation_id: Correlation ID for tracing
        method: Storacle method (default: "wal.append")
        fields: Optional allowlist of keys to persist
        field_map: Optional key rename mapping {new_key: old_key}
        field_defaults: Optional default values for missing keys

    Returns:
        StoraclePlan ready for submission to storacle
    """
    processed = _apply_field_params(items, fields, field_map, field_defaults)

    ops: list[StoracleOp] = []
    for item in processed:
        op = StoracleOp(
            op_id=str(uuid.uuid4()),
            method=method,
            params=item,
            idempotency_key=_compute_idempotency_key(item, method) if method == "wal.append" else None,
        )
        ops.append(op)

    return StoraclePlan(
        correlation_id=correlation_id,
        ops=ops,
    )
