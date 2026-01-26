"""
Plan Builder - Convert CallableResult to StoraclePlan.

This module converts the normalized CallableResult from callables
into a StoraclePlan that can be submitted to storacle.

StoraclePlan schema (from epic):
{
  "kind": "storacle.plan",
  "version": "0.1",
  "correlation_id": "...",
  "ops": [
    {
      "op_id": "uuid",
      "method": "wal.append",
      "idempotency_key": "sha256:...",
      "params": { }
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

from lorchestra.callable.result import CallableResult


@dataclass
class StoracleOp:
    """A single operation in a StoraclePlan."""
    op_id: str
    method: str
    params: dict
    idempotency_key: str | None = None


@dataclass
class StoraclePlan:
    """
    Plan for storacle to execute.

    Contains a list of operations to be executed atomically
    or in sequence by storacle.
    """
    kind: str = "storacle.plan"
    version: str = "0.1"
    correlation_id: str = ""
    ops: list[StoracleOp] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "kind": self.kind,
            "version": self.version,
            "correlation_id": self.correlation_id,
            "ops": [
                {
                    "op_id": op.op_id,
                    "method": op.method,
                    "params": op.params,
                    "idempotency_key": op.idempotency_key,
                }
                for op in self.ops
            ],
        }


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
    for key in ("idem_key", "stream_id", "event_id", "entity_id", "version", "id"):
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
