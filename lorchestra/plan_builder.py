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

    Processing order (matches batch path in _build_batch_plan):
    1. field_defaults: backfill missing keys with defaults
    2. field_map: rename keys (new_key -> old_key mapping)
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

        # 1. Apply field_defaults (backfill missing keys)
        if field_defaults:
            for key, default in field_defaults.items():
                if key not in processed:
                    processed[key] = default

        # 2. Apply field_map (rename keys)
        if field_map:
            for new_key, old_key in field_map.items():
                if old_key in processed:
                    processed[new_key] = processed.pop(old_key)

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


def _resolve_dataset(logical_name: str) -> str:
    """
    Resolve logical dataset name to actual BQ dataset name via config.

    Logical names: "raw", "canonical", "derived"
    Actual names come from LorchestraConfig (e.g., "raw", "canonical", "derived"
    or environment-specific overrides like "events_dev").

    If the name is not a known logical name, it is returned as-is
    (allows direct BQ dataset names as an escape hatch).

    TODO(e005b-08): load_config() reads config.yaml on every call.
    Fine for single plan builds, but if we ever batch-build plans
    in a tight loop, add an @lru_cache or accept config as a param.

    Args:
        logical_name: Logical dataset name from YAML job def

    Returns:
        Actual BQ dataset name
    """
    from lorchestra.config import load_config

    _LOGICAL_MAP = {
        "raw": "dataset_raw",
        "canonical": "dataset_canonical",
        "derived": "dataset_derived",
    }

    attr = _LOGICAL_MAP.get(logical_name)
    if attr is None:
        return logical_name

    config = load_config()
    return getattr(config, attr)


def _compute_idem_key(
    item: dict,
    id_field: str,
    field_defaults: dict[str, Any] | None = None,
) -> str:
    """
    Compute idem_key from raw item identity field + metadata.

    Pattern: {source_system}:{connection_name}:{object_type}:{item[id_field]}

    Args:
        item: Raw item dict (pre-wrapping, original callable output)
        id_field: Key to extract the unique identifier from the raw item
        field_defaults: Metadata dict containing source_system, connection_name, object_type

    Returns:
        Idem key string

    Raises:
        ValueError: If id_field not found in item or required metadata missing
    """
    defaults = field_defaults or {}
    id_value = item.get(id_field)
    if id_value is None:
        raise ValueError(
            f"id_field '{id_field}' not found in item. "
            f"Available keys: {list(item.keys())}"
        )

    source_system = defaults.get("source_system", "")
    connection_name = defaults.get("connection_name", "")
    object_type = defaults.get("object_type", "")

    return f"{source_system}:{connection_name}:{object_type}:{id_value}"


def _extract_external_id(item: dict) -> str | None:
    """
    Extract natural external_id from a raw item payload.

    Probes common ID field names across providers, matching the V1
    logic in event_client._extract_external_id().

    Args:
        item: Raw item dict from callable output

    Returns:
        External ID as string, or None
    """
    val = (
        item.get("id") or
        item.get("message_id") or
        item.get("external_id") or
        item.get("uuid") or
        item.get("responseId") or
        item.get("contactid") or
        item.get("cre92_clientsessionid") or
        item.get("cre92_clientreportid")
    )
    return str(val) if val else None


def build_plan_from_items(
    items: list[dict],
    correlation_id: str,
    method: str = "wal.append",
    fields: list[str] | None = None,
    field_map: dict[str, str] | None = None,
    field_defaults: dict[str, Any] | None = None,
    # Batch wrapping params (e005b-07)
    dataset: str | None = None,
    table: str | None = None,
    key_columns: list[str] | None = None,
    payload_wrap: bool = False,
    id_field: str | None = None,
    auto_external_id: bool = False,
    auto_timestamp_columns: list[str] | None = None,
    # Suffix params (e005b-08)
    idem_key_suffix: str | None = None,
    # MERGE behavior params
    skip_update_columns: list[str] | None = None,
) -> StoraclePlan:
    """
    Build StoraclePlan from raw items. Used by plan.build native op.

    This is the explicit plan building entry point — items come from
    a previous step's output (via @run.step_id.items), not from a
    CallableResult directly.

    When dataset + table + key_columns are all provided, enters **batch
    wrapping mode**: processes items through payload wrapping, idem_key
    computation, metadata injection, field transforms, and bundles all
    rows into a single bq.upsert op.

    When batch params are NOT provided, keeps current behavior: one op
    per item, item dict becomes params directly.

    Args:
        items: List of item dicts to convert to storacle ops
        correlation_id: Correlation ID for tracing
        method: Storacle method (default: "wal.append")
        fields: Optional allowlist of keys to persist
        field_map: Optional key rename mapping {new_key: old_key}
        field_defaults: Optional default values for missing keys
        dataset: Logical dataset name (triggers batch mode with table + key_columns)
        table: Target table name
        key_columns: Merge key columns for bq.upsert
        payload_wrap: If true, nest each raw item as JSON payload column
        id_field: Field name to extract unique ID for idem_key computation
        auto_external_id: If true, probe raw item for external_id (V1 compat)
        auto_timestamp_columns: Column names to auto-fill with current UTC timestamp
        skip_update_columns: Columns to exclude from UPDATE SET in MERGE
            (still included in INSERT). For immutable columns like created_at.

    Returns:
        StoraclePlan ready for submission to storacle
    """
    batch_mode = dataset is not None and table is not None and key_columns is not None

    if batch_mode:
        return _build_batch_plan(
            items=items,
            correlation_id=correlation_id,
            method=method,
            fields=fields,
            field_map=field_map,
            field_defaults=field_defaults,
            dataset=dataset,
            table=table,
            key_columns=key_columns,
            payload_wrap=payload_wrap,
            id_field=id_field,
            auto_external_id=auto_external_id,
            auto_timestamp_columns=auto_timestamp_columns,
            idem_key_suffix=idem_key_suffix,
            skip_update_columns=skip_update_columns,
        )

    # Non-batch mode: one op per item (existing behavior)
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


def _build_batch_plan(
    items: list[dict],
    correlation_id: str,
    method: str,
    fields: list[str] | None,
    field_map: dict[str, str] | None,
    field_defaults: dict[str, Any] | None,
    dataset: str,
    table: str,
    key_columns: list[str],
    payload_wrap: bool,
    id_field: str | None,
    auto_external_id: bool = False,
    auto_timestamp_columns: list[str] | None = None,
    idem_key_suffix: str | None = None,
    skip_update_columns: list[str] | None = None,
) -> StoraclePlan:
    """
    Build a batch StoraclePlan: all rows bundled into a single op.

    Processing order per spec:
    1. payload_wrap: nest raw item as JSON payload column
    2. auto_external_id: extract external_id from raw item (before wrapping hides it)
    3. field_defaults: add metadata columns
    4. auto_timestamp_columns: set timestamp columns to current UTC time
    5. correlation_id: inject into each row
    6. id_field: compute idem_key
    7. field_map: rename keys
    8. fields: column allowlist
    9. Bundle into single {dataset, table, key_columns, rows} op
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []

    for item in items:
        row: dict[str, Any] = {}

        # 1. Payload wrapping
        if payload_wrap:
            row["payload"] = json.dumps(item, default=str)
        else:
            row = dict(item)

        # 2. Extract external_id from raw item (before payload_wrap hides fields)
        if auto_external_id:
            row["external_id"] = _extract_external_id(item)

        # 3. Field defaults (metadata injection)
        if field_defaults:
            for key, default in field_defaults.items():
                if key not in row:
                    row[key] = default

        # 4. Auto timestamp columns (e.g. first_seen, last_seen, canonicalized_at)
        if auto_timestamp_columns:
            for col in auto_timestamp_columns:
                row[col] = now

        # 5. Correlation ID — inject into each row (V1 stores it per-row)
        if "correlation_id" not in row:
            row["correlation_id"] = correlation_id

        # 6. Idem key computation (before field_map/fields, uses raw item)
        if id_field:
            row["idem_key"] = _compute_idem_key(item, id_field, field_defaults)

        # 6b. Idem key suffix: append #suffix to existing idem_key
        if idem_key_suffix and "idem_key" in row:
            raw_key = row["idem_key"]
            row["idem_key"] = f"{raw_key}#{idem_key_suffix}"

        # 7. Field map (rename keys)
        if field_map:
            for new_key, old_key in field_map.items():
                if old_key in row:
                    row[new_key] = row.pop(old_key)

        # 8. Fields allowlist
        if fields:
            missing = [f for f in fields if f not in row]
            if missing:
                raise ValueError(
                    f"Item missing required field(s): {missing}. "
                    f"Available: {list(row.keys())}"
                )
            row = {k: v for k, v in row.items() if k in fields}

        rows.append(row)

    # Resolve logical dataset name
    resolved_dataset = _resolve_dataset(dataset)

    # Bundle all rows into a single op
    op_params: dict[str, Any] = {
        "dataset": resolved_dataset,
        "table": table,
        "key_columns": key_columns,
        "rows": rows,
    }
    if skip_update_columns:
        op_params["skip_update_columns"] = skip_update_columns

    op = StoracleOp(
        op_id=str(uuid.uuid4()),
        method=method,
        params=op_params,
    )

    return StoraclePlan(
        correlation_id=correlation_id,
        ops=[op],
    )
