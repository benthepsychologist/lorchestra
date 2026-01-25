"""
Data Plane Handler for storacle operations.

Handles all data-plane operations:
- query.* (query.raw_objects, query.canonical_objects, query.last_sync)
- write.* (write.upsert, write.insert, write.delete, write.merge)
- assert.* (assert.rows, assert.schema, assert.unique)

These operations are delegated to a StoracleClient implementation,
keeping the orchestration layer free of storage implementation details.

Defense-in-depth:
- Query operations are restricted to an allowlist (enforced here, not just in storacle)
- Query operations require a limit parameter (max 1000) for bounded cost
- WAL scan queries (query.raw_objects) require a time_range for bounded scans
"""

from typing import Any

from lorchestra.handlers.base import Handler
from lorchestra.handlers.storacle_client import StoracleClient
from lorchestra.schemas import StepManifest, Op


# Query operation allowlist - defense-in-depth (per e005 spec)
# lorchestra enforces this before dispatch, not relying solely on storacle
QUERY_OP_ALLOWLIST = frozenset({
    Op.QUERY_RAW_OBJECTS,
    Op.QUERY_CANONICAL_OBJECTS,
    Op.QUERY_LAST_SYNC,
})

# Maximum limit for query operations (bounded cost)
MAX_QUERY_LIMIT = 1000

# Query operations that scan WAL tables and require time_range
WAL_SCAN_OPS = frozenset({
    Op.QUERY_RAW_OBJECTS,
})


class DataPlaneHandler(Handler):
    """
    Handler for data-plane operations (query.*, write.*, assert.*).

    Delegates to storacle via StoracleClient interface.
    """

    def __init__(self, client: StoracleClient):
        """
        Initialize the data plane handler.

        Args:
            client: StoracleClient implementation for storage operations
        """
        self._client = client

    def execute(self, manifest: StepManifest) -> dict[str, Any]:
        """
        Execute a data-plane operation.

        Dispatches to the appropriate StoracleClient method based on the Op.

        Defense-in-depth:
        - Query operations are validated against an allowlist
        - Query operations require a limit parameter (max 1000)

        Args:
            manifest: The StepManifest containing operation details

        Returns:
            The operation result as a dictionary

        Raises:
            ValueError: If the operation is not a data-plane operation
            ValueError: If a query op is not in the allowlist
            ValueError: If a query op exceeds the max limit
        """
        op = manifest.op
        params = manifest.resolved_params

        # Defense-in-depth: Enforce query op allowlist
        if op.is_query_op():
            self._validate_query_op(op, params)

        # Query operations
        if op == Op.QUERY_RAW_OBJECTS:
            return self._query_raw_objects(params)
        elif op == Op.QUERY_CANONICAL_OBJECTS:
            return self._query_canonical_objects(params)
        elif op == Op.QUERY_LAST_SYNC:
            return self._query_last_sync(params)

        # Write operations
        elif op == Op.WRITE_UPSERT:
            return self._write_upsert(params)
        elif op == Op.WRITE_INSERT:
            return self._write_insert(params)
        elif op == Op.WRITE_DELETE:
            return self._write_delete(params)
        elif op == Op.WRITE_MERGE:
            return self._write_merge(params)

        # Assert operations
        elif op == Op.ASSERT_ROWS:
            return self._assert_rows(params)
        elif op == Op.ASSERT_SCHEMA:
            return self._assert_schema(params)
        elif op == Op.ASSERT_UNIQUE:
            return self._assert_unique(params)

        raise ValueError(f"Unsupported data_plane op: {op.value}")

    def _validate_query_op(self, op: Op, params: dict[str, Any]) -> None:
        """
        Validate query operation against allowlist and bounded cost rules.

        Defense-in-depth per e005 spec:
        - Query ops must be in the allowlist (query.raw_objects, query.canonical_objects, query.last_sync)
        - Query ops (except query.last_sync) require a limit parameter (max 1000)
        - WAL scan ops (query.raw_objects) require a time_range parameter

        Args:
            op: The query operation
            params: The operation parameters

        Raises:
            ValueError: If the op is not in the allowlist
            ValueError: If limit is missing or exceeds MAX_QUERY_LIMIT
            ValueError: If time_range is missing for WAL scan ops
        """
        # Check allowlist
        if op not in QUERY_OP_ALLOWLIST:
            raise ValueError(
                f"Query operation '{op.value}' is not in the allowlist. "
                f"Allowed: {[o.value for o in QUERY_OP_ALLOWLIST]}"
            )

        # Check bounded cost (limit required, max 1000)
        # query.last_sync is exempt as it returns a single value, not a result set
        if op != Op.QUERY_LAST_SYNC:
            limit = params.get("limit")
            if limit is None:
                raise ValueError(
                    f"Query operation '{op.value}' requires a 'limit' parameter for bounded cost. "
                    f"Maximum allowed: {MAX_QUERY_LIMIT}"
                )
            if limit > MAX_QUERY_LIMIT:
                raise ValueError(
                    f"Query limit {limit} exceeds maximum allowed ({MAX_QUERY_LIMIT}). "
                    f"Use pagination for larger result sets."
                )

        # Check time_range required for WAL scan operations
        if op in WAL_SCAN_OPS:
            time_range = params.get("time_range")
            if time_range is None:
                raise ValueError(
                    f"Query operation '{op.value}' requires a 'time_range' parameter for bounded WAL scans. "
                    f"Provide {{'start': datetime, 'end': datetime}} to limit scan scope."
                )
            # Validate time_range structure
            if not isinstance(time_range, dict) or "start" not in time_range or "end" not in time_range:
                raise ValueError(
                    f"time_range must be a dict with 'start' and 'end' keys, got: {time_range}"
                )

    # -------------------------------------------------------------------------
    # Query Operations
    # -------------------------------------------------------------------------

    def _query_raw_objects(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute query.raw_objects operation."""
        results = list(
            self._client.query_raw_objects(
                source_system=params["source_system"],
                object_type=params["object_type"],
                filters=params.get("filters"),
                limit=params.get("limit"),
            )
        )
        return {"rows": results, "count": len(results)}

    def _query_canonical_objects(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute query.canonical_objects operation."""
        results = list(
            self._client.query_canonical_objects(
                canonical_schema=params.get("canonical_schema"),
                filters=params.get("filters"),
                limit=params.get("limit"),
            )
        )
        return {"rows": results, "count": len(results)}

    def _query_last_sync(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute query.last_sync operation."""
        last_sync = self._client.query_last_sync(
            source_system=params["source_system"],
            connection_name=params["connection_name"],
            object_type=params["object_type"],
        )
        return {
            "last_sync": last_sync.isoformat() if last_sync else None,
            "has_synced": last_sync is not None,
        }

    # -------------------------------------------------------------------------
    # Write Operations
    # -------------------------------------------------------------------------

    def _write_upsert(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute write.upsert operation."""
        result = self._client.upsert(
            table=params["table"],
            objects=params["objects"],
            idem_key_field=params.get("idem_key_field", "idem_key"),
        )
        return {
            "inserted": result["inserted"],
            "updated": result["updated"],
            "total": result["inserted"] + result["updated"],
        }

    def _write_insert(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute write.insert operation."""
        count = self._client.insert(
            table=params["table"],
            objects=params["objects"],
        )
        return {"inserted": count}

    def _write_delete(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute write.delete operation."""
        count = self._client.delete(
            table=params["table"],
            filters=params["filters"],
        )
        return {"deleted": count}

    def _write_merge(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute write.merge operation."""
        result = self._client.merge(
            table=params["table"],
            objects=params["objects"],
            merge_keys=params["merge_keys"],
        )
        return {
            "inserted": result["inserted"],
            "updated": result["updated"],
            "deleted": result.get("deleted", 0),
            "total": result["inserted"] + result["updated"],
        }

    # -------------------------------------------------------------------------
    # Assert Operations
    # -------------------------------------------------------------------------

    def _assert_rows(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute assert.rows operation."""
        result = self._client.assert_rows(
            table=params["table"],
            filters=params.get("filters"),
            expected_count=params.get("expected_count"),
            min_count=params.get("min_count"),
            max_count=params.get("max_count"),
        )
        return result

    def _assert_schema(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute assert.schema operation."""
        result = self._client.assert_schema(
            table=params["table"],
            expected_columns=params["expected_columns"],
        )
        return result

    def _assert_unique(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute assert.unique operation."""
        result = self._client.assert_unique(
            table=params["table"],
            columns=params["columns"],
        )
        return result
