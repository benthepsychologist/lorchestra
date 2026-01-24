"""
Storacle client interface for data-plane operations.

This module defines the protocol that any storacle client must implement,
allowing the DataPlaneHandler to be decoupled from the actual storage backend.

Implementations:
- NoOpStoracleClient: For testing and dry-run mode
- StoracleRpcClient: Real implementation calling storacle.rpc (external)
"""

from datetime import datetime
from typing import Any, Iterator, Protocol, runtime_checkable


@runtime_checkable
class StoracleClient(Protocol):
    """
    Protocol for data-plane operations via storacle.

    This interface abstracts all storage operations so that:
    1. lorchestra orchestration layer has no BigQuery imports
    2. Storage backend can be swapped (BQ, SQLite, mock, etc.)
    3. Testing is simplified via mock implementations
    """

    # -------------------------------------------------------------------------
    # Query Operations (query.*)
    # -------------------------------------------------------------------------

    def query_raw_objects(
        self,
        source_system: str,
        object_type: str,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Query raw objects from storage.

        Args:
            source_system: Filter by source_system column
            object_type: Filter by object_type column
            filters: Additional column filters (e.g., {"validation_status": "pass"})
            limit: Maximum records to return

        Yields:
            Dict with idem_key, payload, and metadata columns
        """
        ...

    def query_canonical_objects(
        self,
        canonical_schema: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Query canonical objects from storage.

        Args:
            canonical_schema: Filter by canonical_schema column
            filters: Additional column filters
            limit: Maximum records to return

        Yields:
            Dict with idem_key, payload, and metadata columns
        """
        ...

    def query_last_sync(
        self,
        source_system: str,
        connection_name: str,
        object_type: str,
    ) -> datetime | None:
        """
        Get the last sync timestamp for incremental sync.

        Args:
            source_system: Source system identifier
            connection_name: Connection name
            object_type: Object type

        Returns:
            Last sync timestamp, or None if never synced
        """
        ...

    # -------------------------------------------------------------------------
    # Write Operations (write.*)
    # -------------------------------------------------------------------------

    def upsert(
        self,
        table: str,
        objects: list[dict[str, Any]],
        idem_key_field: str = "idem_key",
    ) -> dict[str, int]:
        """
        Upsert objects by idempotency key.

        Uses MERGE semantics: insert new, update existing by idem_key.

        Args:
            table: Target table name
            objects: List of objects to upsert
            idem_key_field: Field name containing the idempotency key

        Returns:
            Dict with counts: {"inserted": N, "updated": M}
        """
        ...

    def insert(
        self,
        table: str,
        objects: list[dict[str, Any]],
    ) -> int:
        """
        Insert objects (no deduplication).

        Args:
            table: Target table name
            objects: List of objects to insert

        Returns:
            Number of rows inserted
        """
        ...

    def delete(
        self,
        table: str,
        filters: dict[str, Any],
    ) -> int:
        """
        Delete objects matching filters.

        Args:
            table: Target table name
            filters: Column filters for deletion

        Returns:
            Number of rows deleted
        """
        ...

    def merge(
        self,
        table: str,
        objects: list[dict[str, Any]],
        merge_keys: list[str],
    ) -> dict[str, int]:
        """
        Perform a MERGE operation with custom merge keys.

        Args:
            table: Target table name
            objects: List of objects for merge
            merge_keys: Columns to use as merge keys

        Returns:
            Dict with counts: {"inserted": N, "updated": M, "deleted": D}
        """
        ...

    # -------------------------------------------------------------------------
    # Assert Operations (assert.*)
    # -------------------------------------------------------------------------

    def assert_rows(
        self,
        table: str,
        filters: dict[str, Any] | None = None,
        expected_count: int | None = None,
        min_count: int | None = None,
        max_count: int | None = None,
    ) -> dict[str, Any]:
        """
        Assert row count constraints.

        Args:
            table: Table to check
            filters: Optional filters to apply
            expected_count: Exact count expected (if set)
            min_count: Minimum count (if set)
            max_count: Maximum count (if set)

        Returns:
            Dict with {"passed": bool, "actual_count": N, "message": str}

        Raises:
            AssertionError: If assertion fails and strict mode
        """
        ...

    def assert_schema(
        self,
        table: str,
        expected_columns: list[dict[str, str]],
    ) -> dict[str, Any]:
        """
        Assert table schema matches expectations.

        Args:
            table: Table to check
            expected_columns: List of {"name": str, "type": str} dicts

        Returns:
            Dict with {"passed": bool, "missing": [...], "extra": [...], "mismatched": [...]}
        """
        ...

    def assert_unique(
        self,
        table: str,
        columns: list[str],
    ) -> dict[str, Any]:
        """
        Assert columns form a unique key (no duplicates).

        Args:
            table: Table to check
            columns: Columns that should be unique together

        Returns:
            Dict with {"passed": bool, "duplicate_count": N}
        """
        ...


class NoOpStoracleClient:
    """
    No-op implementation of StoracleClient for testing.

    Returns empty/mock results without performing actual operations.
    """

    def query_raw_objects(
        self,
        source_system: str,
        object_type: str,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Return empty iterator."""
        return iter([])

    def query_canonical_objects(
        self,
        canonical_schema: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Return empty iterator."""
        return iter([])

    def query_last_sync(
        self,
        source_system: str,
        connection_name: str,
        object_type: str,
    ) -> datetime | None:
        """Return None (never synced)."""
        return None

    def upsert(
        self,
        table: str,
        objects: list[dict[str, Any]],
        idem_key_field: str = "idem_key",
    ) -> dict[str, int]:
        """Return mock upsert result."""
        return {"inserted": len(objects), "updated": 0}

    def insert(
        self,
        table: str,
        objects: list[dict[str, Any]],
    ) -> int:
        """Return mock insert count."""
        return len(objects)

    def delete(
        self,
        table: str,
        filters: dict[str, Any],
    ) -> int:
        """Return mock delete count."""
        return 0

    def merge(
        self,
        table: str,
        objects: list[dict[str, Any]],
        merge_keys: list[str],
    ) -> dict[str, int]:
        """Return mock merge result."""
        return {"inserted": len(objects), "updated": 0, "deleted": 0}

    def assert_rows(
        self,
        table: str,
        filters: dict[str, Any] | None = None,
        expected_count: int | None = None,
        min_count: int | None = None,
        max_count: int | None = None,
    ) -> dict[str, Any]:
        """Return mock assertion result (always passes)."""
        return {"passed": True, "actual_count": 0, "message": "noop"}

    def assert_schema(
        self,
        table: str,
        expected_columns: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Return mock assertion result (always passes)."""
        return {"passed": True, "missing": [], "extra": [], "mismatched": []}

    def assert_unique(
        self,
        table: str,
        columns: list[str],
    ) -> dict[str, Any]:
        """Return mock assertion result (always passes)."""
        return {"passed": True, "duplicate_count": 0}
