"""Base protocols and interfaces for lorchestra processors.

This module defines the core abstractions:
- Processor: Protocol for job processors (ingest, canonize, final_form)
- StorageClient: Interface for reading/writing objects to storage
- ProcessorRegistry: Dispatch mechanism for job_type → processor
"""

from dataclasses import dataclass
from typing import Any, Callable, Iterator, Protocol, runtime_checkable

from google.cloud import bigquery


@dataclass
class UpsertResult:
    """Result of an upsert operation."""

    inserted: int
    updated: int


@dataclass
class JobContext:
    """Context passed to processors during execution.

    Contains shared clients and runtime configuration.
    """

    bq_client: bigquery.Client
    run_id: str
    dry_run: bool = False
    test_table: bool = False


@runtime_checkable
class StorageClient(Protocol):
    """Protocol for storage operations.

    Abstracts BigQuery operations so processors don't talk directly to BQ SDK.
    """

    def query_objects(
        self,
        source_system: str,
        object_type: str,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query objects from raw_objects or canonical_objects.

        Args:
            source_system: Filter by source_system column
            object_type: Filter by object_type column
            filters: Additional column filters (e.g., {"validation_status": "pass"})
            limit: Max records to return

        Yields:
            Dict with idem_key, payload, and metadata columns
        """
        ...

    def upsert_objects(
        self,
        objects: Iterator[dict[str, Any]],
        source_system: str,
        connection_name: str,
        object_type: str,
        idem_key_fn: Callable[[dict[str, Any]], str],
        correlation_id: str,
    ) -> UpsertResult:
        """Batch upsert objects to raw_objects.

        Args:
            objects: Iterator of objects to upsert
            source_system: source_system column value
            connection_name: connection_name column value
            object_type: object_type column value
            idem_key_fn: Function to generate idem_key from object
            correlation_id: Correlation ID for tracing

        Returns:
            UpsertResult with inserted/updated counts
        """
        ...

    def update_field(
        self,
        idem_keys: list[str],
        field: str,
        value: Any,
    ) -> int:
        """Update a field on existing objects.

        Args:
            idem_keys: List of idem_keys to update
            field: Field name to update
            value: New value

        Returns:
            Number of rows updated
        """
        ...

    def query_objects_for_canonization(
        self,
        source_system: str,
        object_type: str,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
        canonical_schema: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query validated raw_objects that need canonization.

        Returns records where validation_status='pass' AND either:
        - Not yet in canonical_objects for this schema (never canonized), OR
        - raw_objects.last_seen > canonical_objects.canonicalized_at (raw updated since)

        Args:
            source_system: Filter by source_system column
            object_type: Filter by object_type column
            filters: Additional column filters
            limit: Max records to return
            canonical_schema: Target canonical schema (for 1:N raw->canonical mappings)

        Yields:
            Dict with idem_key, payload, and metadata columns
        """
        ...

    def upsert_canonical(
        self,
        objects: list[dict[str, Any]],
        correlation_id: str,
    ) -> dict[str, int]:
        """Upsert canonical objects (insert new, update existing).

        Args:
            objects: List of canonical objects to upsert
            correlation_id: Correlation ID for tracing

        Returns:
            Dict with 'inserted' and 'updated' counts
        """
        ...

    def query_canonical(
        self,
        canonical_schema: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query canonical objects.

        Args:
            canonical_schema: Filter by canonical_schema column
            filters: Additional column filters
            limit: Max records to return

        Yields:
            Dict with idem_key, payload, and metadata columns
        """
        ...

    def query_canonical_for_formation(
        self,
        canonical_schema: str | None = None,
        filters: dict[str, Any] | None = None,
        measurement_table: str = "measurement_events",
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query canonical objects that need formation (incremental).

        Returns records where:
        - canonical_schema matches (if provided)
        - additional filters match
        - AND either:
          - Not yet in measurement_events (never formed), OR
          - canonical_objects.canonicalized_at > measurement_events.processed_at

        Args:
            canonical_schema: Filter by canonical_schema column
            filters: Additional column filters
            measurement_table: Name of measurement events table
            limit: Max records to return

        Yields:
            Dict with idem_key, payload, and metadata columns
        """
        ...

    def insert_measurements(
        self,
        measurements: list[dict[str, Any]],
        table: str,
        correlation_id: str,
    ) -> int:
        """Insert measurement events.

        Args:
            measurements: List of measurement events to insert
            table: Target table name
            correlation_id: Correlation ID for tracing

        Returns:
            Number of rows inserted
        """
        ...

    def insert_observations(
        self,
        observations: list[dict[str, Any]],
        table: str,
        correlation_id: str,
    ) -> int:
        """Insert observations.

        Args:
            observations: List of observations to insert
            table: Target table name
            correlation_id: Correlation ID for tracing

        Returns:
            Number of rows inserted
        """
        ...

    def upsert_measurements(
        self,
        measurements: list[dict[str, Any]],
        table: str,
        correlation_id: str,
    ) -> int:
        """Upsert measurement events using MERGE by idem_key.

        Inserts new rows or updates existing rows based on idem_key.
        This provides idempotent writes for re-processing.

        Args:
            measurements: List of measurement events to upsert
            table: Target table name
            correlation_id: Correlation ID for tracing

        Returns:
            Number of rows affected (inserted + updated)
        """
        ...

    def upsert_observations(
        self,
        observations: list[dict[str, Any]],
        table: str,
        correlation_id: str,
    ) -> int:
        """Upsert observations using MERGE by idem_key.

        Inserts new rows or updates existing rows based on idem_key.
        This provides idempotent writes for re-processing.

        Args:
            observations: List of observations to upsert
            table: Target table name
            correlation_id: Correlation ID for tracing

        Returns:
            Number of rows affected (inserted + updated)
        """
        ...


@runtime_checkable
class EventClient(Protocol):
    """Protocol for event emission.

    Wraps the existing event_client module.
    """

    def log_event(
        self,
        event_type: str,
        source_system: str,
        correlation_id: str,
        status: str,
        connection_name: str | None = None,
        target_object_type: str | None = None,
        payload: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> None:
        """Log an event to event_log.

        Args:
            event_type: Event type (e.g., "ingest.completed")
            source_system: Source system identifier
            correlation_id: Run ID for tracing
            status: Status ("ok" or "failed")
            connection_name: Optional connection name
            target_object_type: Optional object type
            payload: Optional telemetry payload
            error_message: Optional error message for failures
        """
        ...


@runtime_checkable
class Processor(Protocol):
    """Protocol for job processors.

    Each processor handles a specific job_type (ingest, canonize, final_form).
    """

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute the job.

        Args:
            job_spec: Parsed job specification
            context: Execution context with shared clients
            storage_client: Storage operations interface
            event_client: Event emission interface

        Raises:
            Exception: On job failure (will be caught and logged by runner)
        """
        ...


class ProcessorRegistry:
    """Registry for dispatching jobs to processors by job_type."""

    def __init__(self) -> None:
        self._processors: dict[str, Processor] = {}

    def register(self, job_type: str, processor: Processor) -> None:
        """Register a processor for a job type.

        Args:
            job_type: Job type identifier (e.g., "ingest")
            processor: Processor instance
        """
        self._processors[job_type] = processor

    def get(self, job_type: str) -> Processor:
        """Get processor for a job type.

        Args:
            job_type: Job type identifier

        Returns:
            Processor instance

        Raises:
            KeyError: If job_type not registered
        """
        if job_type not in self._processors:
            raise KeyError(f"Unknown job_type: {job_type}. Registered: {list(self._processors.keys())}")
        return self._processors[job_type]

    def list_types(self) -> list[str]:
        """List registered job types."""
        return list(self._processors.keys())


# Global registry instance
registry = ProcessorRegistry()
