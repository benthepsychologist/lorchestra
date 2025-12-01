"""Tests for processor protocols and registry."""

import pytest
from unittest.mock import MagicMock
from typing import Any, Iterator

from lorchestra.processors import (
    EventClient,
    JobContext,
    Processor,
    ProcessorRegistry,
    StorageClient,
    UpsertResult,
    registry,
)


class TestUpsertResult:
    """Tests for UpsertResult dataclass."""

    def test_upsert_result_creation(self):
        """UpsertResult stores inserted and updated counts."""
        result = UpsertResult(inserted=10, updated=5)
        assert result.inserted == 10
        assert result.updated == 5


class TestJobContext:
    """Tests for JobContext dataclass."""

    def test_job_context_creation(self):
        """JobContext stores execution context."""
        bq_client = MagicMock()
        ctx = JobContext(
            bq_client=bq_client,
            run_id="test-run-123",
            dry_run=True,
            test_table=False,
        )
        assert ctx.bq_client is bq_client
        assert ctx.run_id == "test-run-123"
        assert ctx.dry_run is True
        assert ctx.test_table is False

    def test_job_context_defaults(self):
        """JobContext has sensible defaults."""
        bq_client = MagicMock()
        ctx = JobContext(bq_client=bq_client, run_id="test")
        assert ctx.dry_run is False
        assert ctx.test_table is False


class TestStorageClientProtocol:
    """Tests for StorageClient protocol."""

    def test_storage_client_is_protocol(self):
        """StorageClient can be used for isinstance checks."""

        class MockStorageClient:
            def query_objects(
                self,
                source_system: str,
                object_type: str,
                filters: dict[str, Any] | None = None,
                limit: int | None = None,
            ) -> Iterator[dict[str, Any]]:
                yield {"idem_key": "test", "payload": {}}

            def upsert_objects(self, objects, source_system, connection_name, object_type, idem_key_fn, correlation_id):
                return UpsertResult(inserted=1, updated=0)

            def update_field(self, idem_keys, field, value):
                return len(idem_keys)

            def insert_canonical(self, objects, correlation_id):
                return len(objects)

            def query_canonical(self, canonical_schema=None, filters=None, limit=None):
                yield {"idem_key": "test", "payload": {}}

            def insert_measurements(self, measurements, table, correlation_id):
                return len(measurements)

            def insert_observations(self, observations, table, correlation_id):
                return len(observations)

        client = MockStorageClient()
        assert isinstance(client, StorageClient)


class TestEventClientProtocol:
    """Tests for EventClient protocol."""

    def test_event_client_is_protocol(self):
        """EventClient can be used for isinstance checks."""

        class MockEventClient:
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
                pass

        client = MockEventClient()
        assert isinstance(client, EventClient)


class TestProcessorProtocol:
    """Tests for Processor protocol."""

    def test_processor_is_protocol(self):
        """Processor can be used for isinstance checks."""

        class MockProcessor:
            def run(self, job_spec, context, storage_client, event_client):
                pass

        proc = MockProcessor()
        assert isinstance(proc, Processor)


class TestProcessorRegistry:
    """Tests for ProcessorRegistry."""

    def test_register_and_get(self):
        """Registry can register and retrieve processors."""
        reg = ProcessorRegistry()

        class MockProcessor:
            def run(self, job_spec, context, storage_client, event_client):
                pass

        proc = MockProcessor()
        reg.register("test_type", proc)

        retrieved = reg.get("test_type")
        assert retrieved is proc

    def test_get_unknown_type_raises(self):
        """Registry raises KeyError for unknown job types."""
        reg = ProcessorRegistry()

        with pytest.raises(KeyError, match="Unknown job_type: unknown"):
            reg.get("unknown")

    def test_list_types(self):
        """Registry can list registered types."""
        reg = ProcessorRegistry()

        class MockProcessor:
            def run(self, job_spec, context, storage_client, event_client):
                pass

        reg.register("ingest", MockProcessor())
        reg.register("canonize", MockProcessor())

        types = reg.list_types()
        assert "ingest" in types
        assert "canonize" in types

    def test_global_registry_exists(self):
        """Global registry is available."""
        assert registry is not None
        assert isinstance(registry, ProcessorRegistry)
