# Implementation Plan: e005-03-handlers

## Goal

Transform lorchestra into a pure orchestration layer with clean handler abstractions, enforcing strict boundaries:
- **lorchestra** orchestrates (JobDef → JobInstance → RunRecord → StepManifest)
- **storacle** handles data-plane operations (query.*, write.*, assert.*)
- **compute** handles external IO (compute.llm, compute.transform, compute.extract, compute.render)

## Current State Analysis

### Boundary Violations Identified
1. `processors/base.py:12` - Direct `from google.cloud import bigquery` import
2. `processors/base.py:31` - `JobContext.bq_client: bigquery.Client` field
3. `cli.py:63` - Lazy BigQuery import inside `_run_job_impl()`
4. `stack_clients/event_client.py` - Direct BQ writes (acceptable as IO layer)

### Current Architecture
- `Backend` abstract class in `executor.py:184` with single `execute(manifest)` method
- `NoOpBackend` implementation for testing
- Three backend types: `data_plane`, `compute`, `orchestration`
- `Op.backend` property maps ops to backend names

### Missing Pieces
- No concrete `DataPlaneHandler` implementation
- No `ComputeHandler` implementation
- No `OrchestrationHandler` for `job.run` sub-job execution
- No handler registry beyond simple dict in Executor
- Direct BQ coupling in processors/base.py violates boundaries

---

## Implementation Phases

### Phase 1: Define Storacle Client Interface

**File:** `lorchestra/handlers/storacle_client.py`

Create a protocol-based interface for storacle operations that the DataPlaneHandler will use:

```python
from typing import Protocol, Any, Iterator

class StoracleClient(Protocol):
    """Interface for data-plane operations via storacle."""

    def query_raw_objects(
        self,
        source_system: str,
        object_type: str,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query raw objects."""
        ...

    def query_canonical_objects(
        self,
        canonical_schema: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query canonical objects."""
        ...

    def query_last_sync(
        self,
        source_system: str,
        connection_name: str,
        object_type: str,
    ) -> datetime | None:
        """Get last sync timestamp for incremental sync."""
        ...

    def upsert(
        self,
        table: str,
        objects: list[dict[str, Any]],
        idem_key_field: str,
    ) -> dict[str, int]:
        """Upsert objects by idem_key. Returns {inserted: N, updated: M}."""
        ...

    def insert(
        self,
        table: str,
        objects: list[dict[str, Any]],
    ) -> int:
        """Insert objects. Returns row count."""
        ...

    def delete(
        self,
        table: str,
        filters: dict[str, Any],
    ) -> int:
        """Delete objects matching filters. Returns row count."""
        ...

    def merge(
        self,
        table: str,
        objects: list[dict[str, Any]],
        merge_keys: list[str],
    ) -> dict[str, int]:
        """MERGE operation. Returns {inserted: N, updated: M, deleted: D}."""
        ...

    def assert_rows(
        self,
        table: str,
        filters: dict[str, Any],
        expected_count: int | None = None,
        min_count: int | None = None,
        max_count: int | None = None,
    ) -> bool:
        """Assert row count constraints."""
        ...

    def assert_schema(
        self,
        table: str,
        expected_columns: list[dict[str, str]],
    ) -> bool:
        """Assert table schema matches expectations."""
        ...

    def assert_unique(
        self,
        table: str,
        columns: list[str],
    ) -> bool:
        """Assert columns form a unique key."""
        ...
```

**Implementation note:** Initially provide a `NoOpStoracleClient` that returns mock data, and a `StoracleRpcClient` that calls `storacle.rpc.execute_plan()`.

---

### Phase 2: Implement Data Plane Handler

**File:** `lorchestra/handlers/data_plane.py`

```python
from lorchestra.handlers.base import Handler
from lorchestra.handlers.storacle_client import StoracleClient
from lorchestra.schemas import StepManifest, Op

class DataPlaneHandler(Handler):
    """
    Handler for data-plane operations (query.*, write.*, assert.*).

    Delegates to storacle via StoracleClient interface.
    """

    def __init__(self, client: StoracleClient):
        self._client = client

    def execute(self, manifest: StepManifest) -> dict[str, Any]:
        """Dispatch to appropriate storacle operation based on Op."""
        op = manifest.op
        params = manifest.resolved_params

        if op == Op.QUERY_RAW_OBJECTS:
            return self._query_raw_objects(params)
        elif op == Op.QUERY_CANONICAL_OBJECTS:
            return self._query_canonical_objects(params)
        elif op == Op.QUERY_LAST_SYNC:
            return self._query_last_sync(params)
        elif op == Op.WRITE_UPSERT:
            return self._write_upsert(params)
        # ... etc for all data_plane ops

        raise ValueError(f"Unsupported data_plane op: {op}")

    def _query_raw_objects(self, params: dict) -> dict:
        results = list(self._client.query_raw_objects(
            source_system=params["source_system"],
            object_type=params["object_type"],
            filters=params.get("filters"),
            limit=params.get("limit"),
        ))
        return {"rows": results, "count": len(results)}

    # ... similar methods for other ops
```

---

### Phase 3: Implement Compute Handler

**File:** `lorchestra/handlers/compute.py`

```python
from typing import Protocol, Any

class ComputeClient(Protocol):
    """Interface for compute operations."""

    def llm_invoke(
        self,
        prompt: str,
        model: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> dict[str, Any]:
        """Invoke LLM and return response."""
        ...

    def transform(
        self,
        input_data: Any,
        transform_ref: str,
    ) -> Any:
        """Apply a registered transform."""
        ...

    def extract(
        self,
        source: str,
        extractor_ref: str,
    ) -> dict[str, Any]:
        """Extract structured data from source."""
        ...

    def render(
        self,
        template_ref: str,
        context: dict[str, Any],
    ) -> str:
        """Render a template with context."""
        ...


class ComputeHandler(Handler):
    """Handler for compute.* operations."""

    def __init__(self, client: ComputeClient):
        self._client = client

    def execute(self, manifest: StepManifest) -> dict[str, Any]:
        op = manifest.op
        params = manifest.resolved_params

        if op == Op.COMPUTE_LLM:
            return self._client.llm_invoke(
                prompt=params["prompt"],
                model=params.get("model"),
                temperature=params.get("temperature"),
                max_tokens=params.get("max_tokens"),
            )
        elif op == Op.COMPUTE_TRANSFORM:
            result = self._client.transform(
                input_data=params["input"],
                transform_ref=params["transform_ref"],
            )
            return {"output": result}
        elif op == Op.COMPUTE_EXTRACT:
            return self._client.extract(
                source=params["source"],
                extractor_ref=params["extractor_ref"],
            )
        elif op == Op.COMPUTE_RENDER:
            rendered = self._client.render(
                template_ref=params["template_ref"],
                context=params.get("context", {}),
            )
            return {"rendered": rendered}

        raise ValueError(f"Unsupported compute op: {op}")
```

---

### Phase 4: Implement Handler Registry

**File:** `lorchestra/handlers/registry.py`

```python
from typing import Any

from lorchestra.handlers.base import Handler
from lorchestra.schemas import Op

class HandlerRegistry:
    """
    Registry for handler dispatch by backend type.

    Maps backend names (data_plane, compute, orchestration) to Handler instances.
    """

    def __init__(self):
        self._handlers: dict[str, Handler] = {}

    def register(self, backend: str, handler: Handler) -> None:
        """Register a handler for a backend type."""
        self._handlers[backend] = handler

    def get(self, backend: str) -> Handler:
        """Get handler for a backend type."""
        if backend not in self._handlers:
            raise KeyError(f"No handler registered for backend: {backend}")
        return self._handlers[backend]

    def dispatch(self, manifest: StepManifest) -> dict[str, Any]:
        """Dispatch a manifest to the appropriate handler."""
        backend = manifest.backend
        handler = self.get(backend)
        return handler.execute(manifest)

    @classmethod
    def create_default(
        cls,
        storacle_client: StoracleClient | None = None,
        compute_client: ComputeClient | None = None,
    ) -> "HandlerRegistry":
        """Create a registry with default handlers."""
        registry = cls()

        # Data plane handler
        if storacle_client:
            from lorchestra.handlers.data_plane import DataPlaneHandler
            registry.register("data_plane", DataPlaneHandler(storacle_client))
        else:
            from lorchestra.handlers.base import NoOpHandler
            registry.register("data_plane", NoOpHandler())

        # Compute handler
        if compute_client:
            from lorchestra.handlers.compute import ComputeHandler
            registry.register("compute", ComputeHandler(compute_client))
        else:
            from lorchestra.handlers.base import NoOpHandler
            registry.register("compute", NoOpHandler())

        # Orchestration handler (for job.run)
        from lorchestra.handlers.orchestration import OrchestrationHandler
        registry.register("orchestration", OrchestrationHandler())

        return registry
```

---

### Phase 5: Refactor Executor to Use Handlers

**File:** `lorchestra/executor.py`

Changes:
1. Replace `backends: dict[str, Backend]` with `handlers: HandlerRegistry`
2. Update `_execute_step()` to use `handlers.dispatch(manifest)`
3. Update `execute()` and `execute_job()` signatures

```python
class Executor:
    def __init__(
        self,
        store: RunStore,
        handlers: HandlerRegistry | None = None,
        max_attempts: int = 1,
    ):
        self._store = store
        self._handlers = handlers or HandlerRegistry.create_default()
        self._max_attempts = max_attempts

    def _execute_step(
        self,
        step: JobStepInstance,
        run_id: str,
        step_outputs: dict[str, Any],
    ) -> tuple[Any, str, str]:
        # ... resolve refs, compute idempotency key, create manifest ...

        # Store manifest
        manifest_ref = self._store.store_manifest(manifest)

        # Dispatch via handler registry
        output = self._handlers.dispatch(manifest)

        # Store output
        output_ref = self._store.store_output(run_id, step.step_id, output)

        return output, manifest_ref, output_ref
```

**Backwards compatibility:** Keep `backends` parameter as deprecated, auto-convert to registry.

---

### Phase 6: Enforce Boundaries and Remove BQ Imports

**Changes:**

1. **`processors/base.py`:**
   - Remove `from google.cloud import bigquery`
   - Change `JobContext.bq_client` to accept a generic client interface
   - Or deprecate `JobContext` in favor of handler-based execution

2. **`cli.py`:**
   - Remove direct BigQuery client creation
   - Instead, create `HandlerRegistry` with configured clients
   - Pass registry to executor

3. **New boundary test:**
   ```python
   def test_orchestration_layer_no_bigquery():
       """Verify executor.py and handlers/ have NO BigQuery imports."""
       # Check executor.py, handlers/*.py for bigquery imports
       # Should all be clean
   ```

---

### Phase 7: Verify Integration and Boundaries

**Tests:**

1. **Unit tests:**
   - `test_data_plane_handler.py` - Test DataPlaneHandler with mock StoracleClient
   - `test_compute_handler.py` - Test ComputeHandler with mock ComputeClient
   - `test_handler_registry.py` - Test registry dispatch logic

2. **Integration tests:**
   - `test_executor_with_handlers.py` - Full execution flow with NoOp handlers
   - `test_executor_with_storacle.py` - Integration with real storacle (if available)

3. **Boundary tests:**
   - Update `test_boundaries.py` to verify:
     - `lorchestra/executor.py` has no BQ imports
     - `lorchestra/handlers/*.py` have no BQ imports
     - Only `stack_clients/` and `processors/` may have BQ imports (IO layer)

---

## File Structure After Implementation

```
lorchestra/
├── __init__.py
├── executor.py              # Updated to use HandlerRegistry
├── compiler.py              # No changes
├── registry.py              # No changes
├── run_store.py             # No changes
├── handlers/
│   ├── __init__.py          # Export Handler, HandlerRegistry
│   ├── base.py              # Handler protocol, NoOpHandler
│   ├── registry.py          # HandlerRegistry
│   ├── data_plane.py        # DataPlaneHandler
│   ├── compute.py           # ComputeHandler, ComputeClient
│   ├── orchestration.py     # OrchestrationHandler (for job.run)
│   └── storacle_client.py   # StoracleClient protocol, NoOpStoracleClient
├── schemas/                 # No changes
├── processors/              # Updated to remove BQ import
└── stack_clients/           # IO layer - BQ imports acceptable here
```

---

## Migration Path

1. **Phase 1-4:** Add new handlers module (additive, no breaking changes)
2. **Phase 5:** Update Executor with backwards-compatible `backends` → `handlers` conversion
3. **Phase 6:** Remove BQ imports from orchestration layer
4. **Phase 7:** Verify all tests pass, boundaries enforced

---

## Success Criteria

- [ ] `lorchestra/executor.py` has zero BigQuery imports
- [ ] `lorchestra/handlers/*.py` have zero BigQuery imports
- [ ] `lorchestra/compiler.py` has zero BigQuery imports
- [ ] All existing tests continue to pass
- [ ] New handler tests achieve >90% coverage of handler code
- [ ] Boundary test verifies clean separation
- [ ] `Op.backend` correctly routes all operations to handlers
