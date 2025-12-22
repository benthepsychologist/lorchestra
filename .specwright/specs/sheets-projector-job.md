---
version: "0.1"
tier: B
title: "Sheets Projection Job (via ProjectionistProcessor)"
owner: benthepsychologist
goal: Add a new ProjectionistProcessor job type that projects BQ views to Google Sheets via projectionist
labels: [processor, projections, sheets, projectionist]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-18
updated: 2025-12-18
orchestrator_contract: "standard"
repo:
  working_branch: "feat/sheets-projector-job"
---

# Sheets Projection Job (via ProjectionistProcessor)

## Objective

> Add a new `projectionist` job type (handled by a new `ProjectionistProcessor`) that can run projectionist projections. This spec uses it to project BQ views to Google Sheets.

Legacy note:
- Existing projection processors remain as legacy and will be replaced job-by-job.
- Do NOT extend/refactor legacy processors in this spec.

## Acceptance Criteria

- [ ] `projectionist` job type registered and working
- [ ] Uses projectionist `SheetsProjection` for plan/apply semantics
- [ ] Uses storacle `SheetsWriteService` wrapping gorch.SheetsClient (lorchestra must not import gorch)
- [ ] Job definition uses source/sink structure similar to `sync_sqlite`, plus explicit projectionist projection name
- [ ] Events logged: `sheets_projection.started`, `sheets_projection.completed`, `sheets_projection.failed`
- [ ] Auth via existing `gdrive` authctl account
- [ ] CI green (ruff + pytest)
- [ ] Unit tests with mocked services

## Context

### Background

We have `sync_sqlite` for local SQLite sync. Now we need the same pattern for Google Sheets dashboards.

The key difference: Sheets projection uses **projectionist** for semantics:
- `plan()` generates a deterministic, hashable plan with all values materialized
- `apply()` executes the plan via injected services

This gives us dry-run, auditability, and replay for free.

### Architecture

### Repo Scope (IMPORTANT)

This spec spans multiple workspace packages and may modify files outside the lorchestra repo:
- `/workspace/lorchestra` (processor + job definition)
- `/workspace/projectionist` (projection semantics)
- `/workspace/storacle` (Sheets write adapter)
- `/workspace/gorch` (SheetsClient helper)

If your agent runner is restricted to a single repo, split this into one spec per repo.

```
life (YAML job)
  → lorchestra (processor + services construction)
    → projectionist (plan + apply semantics)
      → storacle.SheetsWriteService (wraps gorch)
        → gorch.SheetsClient (Google Sheets API)
```

Responsibility boundaries:
- **gorch**: Raw Sheets API client (auth delegated to caller)
- **storacle**: Service adapter implementing projectionist protocols
- **lorchestra**: Processor that constructs services and invokes projectionist
- **projectionist**: Owns semantics (query, column order, header generation)

### Constraints

- No Google API code in lorchestra or projectionist
- Auth via existing `gdrive` authctl account (has Sheets scope)
- Job JSON is the registry (spreadsheet_id in job definition, not config file)
- Projectionist remains environment-agnostic (no env vars, no hardcoded IDs)
- New processor is additive; legacy processors remain unchanged in this spec

## Job Definition

```json
{
    "job_id": "proj_sheets_proj_clients",
    "job_type": "projectionist",
    "projection": {
        "name": "sheets"
    },
  "source": {
    "projection": "proj_clients"
  },
  "sink": {
        "kind": "sheets",
    "spreadsheet_id": "1abc...",
    "sheet_name": "clients",
    "strategy": "replace"
  }
}
```

Mostly matches `sync_sqlite` structure:
- `source.projection`: BQ view name (processor builds fully qualified query)
- `sink.spreadsheet_id`: Google Sheets spreadsheet ID
- `sink.sheet_name`: Target sheet/tab name
- `sink.strategy`: `"replace"` (clear + write) or `"append"`
Plus:
- `projection.name`: projectionist projection name (e.g. `"sheets"`)
- `sink.kind`: identifies sink family (currently `"sheets"`)

## Plan

### Step 1: Extend gorch.SheetsClient [G1: gorch Ready]

**Role:** agentic

**Prompt:**

Add `clear_and_update()` method to `gorch/src/gorch/sheets/client.py`:

```python
def clear_and_update(
    self,
    spreadsheet_id: str,
    sheet_name: str,
    values: list[list[Any]],
    *,
    value_input_option: str = "RAW",
) -> int:
    """Clear sheet and write new values (replace strategy).

    Args:
        spreadsheet_id: The spreadsheet ID.
        sheet_name: The sheet/tab name.
        values: 2D list of values (header row is values[0]).
        value_input_option: How to interpret input.

    Returns:
        Number of data rows written (excludes header).
    """
    self.clear_values(spreadsheet_id, sheet_name)
    self.update_values(spreadsheet_id, f"{sheet_name}!A1", values, value_input_option=value_input_option)
    return len(values) - 1 if values else 0
```

**Allowed Paths:**

- `src/gorch/sheets/client.py`
- `tests/test_sheets_client.py`

**Verification Commands:**

```bash
ruff check src/
pytest tests/test_sheets_client.py -v
```

**Outputs:**

- `src/gorch/sheets/client.py` (updated)
- `tests/test_sheets_client.py` (updated)

---

### Step 2: Add SheetsWriteService to storacle [G2: storacle Ready]

**Role:** agentic

**Prompt:**

Create `storacle/src/storacle/clients/sheets.py`:

```python
"""Sheets write service wrapping gorch.SheetsClient."""

from typing import Any, Literal

from gorch import SheetsClient


class SheetsWriteService:
    """Service adapter implementing projectionist's SheetsWriteService protocol.

    Wraps gorch.SheetsClient to provide the write_table interface.
    """

    def __init__(self, sheets_client: SheetsClient):
        self._client = sheets_client

    def write_table(
        self,
        *,
        spreadsheet_id: str,
        sheet_name: str,
        values: list[list[Any]],
        strategy: Literal["replace", "append"],
    ) -> int:
        """Write a table to a sheet.

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID.
            sheet_name: The name of the sheet/tab.
            values: 2D list of values, header row is values[0].
            strategy: "replace" clears and rewrites; "append" adds rows.

        Returns:
            Number of rows written.
        """
        if strategy == "replace":
            return self._client.clear_and_update(spreadsheet_id, sheet_name, values)
        else:  # append
            # IMPORTANT: projectionist includes a header row at values[0].
            # In append mode we skip the header to avoid duplicating it on each run.
            data_rows = values[1:] if values else []
            if not data_rows:
                return 0
            self._client.append_values(
                spreadsheet_id,
                f"{sheet_name}!A1",
                data_rows,
            )
            return len(data_rows)

    @classmethod
    def from_authctl(cls, account: str = "gdrive") -> "SheetsWriteService":
        """Create service using authctl for authentication.

        Args:
            account: authctl account name (default: gdrive)
        """
        client = SheetsClient.from_authctl(account)
        return cls(client)
```

Export from `storacle/clients/__init__.py` (and optionally re-export from `storacle/__init__.py`).

Add `gorch` as dependency in `storacle/pyproject.toml`.

**Allowed Paths:**

- `src/storacle/clients/__init__.py` (create if needed)
- `src/storacle/clients/sheets.py`
- `src/storacle/__init__.py`
- `pyproject.toml`
- `tests/test_sheets_service.py`

**Verification Commands:**

```bash
ruff check src/
pytest tests/test_sheets_service.py -v
```

**Outputs:**

- `src/storacle/clients/sheets.py` (new)
- `tests/test_sheets_service.py` (new)

---

### Step 3: Implement ProjectionistProcessor (sheets enabled) [G3: lorchestra Ready]

**Role:** agentic

**Prompt:**

Create `lorchestra/lorchestra/processors/projectionist.py`:

```python
"""Projectionist processor: orchestrator glue for projectionist projections.

This is the new, additive processor that will replace legacy projection processors job-by-job.
"""

import logging
from typing import Any, Iterator

from projectionist.context import ProjectionContext
from projectionist.projections import SheetsProjection
from storacle.clients.sheets import SheetsWriteService

from lorchestra.processors import registry
from lorchestra.processors.base import EventClient, JobContext, StorageClient

logger = logging.getLogger(__name__)


class BqQueryServiceAdapter:
    """Adapter wrapping StorageClient as projectionist BqQueryService."""

    def __init__(self, storage_client: StorageClient):
        self._storage = storage_client

    def query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        rows = self._storage.query_to_dataframe(sql)
        yield from rows


class ProjectionServicesImpl:
    """Concrete implementation of ProjectionServices for lorchestra."""

    def __init__(
        self,
        bq_service: BqQueryServiceAdapter,
        sheets_service: SheetsWriteService,
    ):
        self._bq = bq_service
        self._sheets = sheets_service

    @property
    def bq(self) -> BqQueryServiceAdapter:
        return self._bq

    @property
    def sheets(self) -> SheetsWriteService:
        return self._sheets


class ProjectionistProcessor:
    """Run projectionist projections based on job JSON."""

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        source = job_spec["source"]
        sink = job_spec["sink"]
        projection_spec = job_spec.get("projection", {})
        projection_name = projection_spec.get("name")
        if projection_name != "sheets":
            raise ValueError(
                f"Unsupported projectionist projection: {projection_name}. TODO: add mapping."
            )
        proj_name = source["projection"]
        source_system = job_spec.get("source_system", "lorchestra")

        # Build fully qualified query
        project = context.config.project
        dataset = context.config.dataset_canonical
        query = f"SELECT * FROM `{project}.{dataset}.{proj_name}`"

        # Log start
        event_client.log_event(
            event_type="sheets_projection.started",
            source_system=source_system,
            correlation_id=context.run_id,
            status="success",
            payload={
                "projection_name": proj_name,
                "spreadsheet_id": sink["spreadsheet_id"],
                "sheet_name": sink["sheet_name"],
            },
        )

        try:
            # Build services
            bq_service = BqQueryServiceAdapter(storage_client)
            # IMPORTANT: keep gorch/authctl usage out of lorchestra; storacle owns the adapter.
            sheets_service = SheetsWriteService.from_authctl("gdrive")
            services = ProjectionServicesImpl(bq_service, sheets_service)

            # Build context
            ctx = ProjectionContext(
                dry_run=context.dry_run,
                run_id=context.run_id,
                config={
                    "query": query,
                    "spreadsheet_id": sink["spreadsheet_id"],
                    "sheet_name": sink["sheet_name"],
                    "strategy": sink.get("strategy", "replace"),
                },
            )

            # Plan + Apply
            projection = SheetsProjection()
            plan = projection.plan(services, ctx)
            result = projection.apply(plan, services, ctx)

            # Log completion
            event_client.log_event(
                event_type="sheets_projection.completed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="success",
                payload={
                    "projection_name": proj_name,
                    "plan_id": result.plan_id,
                    "rows_affected": result.rows_affected,
                    "duration_seconds": result.duration_seconds,
                    "dry_run": result.dry_run,
                },
            )

        except Exception as e:
            event_client.log_event(
                event_type="sheets_projection.failed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={"projection_name": proj_name},
            )
            raise


registry.register("projectionist", ProjectionistProcessor())
```

Add job definition:

```json
// lorchestra/lorchestra/jobs/definitions/projection/sync/proj_sheets_proj_clients.json
{
    "job_id": "proj_sheets_proj_clients",
    "job_type": "projectionist",
    "projection": {
        "name": "sheets"
    },
  "source": {
    "projection": "proj_clients"
  },
  "sink": {
        "kind": "sheets",
    "spreadsheet_id": "YOUR_SPREADSHEET_ID",
    "sheet_name": "clients",
    "strategy": "replace"
  }
}
```

Add dependencies to `lorchestra/pyproject.toml`:
- `projectionist`
- `storacle`

**Allowed Paths:**

- `lorchestra/lorchestra/processors/projectionist.py`
- `lorchestra/lorchestra/processors/__init__.py`
- `lorchestra/lorchestra/jobs/definitions/projection/sync/`
- `pyproject.toml`
- `tests/test_sheets_projection_processor.py`

**Verification Commands:**

```bash
ruff check lorchestra/
pytest tests/test_sheets_projection_processor.py -v
lorchestra run proj_sheets_proj_clients --dry-run
```

**Outputs:**

- `lorchestra/lorchestra/processors/projectionist.py` (new)
- `lorchestra/lorchestra/jobs/definitions/projection/sync/proj_sheets_proj_clients.json` (new)
- `tests/test_sheets_projection_processor.py` (new)

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────┐
│                    lorchestra (processor)                     │
│  ProjectionistProcessor                                       │
│    - constructs services from storacle + gorch                │
│    - calls projectionist plan() then apply()                  │
│    - logs events                                              │
└───────────────────────────────┬─────────────────────────────┘
                                │ injects services
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                   projectionist (semantics)                   │
│  SheetsProjection.plan()                                      │
│    - executes BQ query via services.bq                        │
│    - builds 2D tuple with header at values[0]                 │
│    - returns finalized ProjectionPlan                         │
│  SheetsProjection.apply()                                     │
│    - calls services.sheets.write_table()                      │
│    - returns ProjectionResult                                 │
└───────────────────────────────┬─────────────────────────────┘
                                │ capability calls
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                storacle.SheetsWriteService                    │
│    - wraps gorch.SheetsClient                                 │
│    - implements write_table(strategy="replace"|"append")      │
└───────────────────────────────┬─────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                   gorch.SheetsClient                          │
│    - Google Sheets API v4                                     │
│    - auth via authctl (gdrive account)                        │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
[BQ: proj_clients view]
    ↓
SheetsProjectionProcessor (lorchestra)

(Implemented via ProjectionistProcessor in this spec)
    ↓
SheetsProjection.plan() (projectionist)
  - SELECT * FROM proj_clients
  - materializes all values
  - returns ProjectionPlan with plan_id
    ↓
SheetsProjection.apply() (projectionist)
    ↓
SheetsWriteService.write_table() (storacle)
    ↓
SheetsClient.clear_and_update() (gorch)
    ↓
[Google Sheets: spreadsheet/clients tab]
```

### Event Payloads

```python
# sheets_projection.started
{
    "projection_name": "proj_clients",
    "spreadsheet_id": "1abc...",
    "sheet_name": "clients",
}

# sheets_projection.completed
{
    "projection_name": "proj_clients",
    "plan_id": "sha256...",
    "rows_affected": 42,
    "duration_seconds": 1.23,
    "dry_run": False,
}

# sheets_projection.failed
{
    "projection_name": "proj_clients",
    "error": "...",
}
```

## Dependencies

```toml
# lorchestra/pyproject.toml (matches this repo's dependency style)
dependencies = [
    # ...
    "projectionist @ file:///workspace/projectionist",
    "storacle @ file:///workspace/storacle",
]

# storacle/pyproject.toml
dependencies = [
    # ...
    "gorch @ file:///workspace/gorch",
]
```

## Models & Tools

**Tools:** bash, pytest, ruff

**Models:** sonnet (implementation), haiku (tests)

## Repository

**Branch:** `feat/sheets-projector-job`

**Merge Strategy:** squash
