<!-- BEGIN SYNCED: lorchestra -->
# Lorchestra Project Context

## Description

Lightweight job orchestrator for PHI data pipelines. Loads job specs (JSON/YAML) and dispatches to typed processors and callable handlers for ingest, canonize, project, and sync operations.

## Invariants

- Libraries (injest, canonizer, final-form) are pure transforms with no IO.
- All IO happens in the processor layer via StorageClient and EventClient.
- BigQuery is the primary storage using two-table pattern (event_log + raw_objects).
- Jobs are defined as JSON/YAML specs in jobs/definitions/, not code.
- Processors receive clients, never raw credentials.

## Boundaries

### cli
- Type: inbound
- Contract: lorchestra <command> [args]
- Consumers: operator (human), life CLI, cron

### python_api
- Type: inbound
- Contract: run_job(job_id, **options) -> JobResult
- Consumers: life orchestrator, test harness

### injest
- Type: dependency
- Contract: injest.get_stream(name) -> SourceStream
- Requires: injest package, AUTHCTL_HOME

### canonizer
- Type: dependency
- Contract: canonizer transforms
- Requires: canonizer package, canonizer_registry_root config

### bigquery
- Type: outbound

### sqlite
- Type: outbound
- Contract: Local SQLite sync for offline queries
- Requires: sqlite_path config

### local_files
- Type: outbound
- Contract: Markdown file projections per client
- Requires: local_views_root config

## Architecture Decisions

### adr-001: JSON/YAML job specs over code
**Status:** accepted

**Rationale:** Declarative specs enable inspection, validation, and generation without executing code.

**Decision:** All job definitions live in jobs/definitions/ as JSON or YAML files dispatched by job_type.

### adr-002: Typed processor registry
**Status:** accepted

**Rationale:** Central dispatch enables job_type validation and discovery.

**Decision:** Processors register via ProcessorRegistry; job_runner dispatches by job_type.

### adr-003: Two-table event pattern
**Status:** accepted

**Rationale:** Separate audit trail from object state for debugging and replay.

**Decision:** event_log captures all events; raw_objects/canonical_objects store latest state.

### adr-004: Client injection over credential passing
**Status:** accepted

**Rationale:** Processors should not handle auth; simplifies testing and security.

**Decision:** JobContext provides pre-authenticated clients; processors never see credentials.

### adr-005: Read-only SQL gate
**Status:** accepted

**Rationale:** Prevent accidental mutations when running ad-hoc queries.

**Decision:** sql_runner.py validates queries are SELECT/WITH only before execution.

## Running Jobs

**STORACLE_NAMESPACE_SALT**: Every `lorchestra run` requires `STORACLE_NAMESPACE_SALT` to be set (default: `storacle-dev`). Without it, the `storacle.submit` step silently no-ops and the job reports false SUCCESS. The CLI now sets this automatically (matching life's behavior), but be aware of it when calling lorchestra programmatically or in tests.

<!-- END SYNCED: lorchestra -->

## BigQuery Schema (Current — do not guess these names)

**GCP Project**: `local-orchestration`

**WAL (append-only source of truth)**
- Dataset: `wal`
- Table: `domain_events` — full qualified: `local-orchestration.wal.domain_events`
- Columns: event_id, subject_id, occurred_at, ingested_at, event_type, aggregate_type, aggregate_id, producer_service, producer_component, correlation_id, payload (JSON), payload_sha256, schema_version
- Purpose: Immutable audit log of all domain object synthesis/updates

**Domain Objects (read-side VIEW)**
- Dataset: `domain`
- View: `objects` — full qualified: `local-orchestration.domain.objects`
- Columns: aggregate_id, idem_key, entity_id, object_type, object_variant, content, model_used, inference_audit, synthesized_at, created_at
- Query pattern: latest event per aggregate_id WHERE event_type NOT LIKE '%.deleted'
- Purpose: Current state of each domain object (automatically reflects new wal.append events)

**Do NOT create or reference**: event_log, raw_objects, canonical_objects, or any table not listed here without checking storacle's actual BQ client config.

**Write Pattern**
- All BQ writes go through `storacle.wal.append`. Never write to BigQuery directly.
- `aggregate_id` format: `{entity_id}:{object_variant}` (e.g., `sess_abc123:evidence`)
- `event_id` is deterministic (SHA256 of envelope) — same inputs → DUPLICATE status, no actual insert
- Idempotency: re-running same job with same inputs produces same event_id; storacle.wal.append returns DUPLICATE without duplicating the row
