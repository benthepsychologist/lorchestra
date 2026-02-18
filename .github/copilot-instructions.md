# Lorchestra - Copilot Instructions

## Overview

Lorchestra is a lightweight job orchestrator for PHI data pipelines. Jobs are defined as JSON/YAML specs in `lorchestra/jobs/definitions/` and dispatched to typed handlers for ingest, canonize, formation (clinical scoring), and projection operations.

**Core Principle**: Libraries (injest, canonizer, finalform) are pure transforms with no IO. All IO happens in the processor/handler layer via StorageClient and EventClient.

## Build, Test, and Lint

```bash
# Install
uv venv && source .venv/bin/activate
uv pip install -e .

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_compiler.py -v

# Run single test
pytest tests/test_compiler.py::test_function_name -v

# With coverage
pytest --cov=lorchestra tests/

# Lint (ruff)
ruff check .
ruff format .

# Format (black)
black lorchestra/ tests/
```

## High-Level Architecture

### Job Execution Flow

```
lorchestra run <job_id>
    │
    ├── JobRegistry loads JobDef from jobs/definitions/{job_id}.yaml
    │
    ├── Compiler creates JobInstance with resolved references
    │
    └── Executor dispatches steps by op type:
        ├── Native ops (handled directly):
        │   ├── call → dispatch to callable handler
        │   ├── plan.build → build StoraclePlan
        │   └── storacle.* → submit/query operations
        │
        └── Handler-dispatched ops (via HandlerRegistry):
            ├── compute.* → ComputeHandler (LLM service)
            └── job.* → OrchestrationHandler (nested jobs)
```

### Data Flow (External Sources → BigQuery → Local Files)

```
1. INGEST
   External Sources (Gmail, Exchange, Dataverse, Stripe, Google Forms)
       ↓
   injest library extracts → JSONL temporary files
       ↓
   lorchestra job reads JSONL → emits via EventClient
       ↓
   BigQuery two-table pattern:
       - event_log: Audit trail (immutable event envelopes)
       - raw_objects: State projection (deduped by idem_key)

2. CANONIZE
   raw_objects (source-specific schemas)
       ↓
   canonizer transforms (JSONata or Python)
       ↓
   canonical_objects (normalized: JMAP Lite email, form_response, etc.)

3. FORMATION (Clinical Scoring)
   canonical_objects (form_response)
       ↓
   finalform library (PHQ-9, GAD-7, IPIP-NEO, etc.)
       ↓
   measurement_events, observations (scored clinical data)

4. PROJECTION
   canonical_objects, measurement_events
       ↓
   BigQuery views → SQLite sync → Markdown files
       ↓
   ~/lifeos/local_views/ (client-specific reports)
```

### Storage: BigQuery Two-Table Pattern

All data flows through BigQuery as the primary storage:

**event_log** (Audit trail):
- Partitioned by date
- Records every job execution and event
- Fields: event_id, event_type, source_system, connection_name, target_object_type, correlation_id, created_at, status, payload

**raw_objects** (Deduped state):
- Clustered by source_system, connection_name, object_type
- One row per unique object (MERGE provides idempotency)
- Fields: idem_key, source_system, connection_name, object_type, external_id, payload, first_seen, last_seen

**canonical_objects** (Transformed data):
- Similar structure but with canonical schemas

## Key Conventions

### Job Definition Structure

Jobs are YAML or JSON files in `lorchestra/jobs/definitions/` organized by type:
- `ingest/` - Data extraction jobs
- `canonize/` - Transformation jobs
- `formation/` - Clinical scoring jobs
- `projection/` - Local file projection jobs
- `pm/` - Project management jobs
- `pipeline/` - Multi-step workflows

**Job Schema (v2.0)**:
```yaml
job_id: my_job           # Matches filename (without extension)
version: '2.0'
description: "..."       # Optional
steps:
- step_id: step_name
  op: call               # or storacle.query, compute.llm, job.*, plan.build, etc.
  params:
    # Parameters vary by op type
    callable: handler_name  # for 'call' op
    op: handler.operation   # handler-specific operation
    # ... other params
```

### Reference Resolution

Jobs support `@run.*` and `@payload.*` references:
- `@run.step_id.field` - Access output from previous step
- `@run.step_id.items[0]` - Access array elements
- `@payload.field` - Access job input payload

### Handler Pattern

Handlers implement domain-specific operations:
- **ComputeHandler**: LLM operations (`compute.llm`)
- **OrchestrationHandler**: Nested job execution (`job.*`)
- **Callables**: Registered functions in `lorchestra/callable/`

To add a new callable:
1. Define function in appropriate module under `lorchestra/callable/`
2. Register in `lorchestra/callable/__init__.py`
3. Use via `op: call` with `callable: your_handler_name`

### idem_key Pattern

Idempotency keys follow the format:
```
{source_system}:{connection_name}:{object_type}:{external_id}
```

Examples:
- `gmail:gmail-acct1:email:18c5a7b2e3f4d5c6`
- `dataverse:dataverse-clinic:session:739330df-...`

**Important**: Never parse idem_key. Use explicit columns (source_system, connection_name, object_type) for filtering.

### Client Injection

Processors/handlers receive pre-authenticated clients:
- **StorageClient** (via StoracleHandler): BigQuery read/write/query
- **EventClient**: Event emission with two-table pattern
- **ComputeClient**: LLM service calls

Never pass raw credentials to libraries or handlers. All authentication happens in the orchestrator layer.

### Error Handling

Steps support `continue_on_error: true` for fault-tolerant pipelines:
```yaml
- step_id: optional_enrichment
  op: call
  continue_on_error: true
  params:
    callable: enrichment_handler
```

## Daily Pipeline Scripts

Located in `scripts/`:
- `daily_ingest.sh` - Ingest from all external sources
- `daily_canonize.sh` - Transform raw → canonical
- `daily_formation.sh` - Score clinical instruments
- `daily_local_projection.sh` - Sync to local files
- `daily_molt_projection.sh` - Molt-based projections

These scripts orchestrate multiple `lorchestra run <job_id>` commands with proper error handling.

## Configuration

Configuration lives in `~/.lorchestra/config.yaml`:
```yaml
project: lifeos-dev           # GCP project
dataset_raw: raw              # Raw objects dataset
dataset_canonical: canonical  # Canonical objects dataset
dataset_derived: derived      # Derived tables/views
run_path: ~/.lorchestra/runs  # Execution logs
```

Initialize with: `lorchestra init`

## CLI Commands

```bash
# Run a job
lorchestra run <job_id> [--dry-run] [--payload '{"key": "value"}']

# List jobs
lorchestra jobs list [--type ingest|canonize|formation|...]

# Show job definition
lorchestra jobs show <job_id>

# Query predefined SQL
lorchestra query <query_name>

# Ad-hoc SQL (read-only)
lorchestra sql "SELECT ..."

# Pipeline stats
lorchestra stats raw          # Raw object counts
lorchestra stats canonical    # Canonical object counts
lorchestra stats jobs         # Recent executions
```

## Common Patterns

### Adding a New Ingest Job

1. Create job definition in `lorchestra/jobs/definitions/ingest/`:
```yaml
job_id: ingest_new_source
version: '2.0'
steps:
- step_id: ingest
  op: call
  params:
    callable: injest_stream
    stream: new_source.stream_name
    identity: provider:account
    sink:
      source_system: provider
      connection_name: provider-account
      object_type: object_type
```

2. Ensure injest package supports the stream
3. Run: `lorchestra run ingest_new_source`

### Adding a New Canonization Transform

1. Create transform in canonizer package
2. Create job in `lorchestra/jobs/definitions/canonize/`:
```yaml
job_id: canonize_new_source
version: '2.0'
steps:
- step_id: read_raw
  op: storacle.query
  params:
    dataset: raw
    table: raw_objects
    filters:
      source_system: provider
      object_type: object_type
- step_id: transform
  op: call
  params:
    callable: canonizer_transform
    transform_ref: provider/to_canonical@1.0.0
    items: '@run.read_raw.items'
```

### Adding a Formation (Scoring) Job

1. Ensure finalform supports the instrument
2. Create job in `lorchestra/jobs/definitions/formation/`:
```yaml
job_id: form_score_instrument
version: '2.0'
steps:
- step_id: read_responses
  op: storacle.query
  params:
    dataset: canonical
    table: canonical_objects
    filters:
      canonical_schema: form_response
      form_id: instrument_id
- step_id: score
  op: call
  params:
    callable: finalform_score
    instrument: instrument_name
    items: '@run.read_responses.items'
```

## Dependencies

External packages (separate repos, editable installs):
- **injest**: Meltano wrapper for data extraction
- **authctl**: Credential management
- **canonizer**: Transform registry and execution
- **finalform**: Clinical instrument scoring
- **projectionist**: Local file projection
- **storacle**: BigQuery client abstraction
- **egret**: Event client for two-table pattern

All dependencies are specified in `pyproject.toml` with local editable paths in `[tool.uv.sources]`.

## Testing Notes

- Tests use pytest fixtures for BQ clients and mock data
- Many tests require GCP credentials (set `GOOGLE_APPLICATION_CREDENTIALS`)
- Integration tests may be slow (they hit real BigQuery)
- Use `pytest -m "not slow"` to skip integration tests (if markers are configured)

## Important: Boundaries to Respect

1. **Pure Libraries**: injest, canonizer, finalform must remain IO-free
2. **Client Injection**: Never pass credentials, always pass authenticated clients
3. **Job Specs**: Don't add Python code as jobs; keep specs declarative
4. **Two-Table Pattern**: Maintain separation of audit (event_log) and state (raw_objects)
5. **idem_key Opacity**: Never parse or construct idem_keys outside event_client
