---
version: "0.1"
tier: C
title: Hardcoded Projections v0
owner: benthepsychologist
goal: Add hardcoded BQ views + SQLite sync + file projections for therapist surface
labels: [processor, projections, bigquery, sqlite]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-04T14:00:00.000000+00:00
updated: 2025-12-04T14:00:00.000000+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/projections-v0"
---

# Hardcoded Projections v0

## Objective

> Add hardcoded BQ views, SQLite sync, and file projections to give you a usable therapist surface today. No registry, no codegen - just inline SQL and simple jobs.

## Acceptance Criteria

- [ ] `proj_client_sessions` BQ view created with inline SQL
- [ ] `sync_sqlite` job type syncs BQ projections → local SQLite
- [ ] `file_projection` job type renders SQLite data → markdown files
- [ ] All projection SQL lives in one place: `lorchestra/sql/projections.py`
- [ ] Naming follows convention: `proj_<domain>_<entity>`
- [ ] Events logged: `projection.started`, `projection.completed`, `projection.failed`
- [ ] CI green (ruff + pytest)

## Context

### Background

The pipeline is solid:
- `ingest` → `raw_objects`
- `canonize` → `canonical_objects`
- `final_form` → `measurement_events` / `observations`

Everything after that is **read-only sugar**. You don't need a cathedral to render/slice read-only data.

### What we're building (v0)

1. **Hardcoded BQ views** - SQL string constants in `lorchestra/sql/projections.py`
2. **SQLite sync job** - `SELECT * FROM proj_*` → overwrite local SQLite tables
3. **File projection job** - Query SQLite → render markdown files to disk

### Future-proofing (for eventual projection-assistant)

1. **All SQL in one place** - `lorchestra/sql/projections.py` (easy to migrate later)
2. **Consistent naming** - `proj_client_sessions`, `proj_client_timeline`, etc.
3. **Event logging** - Same `projection.*` events we'd use with a registry

### Constraints

- No external dependencies (no projection-assistant)
- No YAML configs or registries
- Inline SQL is fine - this is intentionally simple

## Projection SQL

All projections live in `lorchestra/sql/projections.py`:

```python
"""Hardcoded projection SQL for v0.

All projection views are defined here as string constants.
When projection-assistant exists, these move to .projections/*.yaml.

Prerequisites: 2-0-0 canonical schemas with source_id, source_entity fields.
"""

# Therapy clients only (contacts with client_type_label = 'Therapy')
PROJ_THERAPY_CLIENTS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.proj_therapy_clients` AS
SELECT
  JSON_VALUE(payload, '$.source_id') AS contact_source_id,
  JSON_VALUE(payload, '$.contact_id') AS contact_id,
  JSON_VALUE(payload, '$.first_name') AS first_name,
  JSON_VALUE(payload, '$.last_name') AS last_name,
  CONCAT(
    JSON_VALUE(payload, '$.first_name'), '_',
    UPPER(SUBSTR(JSON_VALUE(payload, '$.last_name'), 1, 1))
  ) AS client_folder_name,
  JSON_VALUE(payload, '$.email') AS email,
  JSON_VALUE(payload, '$.client_type_code') AS client_type_code,
  JSON_VALUE(payload, '$.client_type_label') AS client_type_label,
  idem_key,
  canonicalized_at
FROM `{project}.{dataset}.canonical_objects`
WHERE canonical_schema = 'iglu:org.canonical/contact/jsonschema/2-0-0'
  AND JSON_VALUE(payload, '$.client_type_label') = 'Therapy'
"""

# Sessions with client info joined
PROJ_CLIENT_SESSIONS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.proj_client_sessions` AS
SELECT
  -- Session identifiers (for writeback)
  JSON_VALUE(s.payload, '$.source_id') AS session_source_id,
  JSON_VALUE(s.payload, '$.session_id') AS session_id,

  -- Client info (joined from therapy clients)
  c.contact_source_id,
  c.first_name AS client_first_name,
  c.last_name AS client_last_name,
  c.client_folder_name,

  -- Session metadata
  JSON_VALUE(s.payload, '$.subject') AS subject,
  JSON_VALUE(s.payload, '$.status') AS status,
  CAST(JSON_VALUE(s.payload, '$.scheduled_start') AS TIMESTAMP) AS scheduled_start,
  CAST(JSON_VALUE(s.payload, '$.scheduled_end') AS TIMESTAMP) AS scheduled_end,
  CAST(JSON_VALUE(s.payload, '$.actual_start') AS TIMESTAMP) AS actual_start,
  CAST(JSON_VALUE(s.payload, '$.actual_end') AS TIMESTAMP) AS actual_end,
  CAST(JSON_VALUE(s.payload, '$.duration_minutes') AS INT64) AS duration_minutes,
  JSON_VALUE(s.payload, '$.session_type') AS session_type,

  -- Timestamps
  s.canonicalized_at,
  s.idem_key AS session_idem_key
FROM `{project}.{dataset}.canonical_objects` s
JOIN `{project}.{dataset}.proj_therapy_clients` c
  ON JSON_VALUE(s.payload, '$.contact_id') = c.contact_id
WHERE s.canonical_schema = 'iglu:org.canonical/clinical_session/jsonschema/2-0-0'
"""

# Clinical documents (notes, summaries) linked to sessions
PROJ_SESSION_DOCUMENTS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.proj_session_documents` AS
SELECT
  -- Document identifiers (for writeback)
  JSON_VALUE(d.payload, '$.source_id') AS document_source_id,
  JSON_VALUE(d.payload, '$.source_entity') AS document_source_entity,
  JSON_VALUE(d.payload, '$.session_source_id') AS session_source_id,

  -- Document metadata
  JSON_VALUE(d.payload, '$.document_id') AS document_id,
  JSON_VALUE(d.payload, '$.document_type') AS document_type,
  JSON_VALUE(d.payload, '$.status') AS status,
  JSON_VALUE(d.payload, '$.title') AS title,
  CAST(JSON_VALUE(d.payload, '$.document_date') AS TIMESTAMP) AS document_date,

  -- Content
  JSON_VALUE(d.payload, '$.content.format') AS content_format,
  JSON_VALUE(d.payload, '$.content.text') AS content_text,

  -- Session context (joined)
  s.client_folder_name,
  s.client_first_name,
  s.scheduled_start AS session_date,

  -- Timestamps
  d.canonicalized_at,
  d.idem_key AS document_idem_key
FROM `{project}.{dataset}.canonical_objects` d
JOIN `{project}.{dataset}.proj_client_sessions` s
  ON JSON_VALUE(d.payload, '$.session_source_id') = s.session_source_id
WHERE d.canonical_schema = 'iglu:org.canonical/clinical_document/jsonschema/2-0-0'
"""

# Transcripts linked to sessions (separate from documents due to size)
PROJ_SESSION_TRANSCRIPTS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.proj_session_transcripts` AS
SELECT
  -- Transcript identifiers (synthetic source_id, no writeback to DV)
  JSON_VALUE(t.payload, '$.source_id') AS transcript_source_id,
  JSON_VALUE(t.payload, '$.source_entity') AS transcript_source_entity,
  JSON_VALUE(t.payload, '$.session_source_id') AS session_source_id,

  -- Transcript metadata
  JSON_VALUE(t.payload, '$.id') AS transcript_id,
  CAST(JSON_VALUE(t.payload, '$.date') AS TIMESTAMP) AS transcript_date,
  CAST(JSON_VALUE(t.payload, '$.duration') AS INT64) AS duration_seconds,
  JSON_VALUE(t.payload, '$.language') AS language,

  -- Content
  JSON_VALUE(t.payload, '$.content.format') AS content_format,
  JSON_VALUE(t.payload, '$.content.text') AS content_text,

  -- Processing info
  JSON_VALUE(t.payload, '$.processing.engine') AS transcription_engine,
  JSON_VALUE(t.payload, '$.processing.status') AS processing_status,

  -- Session context (joined)
  s.client_folder_name,
  s.client_first_name,
  s.scheduled_start AS session_date,

  -- Timestamps
  t.canonicalized_at,
  t.idem_key AS transcript_idem_key
FROM `{project}.{dataset}.canonical_objects` t
JOIN `{project}.{dataset}.proj_client_sessions` s
  ON JSON_VALUE(t.payload, '$.session_source_id') = s.session_source_id
WHERE t.canonical_schema = 'iglu:org.canonical/session_transcript/jsonschema/2-0-0'
"""

PROJECTIONS = {
    "proj_therapy_clients": PROJ_THERAPY_CLIENTS,
    "proj_client_sessions": PROJ_CLIENT_SESSIONS,
    "proj_session_documents": PROJ_SESSION_DOCUMENTS,
    "proj_session_transcripts": PROJ_SESSION_TRANSCRIPTS,
}

# Order matters for creation (dependencies)
PROJECTION_ORDER = [
    "proj_therapy_clients",      # Base: contacts filtered to therapy
    "proj_client_sessions",      # Depends on: proj_therapy_clients
    "proj_session_documents",    # Depends on: proj_client_sessions
    "proj_session_transcripts",  # Depends on: proj_client_sessions
]
```

### Projection Dependencies

```
proj_therapy_clients (base view)
    ↓
proj_client_sessions (joins to therapy_clients)
    ↓
    ├── proj_session_documents (joins to client_sessions)
    └── proj_session_transcripts (joins to client_sessions)
```

### Key Fields for Writeback

| Projection | Writeback Field | Dataverse Entity |
|------------|-----------------|------------------|
| `proj_therapy_clients` | `contact_source_id` | `contact` |
| `proj_client_sessions` | `session_source_id` | `cre92_clientsession` |
| `proj_session_documents` | `document_source_id` | `cre92_clientreport` or `annotation` |
| `proj_session_transcripts` | `transcript_source_id` | synthetic (no writeback) |

## Job Types

### 1. `create_projection` - Create/update BQ view

```json
{
  "job_id": "create_proj_client_sessions",
  "job_type": "create_projection",
  "projection": {
    "name": "proj_client_sessions"
  }
}
```

Processor:
1. Look up SQL from `PROJECTIONS[name]`
2. Format with `{project}`, `{dataset}` from env
3. Execute `CREATE OR REPLACE VIEW`
4. Emit `projection.completed`

### 2. `sync_sqlite` - Sync BQ projection → local SQLite

```json
{
  "job_id": "sync_proj_client_sessions",
  "job_type": "sync_sqlite",
  "source": {
    "projection": "proj_client_sessions"
  },
  "sink": {
    "sqlite_path": "~/lifeos/local.db",
    "table": "proj_client_sessions"
  }
}
```

Processor:
1. `SELECT * FROM proj_client_sessions` (BQ)
2. `DELETE FROM proj_client_sessions` (SQLite)
3. Bulk insert rows into SQLite
4. Emit `sync.completed` with row count

### 3. `file_projection` - Render SQLite → markdown files

```json
{
  "job_id": "project_session_files",
  "job_type": "file_projection",
  "source": {
    "sqlite_path": "~/lifeos/local.db",
    "query": "SELECT client_id, session_id, started_at, session_summary_md, transcript_md FROM proj_client_sessions ORDER BY client_id, started_at"
  },
  "sink": {
    "base_path": "~/lifeos/local_views",
    "path_template": "{client_id}/sessions/{started_at}_{session_id}.md",
    "content_template": "# {started_at} — Session {session_id}\n\n## Summary\n\n{session_summary_md}\n\n## Transcript\n\n{transcript_md}"
  }
}
```

Processor:
1. Query SQLite
2. For each row, format `path_template` and `content_template`
3. Write file (create dirs as needed)
4. Emit `file_projection.completed` with file count

## Plan

### Step 1: Create SQL Module [G0: Plan Approval]

**Prompt:**

Create `lorchestra/sql/projections.py` with:
- `PROJ_CLIENT_SESSIONS` SQL constant
- `PROJECTIONS` dict mapping name → SQL
- `get_projection_sql(name: str, project: str, dataset: str) -> str` helper

**Outputs:**

- `lorchestra/sql/__init__.py`
- `lorchestra/sql/projections.py`

---

### Step 2: Extend StorageClient for Raw SQL [G1: Code Readiness]

**Prompt:**

Add `execute_sql()` to `StorageClient` protocol and implement in `BigQueryStorageClient`:

```python
def execute_sql(
    self,
    sql: str,
) -> dict[str, Any]:
    """Execute arbitrary SQL and return result metadata."""
```

Also add `query_to_dataframe()` for sync jobs:

```python
def query_to_dataframe(
    self,
    sql: str,
) -> list[dict[str, Any]]:
    """Execute query and return results as list of dicts."""
```

**Commands:**

```bash
ruff check lorchestra/
pytest tests/test_job_runner.py -v
```

**Outputs:**

- `lorchestra/processors/base.py` (updated)
- `lorchestra/job_runner.py` (updated)

---

### Step 3: Implement CreateProjectionProcessor [G1: Code Readiness]

**Prompt:**

Create `lorchestra/processors/projection.py`:

```python
class CreateProjectionProcessor:
    """Create/update BQ projection views from hardcoded SQL."""

    def run(self, job_spec, context, storage_client, event_client):
        proj_name = job_spec["projection"]["name"]
        sql = get_projection_sql(proj_name, PROJECT, DATASET)

        event_client.log_event("projection.started", ...)
        storage_client.execute_sql(sql)
        event_client.log_event("projection.completed", ...)
```

Register as `job_type: "create_projection"`.

**Commands:**

```bash
ruff check lorchestra/processors/
pytest tests/test_projection_processor.py -v
```

**Outputs:**

- `lorchestra/processors/projection.py`
- `tests/test_projection_processor.py`

---

### Step 4: Implement SyncSqliteProcessor [G1: Code Readiness]

**Prompt:**

Add `SyncSqliteProcessor` to `lorchestra/processors/projection.py`:

```python
class SyncSqliteProcessor:
    """Sync BQ projection to local SQLite (full replace)."""

    def run(self, job_spec, context, storage_client, event_client):
        proj_name = job_spec["source"]["projection"]
        sqlite_path = job_spec["sink"]["sqlite_path"]
        table = job_spec["sink"]["table"]

        # 1. Query BQ
        rows = storage_client.query_to_dataframe(f"SELECT * FROM {proj_name}")

        # 2. Write to SQLite (DELETE + INSERT)
        conn = sqlite3.connect(sqlite_path)
        conn.execute(f"DELETE FROM {table}")
        # bulk insert rows...

        event_client.log_event("sync.completed", payload={"rows": len(rows)})
```

Register as `job_type: "sync_sqlite"`.

**Commands:**

```bash
ruff check lorchestra/processors/
pytest tests/test_projection_processor.py -v
```

**Outputs:**

- `lorchestra/processors/projection.py` (updated)
- `tests/test_projection_processor.py` (updated)

---

### Step 5: Implement FileProjectionProcessor [G1: Code Readiness]

**Prompt:**

Add `FileProjectionProcessor` to `lorchestra/processors/projection.py`:

```python
class FileProjectionProcessor:
    """Render SQLite data to markdown files."""

    def run(self, job_spec, context, storage_client, event_client):
        sqlite_path = job_spec["source"]["sqlite_path"]
        query = job_spec["source"]["query"]
        base_path = Path(job_spec["sink"]["base_path"]).expanduser()
        path_template = job_spec["sink"]["path_template"]
        content_template = job_spec["sink"]["content_template"]

        conn = sqlite3.connect(sqlite_path)
        rows = conn.execute(query).fetchall()

        for row in rows:
            path = base_path / path_template.format(**row)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content_template.format(**row))

        event_client.log_event("file_projection.completed", payload={"files": len(rows)})
```

Register as `job_type: "file_projection"`.

**Commands:**

```bash
ruff check lorchestra/processors/
pytest tests/test_projection_processor.py -v
```

**Outputs:**

- `lorchestra/processors/projection.py` (updated)
- `tests/test_projection_processor.py` (updated)

---

### Step 6: Create Job Definitions [G1: Code Readiness]

**Prompt:**

Create job definition files:

```json
// jobs/definitions/create_proj_client_sessions.json
{
  "job_id": "create_proj_client_sessions",
  "job_type": "create_projection",
  "projection": {
    "name": "proj_client_sessions"
  }
}

// jobs/definitions/sync_proj_client_sessions.json
{
  "job_id": "sync_proj_client_sessions",
  "job_type": "sync_sqlite",
  "source": {
    "projection": "proj_client_sessions"
  },
  "sink": {
    "sqlite_path": "~/lifeos/local.db",
    "table": "proj_client_sessions"
  }
}

// jobs/definitions/project_session_files.json
{
  "job_id": "project_session_files",
  "job_type": "file_projection",
  "source": {
    "sqlite_path": "~/lifeos/local.db",
    "query": "SELECT * FROM proj_client_sessions ORDER BY client_id, started_at"
  },
  "sink": {
    "base_path": "~/lifeos/local_views",
    "path_template": "{client_id}/sessions/{started_at}_{session_id}.md",
    "content_template": "# {started_at} — Session {session_id}\n\n## Summary\n\n{session_summary_md}\n\n## Transcript\n\n{transcript_md}"
  }
}
```

**Outputs:**

- `lorchestra/jobs/definitions/create_proj_client_sessions.json`
- `lorchestra/jobs/definitions/sync_proj_client_sessions.json`
- `lorchestra/jobs/definitions/project_session_files.json`

---

### Step 7: Integration & CLI [G2: Pre-Release]

**Prompt:**

1. Add imports to `job_runner.py`:
```python
import lorchestra.processors.projection
```

2. Test the full flow:
```bash
lorchestra run create_proj_client_sessions
lorchestra run sync_proj_client_sessions
lorchestra run project_session_files
```

3. Verify files appear in `~/lifeos/local_views/{client_id}/sessions/`

**Commands:**

```bash
ruff check .
pytest --cov=lorchestra/processors/projection
lorchestra run create_proj_client_sessions --dry-run
```

**Outputs:**

- `lorchestra/job_runner.py` (updated)
- Working end-to-end flow

## Architecture Summary

```
lorchestra/
  sql/
    __init__.py
    projections.py             # PROJ_CLIENT_SESSIONS, PROJECTIONS dict
  processors/
    projection.py              # CreateProjectionProcessor
                               # SyncSqliteProcessor
                               # FileProjectionProcessor
  jobs/
    definitions/
      create_proj_client_sessions.json
      sync_proj_client_sessions.json
      project_session_files.json
```

### Data Flow

```
[BQ: canonical_objects + measurement_events]
    ↓
create_proj_client_sessions (CREATE VIEW)
    ↓
[BQ: proj_client_sessions view]
    ↓
sync_proj_client_sessions (SELECT → SQLite)
    ↓
[SQLite: proj_client_sessions table]
    ↓
project_session_files (query → markdown)
    ↓
[Files: ~/lifeos/local_views/{client_id}/sessions/*.md]
```

### Event Payloads

```python
# projection.started
{"projection": "proj_client_sessions"}

# projection.completed
{"projection": "proj_client_sessions"}

# sync.completed
{"projection": "proj_client_sessions", "rows": 150}

# file_projection.completed
{"files": 150, "base_path": "~/lifeos/local_views"}
```

## Models & Tools

**Tools:** bash, pytest, ruff, sqlite3

**Dependencies:** None (all hardcoded)

## Repository

**Branch:** `feat/projections-v0`

**Merge Strategy:** squash
