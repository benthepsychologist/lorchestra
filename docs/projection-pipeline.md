# Projection Pipeline

The projection pipeline transforms canonical data from BigQuery into a local SQLite database and markdown files for the therapist surface.

## Overview

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   BigQuery          │     │   SQLite            │     │   Markdown Files    │
│   canonical_objects │────▶│   local.db          │────▶│   ~/lifeos/         │
│                     │     │                     │     │   local_views/      │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
        │                           │                           │
        │ BQ Views                  │ Tables                    │ Files
        │ (proj_*)                  │                           │
        ▼                           ▼                           ▼
  - proj_clients              - clients                 - <Client>/contact.md
  - proj_sessions             - sessions                - <Client>/sessions/session-N/
  - proj_transcripts          - transcripts               - transcript.md
  - proj_clinical_documents   - clinical_documents        - session-note.md
  - proj_form_responses       - form_responses            - session-summary.md
                                                        - <Client>/reports/
                                                          - <date>-progress-report.md
```

## Components

### 1. BigQuery Projection Views (`lorchestra/sql/projections.py`)

SQL views that extract and flatten data from the `canonical_objects` table:

| View | Description | Filter |
|------|-------------|--------|
| `proj_clients` | Client contact info with folder naming | `client_type_label = 'Therapy'` |
| `proj_sessions` | Clinical sessions with session numbers | `contact_id IS NOT NULL` |
| `proj_transcripts` | Session transcript content | `content IS NOT NULL` |
| `proj_clinical_documents` | Session notes, summaries, reports | `content IS NOT NULL` |
| `proj_form_responses` | Form responses linked to clients by email | All |

### 2. Processors (`lorchestra/processors/projection.py`)

Three processors handle the pipeline stages:

#### CreateProjectionProcessor
- **Job type:** `create_projection`
- **Purpose:** Creates/replaces BQ views
- **Config:**
  ```json
  {
    "job_type": "create_projection",
    "projection": { "name": "proj_clients" }
  }
  ```

#### SyncSqliteProcessor
- **Job type:** `sync_sqlite`
- **Purpose:** Syncs BQ projection to SQLite table (full replace)
- **Config:**
  ```json
  {
    "job_type": "sync_sqlite",
    "source": { "projection": "proj_clients" },
    "sink": {
      "sqlite_path": "~/lifeos/local.db",
      "table": "clients"
    }
  }
  ```

#### FileProjectionProcessor
- **Job type:** `file_projection`
- **Purpose:** Queries SQLite and writes markdown files
- **Config:**
  ```json
  {
    "job_type": "file_projection",
    "source": {
      "sqlite_path": "~/lifeos/local.db",
      "query": "SELECT ... FROM ..."
    },
    "sink": {
      "base_path": "~/lifeos/local_views",
      "path_template": "{client_folder}/sessions/session-{session_num}/transcript.md",
      "content_template": "{content}"
    }
  }
  ```

### 3. Job Definitions (`lorchestra/jobs/definitions/`)

| Job | Type | Purpose |
|-----|------|---------|
| `create_proj_clients` | create_projection | Create proj_clients view |
| `create_proj_sessions` | create_projection | Create proj_sessions view |
| `create_proj_transcripts` | create_projection | Create proj_transcripts view |
| `create_proj_clinical_documents` | create_projection | Create proj_clinical_documents view |
| `create_proj_form_responses` | create_projection | Create proj_form_responses view |
| `sync_proj_clients` | sync_sqlite | Sync clients to SQLite |
| `sync_proj_sessions` | sync_sqlite | Sync sessions to SQLite |
| `sync_proj_transcripts` | sync_sqlite | Sync transcripts to SQLite |
| `sync_proj_clinical_documents` | sync_sqlite | Sync clinical_documents to SQLite |
| `sync_proj_form_responses` | sync_sqlite | Sync form_responses to SQLite |
| `project_contacts` | file_projection | Write contact.md files |
| `project_transcripts` | file_projection | Write transcript.md files |
| `project_session_notes` | file_projection | Write session-note.md files |
| `project_session_summaries` | file_projection | Write session-summary.md files |
| `project_reports` | file_projection | Write progress report files |

## Output Structure

```
~/lifeos/local_views/
├── Adam D/
│   ├── contact.md                    # Client contact info
│   ├── sessions/
│   │   ├── session-1/
│   │   │   ├── transcript.md         # Session transcript
│   │   │   ├── session-note.md       # SOAP note
│   │   │   └── session-summary.md    # Client-facing summary
│   │   ├── session-2/
│   │   │   └── ...
│   │   └── ...
│   └── reports/
│       └── 2025-10-14T14:59:22Z-progress-report.md
├── Jane S/
│   └── ...
└── ...
```

## Scripts

### `./scripts/create_projections.sh`

Creates or recreates all BigQuery projection views. Run this:
- Once during initial setup
- When projection SQL in `lorchestra/sql/projections.py` changes

```bash
./scripts/create_projections.sh
```

### `./scripts/daily_projection.sh`

Runs the full sync and projection pipeline:

```bash
# Normal run (incremental)
./scripts/daily_projection.sh

# Full refresh (clears local_views first)
./scripts/daily_projection.sh --full-refresh
```

**Steps:**
1. Sync all BQ projections to SQLite
2. (Optional) Clear existing markdown files
3. Project all markdown files
4. Print stats

### `./scripts/recanonize_sessions.sh`

Deletes and regenerates canonical session objects. Use when the `clinical_session` transform changes:

```bash
./scripts/recanonize_sessions.sh
```

**Steps:**
1. Delete all canonical sessions from BQ
2. Re-run `canonize_dataverse_sessions` job
3. Print verification stats

## Data Flow Details

### Client Filtering

Only clients with `client_type_label = 'Therapy'` are included. This is enforced at the BQ view level in `proj_clients`.

### Session Numbering

Session numbers come from the source data field `cre92_sessionnumber` (Dataverse). The canonical transform extracts this as `session_num`. This is NOT calculated - it's the actual session number from the source system.

### Client Linking

Different entity types link to clients differently:

| Entity | Link Field | Source |
|--------|-----------|--------|
| Sessions | `contact_id` | `_cre92_client_value` in raw data |
| Transcripts | `subject.reference` | Contains `Contact/<id>` |
| Clinical Documents | `subject.reference` | Contains `Contact/<id>` |
| Form Responses | `respondent.email` | Matched to `clients.email` |

### Content Extraction

Canonical objects store content as nested JSON:
```json
{
  "content": {
    "format": "markdown",
    "text": "The actual content..."
  }
}
```

The projection uses `JSON_VALUE(payload, '$.content.text')` to extract the text.

## Troubleshooting

### Sessions have NULL contact_id

The canonical transform may be missing the `_cre92_client_value` field mapping. Check `.canonizer/registry/transforms/clinical_session/dataverse_to_canonical/2-0-0/spec.jsonata` and ensure it includes:

```jsonata
"contact_id": $string(_cre92_client_value ? _cre92_client_value : ...)
```

Then run `./scripts/recanonize_sessions.sh`.

### Content is NULL in projections

Content is stored as a nested object `{format, text}`. Ensure the projection SQL uses:
```sql
JSON_VALUE(payload, '$.content.text') AS content
```
Not just `$.content`.

### Wrong session numbers

Session numbers should come from source data, not be calculated. If using `ROW_NUMBER()`, sessions will be renumbered incorrectly. Use the actual `session_num` field from canonical data.

### Missing files for a client

1. Check if client has `client_type_label = 'Therapy'`
2. Check if sessions have `contact_id` set
3. Check if content fields are populated
4. Run with `--full-refresh` to rebuild from scratch

## Adding New Projections

1. **Add SQL view** to `lorchestra/sql/projections.py`:
   ```python
   PROJ_NEW_ENTITY = """
   CREATE OR REPLACE VIEW `{project}.{dataset}.proj_new_entity` AS
   SELECT ...
   """

   # Add to registry
   PROJECTIONS["proj_new_entity"] = PROJ_NEW_ENTITY
   ```

2. **Create job definitions:**
   - `create_proj_new_entity.json` (create_projection)
   - `sync_proj_new_entity.json` (sync_sqlite)
   - `project_new_entity.json` (file_projection)

3. **Update scripts** to include new jobs

4. **Run:**
   ```bash
   ./scripts/create_projections.sh
   ./scripts/daily_projection.sh --full-refresh
   ```
