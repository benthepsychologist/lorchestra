# SQLite Schema

The local SQLite database (`~/lifeos/local.db`) stores materialized views from BigQuery for fast local access and file projection.

## Tables

### clients

Therapy clients only (filtered from all contacts).

| Column | Type | Description |
|--------|------|-------------|
| client_id | TEXT | Unique client identifier (UUID) |
| first_name | TEXT | Client first name |
| last_name | TEXT | Client last name |
| email | TEXT | Email address |
| mobile | TEXT | Mobile phone number |
| phone | TEXT | Phone number |
| client_type | TEXT | Always "Therapy" (filtered at source) |
| client_folder | TEXT | Folder name: "FirstName L" format |
| created_at | TEXT | ISO timestamp |
| updated_at | TEXT | ISO timestamp |
| idem_key | TEXT | Idempotency key from BQ |

**Source:** `proj_clients` BQ view

### sessions

Clinical therapy sessions.

| Column | Type | Description |
|--------|------|-------------|
| session_id | TEXT | Unique session identifier (UUID) |
| client_id | TEXT | FK to clients.client_id |
| session_num | INTEGER | Session number for this client (from source) |
| started_at | TEXT | Session start timestamp |
| ended_at | TEXT | Session end timestamp |
| session_status | TEXT | scheduled, completed, cancelled, in_progress, no_show |
| idem_key | TEXT | Idempotency key from BQ |

**Source:** `proj_sessions` BQ view

**Note:** `session_num` is the actual session number from Dataverse, not calculated.

### transcripts

Session transcripts.

| Column | Type | Description |
|--------|------|-------------|
| transcript_id | TEXT | Unique identifier |
| session_id | TEXT | FK to sessions.session_id |
| client_id | TEXT | FK to clients.client_id |
| content | TEXT | Full transcript text (markdown) |
| language | TEXT | Language code (e.g., "en") |
| transcript_date | TEXT | Date of transcript |
| processing_status | TEXT | Processing status |
| created_at | TEXT | ISO timestamp |
| updated_at | TEXT | ISO timestamp |
| idem_key | TEXT | Idempotency key from BQ |

**Source:** `proj_transcripts` BQ view

### clinical_documents

Session notes, summaries, and reports.

| Column | Type | Description |
|--------|------|-------------|
| document_id | TEXT | Unique identifier |
| session_id | TEXT | FK to sessions.session_id |
| client_id | TEXT | FK to clients.client_id |
| doc_type | TEXT | soap-note, client-summary, progress-report |
| title | TEXT | Document title |
| content | TEXT | Full document text (markdown) |
| status | TEXT | Document status |
| audience | TEXT | Intended audience |
| document_date | TEXT | Document date |
| created_at | TEXT | ISO timestamp |
| updated_at | TEXT | ISO timestamp |
| object_type | TEXT | Object type from canonical |
| idem_key | TEXT | Idempotency key from BQ |

**Source:** `proj_clinical_documents` BQ view

**Document Types:**
- `soap-note` - Clinician session notes (SOAP format)
- `client-summary` - Client-facing session summaries
- `progress-report` - Progress reports for insurance/records

### form_responses

Client form responses (questionnaires, assessments).

| Column | Type | Description |
|--------|------|-------------|
| idem_key | TEXT | Idempotency key from BQ |
| response_id | TEXT | Unique response identifier |
| respondent_email | TEXT | Email of respondent |
| client_id | TEXT | FK to clients.client_id (matched by email) |
| status | TEXT | Response status |
| submitted_at | TEXT | Submission timestamp |
| last_updated_at | TEXT | Last update timestamp |
| source_platform | TEXT | Source platform (google_forms, etc.) |
| answers_json | TEXT | JSON array of answers |

**Source:** `proj_form_responses` BQ view

**Note:** `client_id` is linked by matching `respondent_email` to `clients.email`.

## Relationships

```
┌──────────────┐
│   clients    │
└──────┬───────┘
       │ client_id
       │
       ├────────────────┬────────────────┬────────────────┐
       │                │                │                │
       ▼                ▼                ▼                ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   sessions   │ │  transcripts │ │  clinical_   │ │    form_     │
│              │ │              │ │  documents   │ │  responses   │
└──────┬───────┘ └──────────────┘ └──────────────┘ └──────────────┘
       │ session_id      ▲                ▲
       │                 │                │
       └─────────────────┴────────────────┘
```

## Example Queries

### Get all sessions for a client with content

```sql
SELECT
    c.client_folder,
    s.session_num,
    t.content as transcript,
    d1.content as session_note,
    d2.content as session_summary
FROM clients c
JOIN sessions s ON c.client_id = s.client_id
LEFT JOIN transcripts t ON s.session_id = t.session_id
LEFT JOIN clinical_documents d1 ON s.session_id = d1.session_id AND d1.doc_type = 'soap-note'
LEFT JOIN clinical_documents d2 ON s.session_id = d2.session_id AND d2.doc_type = 'client-summary'
WHERE c.first_name = 'Adam' AND c.last_name LIKE 'D%'
ORDER BY s.session_num;
```

### Get form responses for a client

```sql
SELECT
    c.client_folder,
    fr.submitted_at,
    fr.answers_json
FROM clients c
JOIN form_responses fr ON c.client_id = fr.client_id
WHERE c.first_name = 'Adam'
ORDER BY fr.submitted_at DESC;
```

### Count content by client

```sql
SELECT
    c.client_folder,
    COUNT(DISTINCT s.session_id) as sessions,
    COUNT(DISTINCT t.transcript_id) as transcripts,
    COUNT(DISTINCT CASE WHEN d.doc_type = 'soap-note' THEN d.document_id END) as notes,
    COUNT(DISTINCT CASE WHEN d.doc_type = 'client-summary' THEN d.document_id END) as summaries,
    COUNT(DISTINCT CASE WHEN d.doc_type = 'progress-report' THEN d.document_id END) as reports
FROM clients c
LEFT JOIN sessions s ON c.client_id = s.client_id
LEFT JOIN transcripts t ON s.session_id = t.session_id
LEFT JOIN clinical_documents d ON s.session_id = d.session_id
GROUP BY c.client_id, c.client_folder
ORDER BY c.client_folder;
```

## Sync Behavior

Tables are synced using **full replace** strategy:
1. Drop existing table
2. Create new table from BQ query results
3. All data is refreshed on each sync

This ensures consistency with BQ source but means:
- No incremental updates
- No local-only data survives sync
- Sync is idempotent (safe to run multiple times)

## File Location

Default path: `~/lifeos/local.db`

Configurable in job definitions via `sqlite_path` field.
