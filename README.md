# lorchestra - Lightweight Job Orchestrator

A data pipeline orchestrator for personal/clinical data. lorchestra runs JSON-defined jobs that ingest data from external sources, transform it into canonical formats, process clinical measurements, and project data to local files.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              CLI                                         │
│  lorchestra run <job_id> | jobs list | stats | query | sql              │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           JobRunner                                      │
│  - Loads JSON spec from jobs/definitions/{job_id}.json                  │
│  - Dispatches to processor by job_type                                  │
│  - Emits job.started / job.completed / job.failed events                │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
        ┌────────────────────────────┼────────────────────────────┐
        │                            │                            │
        ▼                            ▼                            ▼
┌───────────────┐          ┌──────────────────┐          ┌───────────────┐
│IngestProcessor│          │CanonizeProcessor │          │ ProjectionProc│
│               │          │                  │          │               │
│ injest lib    │          │ canonizer lib    │          │ BQ views      │
│ → raw_objects │          │ → canonical_obj  │          │ SQLite sync   │
└───────────────┘          └──────────────────┘          │ File output   │
                                                         └───────────────┘
```

## Data Flow

```
External Sources          BigQuery Tables              Local Files
─────────────────         ────────────────             ───────────
Gmail, Exchange    ──▶    raw_objects       ──▶       ~/lifeos/
Dataverse, Stripe         (ingested data)             local_views/
Google Forms                    │                         │
                                ▼                         │
                          canonical_objects  ──▶    SQLite local.db
                          (transformed)                   │
                                │                         │
                                ▼                         ▼
                          measurement_events        Markdown files
                          observations              (transcripts,
                          (clinical data)           session notes,
                                                    reports)
```

## Quick Start

```bash
# Install
cd /workspace/lorchestra
uv venv && source .venv/bin/activate
uv pip install -e .

# Configure
cp .env.example .env
# Edit .env with BigQuery credentials

# List available jobs
lorchestra jobs list

# Run a single job
lorchestra run ingest_gmail_acct1

# Run daily pipeline
./scripts/daily_ingest.sh      # Ingest + validate
./scripts/daily_canonize.sh    # Transform to canonical
./scripts/daily_form.sh        # Process clinical forms
./scripts/daily_projection.sh  # Sync to local files
```

## Daily Pipeline

The pipeline runs in four stages, each with its own script:

### 1. Ingest & Validate (`daily_ingest.sh`)

Pulls data from external sources and validates against schemas:

| Source | Jobs | Object Types |
|--------|------|--------------|
| Gmail | 3 accounts | email |
| Exchange | 3 accounts | email |
| Dataverse | contacts, sessions, reports | contact, session, report |
| Stripe | customers, invoices, payments, refunds | customer, invoice, etc. |
| Google Forms | 4 forms | form_response |

### 2. Canonize (`daily_canonize.sh`)

Transforms validated raw objects to canonical format:

- Email → JMAP Lite format
- Dataverse sessions → clinical_session + transcript + notes + summary
- Stripe → normalized customer/invoice/payment
- Google Forms → form_response

### 3. Form Processing (`daily_form.sh`)

Processes clinical instruments from form responses:

- PHQ-9, GAD-7 (depression/anxiety)
- FSCRS (self-criticism)
- IPIP-NEO-60 (personality)
- PSS-10 (stress)
- PHLMS-10 (mindfulness)

### 4. Projection (`daily_projection.sh`)

Syncs canonical data to local files:

1. Create BQ views (`create_projections.sh` - one time)
2. Sync views to SQLite
3. Project to markdown files in `~/lifeos/local_views/`

## Job Types

Jobs are JSON specs in `lorchestra/jobs/definitions/`:

### ingest
```json
{
  "job_id": "ingest_gmail_acct1",
  "job_type": "ingest",
  "source": { "stream": "gmail_acct1" },
  "sink": { "source_system": "gmail", "connection_name": "acct1", "object_type": "email" },
  "options": { "auto_since": true }
}
```

### canonize
```json
{
  "job_id": "canonize_gmail_jmap",
  "job_type": "canonize",
  "source": { "source_system": "gmail", "object_type": "email" },
  "transform": { "transform_ref": "email/gmail_to_jmap_lite@1.0.0", "schema_out": "iglu:..." }
}
```

### final_form
```json
{
  "job_id": "form_intake_01",
  "job_type": "final_form",
  "source": { "canonical_schema": "iglu:org.canonical/form_response/..." },
  "form": { "form_id": "intake_01", "instruments": ["phq9", "gad7", ...] }
}
```

### create_projection / sync_sqlite / file_projection
See `docs/projection-pipeline.md` for details.

## CLI Commands

```bash
# Run a job
lorchestra run <job_id> [--dry-run] [--test-table]

# List jobs
lorchestra jobs list [--type ingest|canonize|final_form|...]

# Show job definition
lorchestra jobs show <job_id>

# Pipeline stats
lorchestra stats raw          # Raw object counts by source
lorchestra stats canonical    # Canonical object counts by schema
lorchestra stats jobs         # Recent job executions

# Run SQL queries
lorchestra query <query_name>  # Run predefined query
lorchestra sql "SELECT ..."    # Run ad-hoc SQL
```

## Documentation

- **[docs/projection-pipeline.md](docs/projection-pipeline.md)** - Local file projection pipeline
- **[docs/canonical-transforms.md](docs/canonical-transforms.md)** - Transform specifications
- **[docs/sqlite-schema.md](docs/sqlite-schema.md)** - Local SQLite database schema
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - System architecture

## Development

```bash
# Run tests
pytest tests/ -v

# Run specific test file
pytest tests/test_job_runner.py -v

# With coverage
pytest --cov=lorchestra tests/
```

## BigQuery Tables

| Table | Purpose |
|-------|---------|
| `raw_objects` | Ingested data with `source_system`, `object_type`, `payload` |
| `canonical_objects` | Transformed canonical data |
| `measurement_events` | Clinical measurement outcomes |
| `observations` | Individual measurement observations |
| `event_log` | Job execution audit trail |

## Project Structure

```
lorchestra/
├── lorchestra/
│   ├── cli.py              # CLI commands
│   ├── job_runner.py       # Job dispatch
│   ├── processors/         # Job type processors
│   │   ├── ingest.py
│   │   ├── canonize.py
│   │   ├── final_form.py
│   │   └── projection.py
│   ├── jobs/definitions/   # JSON job specs (67 jobs)
│   ├── sql/                # SQL projections
│   └── queries/            # Predefined SQL queries
├── scripts/
│   ├── daily_ingest.sh
│   ├── daily_canonize.sh
│   ├── daily_form.sh
│   └── daily_projection.sh
├── .canonizer/             # Transform registry
├── setup/                  # BigQuery setup scripts
└── tests/
```

---

**Version:** 0.2.0 (JSON specs)
**Last Updated:** 2025-12-05
