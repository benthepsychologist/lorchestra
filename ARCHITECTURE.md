# Architecture Goal: Minimal Event Pipeline

**Status:** Active target architecture (2025-11 through 2025-01)
**Purpose:** Define the minimal, survivable event pipeline architecture for lorchestra's immediate scope
**Scope:** Three working lanes: sales pipeline, measurement pipeline, automated reporting

---

## 1. Purpose and Horizon

### What This Document Is

This document describes the concrete architectural target for the next 2–3 months. It defines the minimal set of components, data flows, and constraints needed to deliver three specific business capabilities:

1. Sales pipeline tracking via email
2. Measurement pipeline via clinical questionnaires (PHQ-9, GAD-7)
3. Automated client reporting

This is an implementation guide with explicit boundaries. It exists to prevent scope creep and maintain focus on a working, deployable system.

### What This Document Is Not

This is not:
- A general manifesto or vision document
- A grand unified design for all future capabilities
- A complete specification of lorchestra's eventual feature set
- A roadmap for advanced orchestration, RAG systems, or research tooling

Features outside the immediate scope are explicitly frozen and documented as out-of-scope.

---

## 2. Guiding Principles

### 2.1 Events-First Architecture

All meaningful business activity is represented as events. Events are immutable, timestamped, and typed. The system produces two classes of events:

- **Raw events**: Direct captures from external systems (email received, form submitted, API call made)
- **Canonical events**: Normalized, validated events derived from raw events (email.canonicalized, questionnaire_response, measurement.scored)

### 2.2 BigQuery Two-Table Pattern

BigQuery is the primary storage system using a two-table pattern:
- **event_log**: Append-only audit trail (immutable event envelopes)
- **raw_objects**: State projection of current object data (payload updates on re-ingestion)

The WAL lives in the source systems (Dataverse, Gmail, Exchange). We sync current state to `raw_objects` while maintaining an audit trail in `event_log`. All downstream projections, views, and reports are derived from BQ tables.

JSONL files are a simple backup and replay mechanism only. One JSONL file per run/day, one event per line, no complex chunking or partitioning logic. JSONL is not a primary architectural layer.

### 2.3 lorchestra as Orchestrator

lorchestra (lorc) is the single cross-cutting orchestrator. It:
- Runs jobs from domain packages
- Owns the initial client helpers (event_client, minimal schema helpers)
- Serves as the front door for running the pipeline
- Coordinates workflow execution

lorchestra does NOT become a monorepo. Domain logic remains in separate packages.

### 2.4 Python Packages with Job Entrypoints

All domain tools (ingester, canonizer, final-form, reporter) are Python packages with callable job entrypoints. They are NOT CLI wrappers or bash-invoked binaries.

Jobs are invoked programmatically by lorc via Python imports and function calls.

### 2.5 Minimal, Frozen Surface Area

The architecture deliberately freezes advanced features to maintain focus:
- No vector-projector / RAG / local projection stack
- No research pipeline or academic tooling
- No dogfold (block-level DSL)
- No auto-gov v2+ (advanced policy engines)
- No complex orchestration frameworks (Airflow, Dagster)
- No cross-cloud harmonization

These remain out of scope until the three target lanes are operational.

---

## 3. Active Components (Now)

### 3.1 lorchestra (lorc)

**Role:** Orchestration CLI and runtime environment

**Responsibilities:**
- Provides the `lorc` command-line interface for running jobs
- Discovers and executes jobs from registered domain packages
- Hosts initial client libraries (event_client, schema helpers)
- Manages pipeline state and logging
- Coordinates cross-domain workflows

**Does NOT:**
- Implement domain logic (extraction, canonization, transformation)
- Directly interact with external APIs (delegates to domain packages)
- Maintain its own data storage beyond logs and state

### 3.2 event_client

**Role:** Unified event writing interface (BigQuery gateway)

**Location:** Inside lorc repo at `lorc/stack_clients/event_client.py`

**Responsibilities:**
- Write events to BigQuery using two-table pattern:
  - `event_log`: Audit trail (event envelopes, no payload)
  - `raw_objects`: Deduped object store (payloads with idem_key)
- Generate deterministic idem_key for idempotency
- Provide MERGE-based deduplication for raw_objects
- Handle partitioning (event_log by date) and clustering

**Design:**
- Simple, focused API: `emit(event_type, payload, source, object_type, bq_client)`
- Two-table pattern ensures:
  - Event audit trail preserved (every emit() creates event_log row)
  - Object state projection (same idem_key updates payload and last_seen)
  - Clean separation: metadata vs. payload
- No JSONL backup in initial implementation (BQ is primary storage)

### 3.3 ingestor

**Role:** Thin Meltano wrapper for raw data extraction

**Location:** Separate package at `/workspace/ingestor`

**Responsibilities:**
- Wrap Meltano taps via `extract_to_jsonl(tap_name, run_id)`
- Run `meltano run tap-x target-jsonl` and return JSONL file path
- Provide iterator for JSONL records: `iter_jsonl_records(path)`
- Handle Meltano configuration and environment setup

**Critical Boundary Contract:**
- **Does NOT** import event_client or know about events
- **Does NOT** import BigQuery or write to BQ
- **Does NOT** emit events (that's lorc jobs' responsibility)
- Pure function: Meltano tap → JSONL file path

**Design:**
- Domain-agnostic (works for any Meltano tap)
- Uses standard Singer target-jsonl (no custom targets)
- Event boundary is in lorc, not ingestor

### 3.4 canonizer

**Role:** Domain package for producing canonical events from raw events

**Responsibilities:**
- Read raw events from BQ
- Apply transformation logic (JSONata or Python-based)
- Emit canonical events via event_client (e.g., `email.canonicalized`, `questionnaire_response`)
- Provide job entrypoints callable by lorc

**Does NOT:**
- Extract from external systems
- Create projections or views (that's separate)

### 3.5 final-form

**Role:** Domain package for scoring and enriching measurement events

**Responsibilities:**
- Read canonical questionnaire_response events
- Calculate clinical scores (PHQ-9, GAD-7)
- Emit scored measurement events (e.g., `measurement.scored`)
- Provide job entrypoints callable by lorc

### 3.6 reporter

**Role:** Domain package for generating and delivering client reports

**Responsibilities:**
- Query BQ for canonical and scored events
- Generate reports (PDF, HTML, email body)
- Send reports via email or other channels
- Provide job entrypoints callable by lorc

---

## 4. Event Flow (Raw → Canonical → Projection)

### 4.1 Raw Event Emission (Three-Layer Pattern)

**Layer 1: ingestor** (Meltano wrapper)
1. lorc job calls `ingestor.extract_to_jsonl("tap-gmail--acct1", run_id)`
2. ingestor runs `meltano run tap-gmail--acct1 target-jsonl`
3. Returns JSONL file path (e.g., `/tmp/phi-vault/jsonl-tmp/{run_id}.jsonl`)

**Layer 2: lorc job** (Event boundary)
4. lorc job reads JSONL using `iter_jsonl_records(jsonl_path)`
5. For each record, lorc job calls `event_client.emit()`:
   - Event type: `email.received`
   - Payload: Raw message data from JSONL
   - Source: `tap-gmail--acct1-personal`
   - Object type: `email`

**Layer 3: event_client** (BigQuery gateway)
6. event_client generates `idem_key` from payload (content-based)
7. event_client writes to BigQuery:
   - `event_log`: INSERT event envelope (event_id, event_type, idem_key, timestamp)
   - `raw_objects`: MERGE payload (dedup by idem_key, update last_seen if exists)

### 4.2 Canonical Event Production

1. lorc invokes a canonizer job (e.g., `canonicalize_email`)
2. Canonizer queries BQ for unprocessed `email.received` events
3. For each raw event, canonizer:
   - Applies transformation logic (extract sender, subject, timestamp, body)
   - Validates against canonical email schema
   - Calls `event_client.write_event()`:
     - Event type: `email.canonicalized`
     - Payload: Normalized email data
     - Metadata: Original event ID, canonicalization timestamp
4. event_client writes to:
   - BigQuery table: `canonical_events` (or unified `events` table)
   - JSONL backup: `backup/canonical/YYYY-MM-DD/run_{run_id}.jsonl`

### 4.3 Projection and Derived Events

1. lorc invokes domain-specific jobs (e.g., `score_phq9`, `build_inbox_view`)
2. Jobs query canonical events from BQ
3. Jobs apply business logic:
   - Scoring: Calculate PHQ-9 score from questionnaire responses
   - Aggregation: Build inbox view from canonicalized emails
   - Enrichment: Add derived fields
4. Jobs emit scored/enriched events or write to projection tables:
   - Scored events: `measurement.scored` (via event_client)
   - Projection tables: Direct BQ inserts (e.g., `inbox_view`, `measurement_timeseries`)

### 4.4 Reporting

1. lorc invokes reporter job (e.g., `generate_weekly_client_report`)
2. Reporter queries projection tables or canonical events from BQ
3. Reporter generates report artifacts (PDF, HTML)
4. Reporter delivers via email or file storage
5. Optional: Reporter emits `report.generated` event for audit trail

---

## 5. Storage Strategy

### 5.1 BigQuery Two-Table Pattern

BigQuery is the primary storage, using a two-table pattern separating audit trail from state projection.

**Implemented Design:** Audit trail (event_log) + state projection (raw_objects)

**Column Standards (aligned with Airbyte/Singer/Meltano):**
- **source_system**: Provider family (gmail, exchange, dataverse, google_forms)
- **connection_name**: Configured account (gmail-acct1, exchange-ben-mensio)
- **object_type / target_object_type**: Domain object (email, contact, session, form_response)

**Table 1: `event_log`** (Audit trail - with optional payload)
```sql
CREATE TABLE event_log (
  event_id STRING NOT NULL,           -- UUID per emit() call
  event_type STRING NOT NULL,         -- Verb: job.started, ingest.completed, upsert.completed
  source_system STRING NOT NULL,      -- Provider (or 'lorchestra' for system events)
  connection_name STRING,             -- Account (NULL for system events)
  target_object_type STRING,          -- Noun: email, session (NULL for job events)
  event_schema_ref STRING,            -- Iglu URI: iglu:com.mensio.event/ingest_completed/jsonschema/1-0-0
  correlation_id STRING,              -- run_id for tracing
  trace_id STRING,                    -- Cross-system trace ID
  created_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,             -- "ok" | "failed"
  error_message STRING,               -- Human-readable error if status="failed"
  payload JSON                        -- Telemetry: counts, duration, parameters
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, event_type;
```
- One row per event (immutable audit trail)
- Partitioned by date for performance
- Event types: job.started, job.completed, job.failed, ingest.completed, ingest.failed, upsert.completed

**Table 2: `raw_objects`** (Deduped object store with payload)
```sql
CREATE TABLE raw_objects (
  idem_key STRING NOT NULL,           -- Opaque: {source_system}:{connection_name}:{object_type}:{external_id}
  source_system STRING NOT NULL,      -- Provider: gmail, exchange, dataverse, google_forms
  connection_name STRING NOT NULL,    -- Account: gmail-acct1, exchange-ben-mensio
  object_type STRING NOT NULL,        -- Domain object: email, contact, session, form_response
  schema_ref STRING,                  -- Iglu URI: iglu:com.mensio.raw/raw_gmail_email/jsonschema/1-0-0
  external_id STRING,                 -- Upstream ID (Gmail message_id, Dataverse GUID, etc.)
  payload JSON NOT NULL,              -- Full raw data
  first_seen TIMESTAMP NOT NULL,
  last_seen TIMESTAMP NOT NULL
)
CLUSTER BY source_system, connection_name, object_type;
```
- One row per unique object (MERGE provides idempotency)
- Same object re-ingested → `payload` and `last_seen` updated (state projection)
- Payload stored as native BigQuery JSON type
- idem_key is opaque - never parsed, all filtering uses explicit columns

**idem_key Pattern:**
```
{source_system}:{connection_name}:{object_type}:{external_id}
```
Examples:
- `gmail:gmail-acct1:email:18c5a7b2e3f4d5c6`
- `exchange:exchange-ben-mensio:email:AAMkAGQ...`
- `dataverse:dataverse-clinic:session:739330df-5757-f011-bec2-6045bd619595`
- `google_forms:google-forms-intake-01:form_response:ACYDBNi84NuJUO2O13A`

**Connection Name Mapping:**
| Provider | connection_name | object_type |
|----------|-----------------|-------------|
| gmail | gmail-acct1, gmail-acct2, gmail-acct3 | email |
| exchange | exchange-ben-mensio, exchange-booking-mensio, exchange-info-mensio | email |
| dataverse | dataverse-clinic | contact, session, report |
| google_forms | google-forms-ipip120, google-forms-intake-01/02, google-forms-followup | form_response |

**Benefits:**
- Idempotency: Re-running same extraction is safe (no duplicates)
- Audit trail: Every ingestion event logged in event_log with telemetry
- Clean separation: Metadata (event_log) vs. Data (raw_objects)
- Query-friendly: Filter by source_system, connection_name, object_type (not parsing idem_key)

**Retention:**
- All events retained indefinitely (immutable log)
- No automatic deletion or archival

### 5.2 JSONL as Backup and Replay

JSONL files provide a simple backup and replay mechanism.

**File Organization:**
- One file per run/day: `backup/{raw|canonical}/YYYY-MM-DD/run_{run_id}.jsonl`
- One event per line (newline-delimited JSON)
- No compression, chunking, or complex indexing

**Use Cases:**
- Emergency replay if BQ becomes unavailable
- Local debugging and inspection
- Audit trail outside of BQ

**Not Used For:**
- Primary querying or analytics
- Complex event sourcing or replay logic
- Performance-critical operations

### 5.3 Projection Tables

Projection tables are optional, denormalized views for specific use cases.

**Examples:**
- `inbox_view`: Denormalized email view for sales pipeline
- `measurement_timeseries`: Time-series data for clinical measurements
- `reporting_snapshots`: Pre-computed aggregates for weekly reports

Projections are derived from canonical events and can be rebuilt at any time.

---

## 6. In-Scope vs Out-of-Scope

### 6.1 In-Scope (Next 2–3 Months)

| Component | Description |
|-----------|-------------|
| lorc orchestrator | Job execution, client hosting, pipeline coordination |
| event_client | Two-table pattern: event_log (audit) + raw_objects (deduped) |
| ingestor package | Thin Meltano wrapper (returns JSONL paths, NO event knowledge) |
| lorc jobs | Event boundary - reads JSONL, emits via event_client |
| canonizer jobs | Canonicalize email, questionnaire responses |
| final-form jobs | Score PHQ-9, GAD-7 measurements |
| reporter jobs | Generate and send weekly client reports |
| BQ event tables | **event_log** (partitioned audit trail), **raw_objects** (deduped payloads) |
| JSONL intermediate | Temporary files from Meltano (not a primary storage layer) |
| Minimal projections | inbox_view, measurement_timeseries, reporting_snapshots |

### 6.2 Explicitly Out-of-Scope (Frozen)

| Component | Reason |
|-----------|--------|
| vector-projector / RAG | Not needed for three target lanes |
| Local projection stack | BQ is sufficient for now |
| research-radar / academic tooling | Not in immediate scope |
| dogfold (block-level DSL) | Too complex, no immediate need |
| auto-gov v2+ | Advanced policy engine deferred |
| block ABI / "blocks everywhere" | Grand vision, not minimal viable |
| Airflow/Dagster orchestration | lorc is sufficient for simple jobs |
| Cross-cloud harmonization (gorch) | Single-cloud deployment for now |
| schema_client (full version) | Minimal schema helpers only |
| tool_client, action_client | Not needed yet |

---

## 7. Milestones for "Feels Real"

### 7.1 Sales Pipeline Alive

**Definition:** End-to-end flow from email ingestion to actionable sales data

**Success Criteria:**
1. Raw email events flowing from Gmail and Exchange into BQ
2. Canonical email events available in `canonical_events` table
3. inbox_view projection built and queryable
4. Manual or scheduled job runs via lorc
5. At least 7 days of historical email data processed

**Deliverables:**
- `lorc run extract_email` works for Gmail and Exchange
- `lorc run canonicalize_email` produces valid canonical events
- `lorc run build_inbox_view` creates queryable projection
- BQ tables visible and query-able via BQ console or CLI

### 7.2 Measurement Pipeline Alive

**Definition:** End-to-end flow from questionnaire submission to scored measurements

**Success Criteria:**
1. Raw form submission events flowing from Google Sheets into BQ
2. Canonical questionnaire_response events available
3. Scored PHQ-9 and GAD-7 measurements available in `canonical_events` or dedicated table
4. measurement_timeseries projection built and queryable
5. At least 10 real or test questionnaire responses processed

**Deliverables:**
- `lorc run extract_forms` ingests Google Sheets responses
- `lorc run canonicalize_questionnaires` produces questionnaire_response events
- `lorc run score_phq9` and `lorc run score_gad7` emit measurement.scored events
- measurement_timeseries projection shows trends over time

### 7.3 Reporting Alive

**Definition:** Automated weekly client reports generated and delivered

**Success Criteria:**
1. Reporter job queries BQ for canonical/scored events
2. Report generated in PDF or HTML format
3. Report sent via email to test recipient
4. Scheduled execution via lorc (manual cron or simple scheduler)
5. At least one real client report delivered

**Deliverables:**
- `lorc run generate_weekly_report` produces report artifact
- Report includes sales pipeline summary and measurement trends
- Report delivered via email with proper formatting
- Execution logged and auditable via lorc logs

---

## 8. Constraints and Assumptions

### 8.1 No Monorepo

lorchestra, ingester, canonizer, final-form, and reporter remain separate repositories. lorc imports them as Python packages via pip or local editable installs.

### 8.2 No Complex State Management

lorc maintains minimal state (logs, run IDs, last execution timestamps). No complex workflow DAGs or dependency graphs initially.

### 8.3 Single-Cloud Deployment

All components deploy to a single cloud provider (likely GCP for BQ proximity). No multi-cloud or cross-cloud harmonization.

### 8.4 Manual Scheduling Initially

Jobs are triggered manually via `lorc run {job_name}` or via simple cron. No advanced orchestration framework (Airflow, Dagster) until after the three lanes are operational.

### 8.5 Minimal Schema Evolution

Event schemas are versioned but not dynamically evolved. Schema changes require code updates and redeployment. No runtime schema registry initially.

---

## 9. Next Actions

To achieve the three "feels real" milestones:

1. **Refactor existing tools into packages with job entrypoints**
   - ingester: Expose `extract_email()`, `extract_forms()` as callable functions
   - canonizer: Expose `canonicalize_email()`, `canonicalize_questionnaires()`
   - final-form: Implement `score_phq9()`, `score_gad7()`
   - reporter: Implement `generate_weekly_report()`

2. **Implement event_client in lorc**
   - Create `lorc/stack_clients/event_client.py`
   - Implement `write_event()` for BQ and JSONL
   - Add simple query helpers for reading from BQ

3. **Set up BQ tables**
   - Create `event_log` and `raw_objects` tables (two-table pattern)
   - Configure partitioning and clustering
   - Set up JSONL backup directories

4. **Build minimal projections**
   - Implement `inbox_view` for sales pipeline
   - Implement `measurement_timeseries` for measurement pipeline
   - Write simple BQ SQL or Python jobs to populate projections

5. **Wire up lorc job execution**
   - Implement job discovery and registration in lorc
   - Add `lorc run {job_name}` command
   - Test end-to-end flows for all three lanes

---

## 10. Review and Evolution

This document describes the target architecture for the next 2–3 months. It will be reviewed monthly and updated as:

- Milestones are achieved
- New constraints or requirements emerge
- Components graduate from in-scope to operational
- Out-of-scope features are reconsidered (only after core lanes are stable)

**Current Status:** Active (2025-11)
**Next Review:** 2025-12-15
**Owner:** Primary system architect
