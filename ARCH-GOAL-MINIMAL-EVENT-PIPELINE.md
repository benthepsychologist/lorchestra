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

### 2.2 BigQuery as Write-Ahead Log

BigQuery is the primary storage system for all events. It serves as the write-ahead log (WAL) and source of truth. All downstream projections, views, and reports are derived from BQ tables.

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

**Role:** Unified event writing interface

**Location:** Inside lorc repo at `lorc/stack_clients/event_client.py`

**Responsibilities:**
- Write events to BigQuery (primary)
- Write events to JSONL backup files (secondary)
- Enforce event schema validation
- Handle partitioning and clustering hints
- Provide simple query helpers for reading events from BQ

**Design:**
- Simple, focused API: `write_event(event_type, payload, metadata)`
- No complex batching or streaming logic initially
- May be extracted to a separate system-clients repo in the future

### 3.3 ingester

**Role:** Domain package for raw event extraction from external systems

**Responsibilities:**
- Extract raw data from Gmail, Exchange, Google Sheets, QuickBooks, etc.
- Emit raw events via event_client (e.g., `email.received`, `form.submitted`)
- Provide job entrypoints callable by lorc
- Handle API authentication and rate limiting

**Does NOT:**
- Normalize or canonicalize data (that's canonizer's job)
- Write directly to BQ (delegates to event_client)

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

### 4.1 Raw Event Emission

1. lorc invokes an ingester job (e.g., `extract_gmail_messages`)
2. Ingester connects to external API (Gmail)
3. For each message retrieved, ingester calls `event_client.write_event()`:
   - Event type: `email.received`
   - Payload: Raw message data (headers, body, metadata)
   - Metadata: Timestamp, source account, extraction run ID
4. event_client writes to:
   - BigQuery table: `raw_events` (or `events` with `event_type` field)
   - JSONL backup: `backup/raw/YYYY-MM-DD/run_{run_id}.jsonl`

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

### 5.1 BigQuery as Primary WAL

All events are written to BigQuery as the primary, authoritative storage.

**Table Design:**
- Option A: Unified `events` table with `event_type` field
- Option B: Separate `raw_events` and `canonical_events` tables
- Partitioning: By `event_timestamp` (daily or hourly)
- Clustering: By `event_type`, `source_account`, or domain-specific fields

**Retention:**
- All events are retained indefinitely (immutable log)
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
| event_client | Write events to BQ and JSONL, simple query helpers |
| ingester jobs | Extract email (Gmail, Exchange), forms (Google Sheets), QuickBooks |
| canonizer jobs | Canonicalize email, questionnaire responses |
| final-form jobs | Score PHQ-9, GAD-7 measurements |
| reporter jobs | Generate and send weekly client reports |
| BQ event tables | raw_events, canonical_events (or unified events table) |
| JSONL backups | Simple one-file-per-run backup mechanism |
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
   - Create `raw_events` and `canonical_events` tables (or unified `events`)
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
