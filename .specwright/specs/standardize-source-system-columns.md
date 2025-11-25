---
version: "0.1"
tier: B
title: Standardize Source System Columns & Event Schema
owner: benthepsychologist
goal: Separate concerns in raw_objects and event_log with proper source/connection/type columns, add schema_ref, and add upsert telemetry
labels: [schema, migration, refactor]
orchestrator_contract: "standard"
repo:
  working_branch: "feat/standardize-source-columns"
---

# Standardize Source System Columns & Event Schema

## Objective

> Separate concerns in raw_objects and event_log following industry standards (Airbyte, Singer/Meltano), add Iglu-style schema references, and add upsert telemetry events.

### Background

**Current Problems:**

1. **raw_objects `source_system` conflates provider + connection:**
   - `"google-forms--intake-01"` mixes provider (`google_forms`) with account (`intake-01`)
   - Makes queries awkward: `WHERE source_system LIKE 'gmail%'`

2. **event_log has blurred concerns:**
   - `object_type` on events is confusing (events aren't objects)
   - `idem_key` on events makes no sense (events don't have idem_keys)
   - No distinction between "what happened" (event_type) and "what it was about" (target_object_type)

3. **No upsert telemetry:**
   - We log `ingestion.completed` but not `upsert.completed`
   - No visibility into insert vs update counts, batch timing

4. **No schema versioning:**
   - No `schema_ref` for future schema evolution
   - Can't track which version of a schema produced a record

**Industry Standard Pattern:**
- **source_system**: provider family (`gmail`, `exchange`, `dataverse`, `stripe`)
- **connection_name**: configured account (`gmail-acct2-business1`, `exchange-info-mensio`)
- **object_type** / **target_object_type**: domain object (`email`, `session`, `invoice`)

### Acceptance Criteria

- `raw_objects` schema updated with: `source_system`, `connection_name`, `object_type`, `schema_ref`
- `event_log` schema updated with: `source_system`, `connection_name`, `target_object_type`, `event_schema_ref`
- `idem_key` pattern: `{source_system}:{connection_name}:{object_type}:{external_id}` (opaque, never parsed)
- `upsert.completed` events emitted with insert/update counts
- All ingestion jobs updated to use new column pattern
- Clean re-ingestion of all data (~200MB)
- Test tables updated with same schema

### Constraints

- Re-ingest approach (not SQL surgery on idem_keys)
- Iglu-style schema_ref: `iglu:com.mensio.raw/raw_gmail_email/jsonschema/1-0-0`
- schema_ref nullable for now (all null until Canonizer owns it)

## Target Schemas

### raw_objects (final)

```sql
CREATE TABLE raw_objects (
  idem_key STRING NOT NULL,           -- opaque: {source_system}:{connection_name}:{object_type}:{external_id}
  source_system STRING NOT NULL,      -- provider: gmail, exchange, dataverse, google_forms, stripe
  connection_name STRING NOT NULL,    -- account: gmail-acct2-business1, exchange-info-mensio
  object_type STRING NOT NULL,        -- domain object: email, contact, session, form_response
  schema_ref STRING,                  -- iglu:com.mensio.raw/raw_gmail_email/jsonschema/1-0-0 (nullable)
  external_id STRING,                 -- upstream ID (Gmail message_id, Dataverse GUID, etc.)
  payload JSON NOT NULL,
  first_seen TIMESTAMP NOT NULL,
  last_seen TIMESTAMP NOT NULL
)
CLUSTER BY source_system, connection_name, object_type;
```

### event_log (final)

```sql
CREATE TABLE event_log (
  event_id STRING NOT NULL,
  event_type STRING NOT NULL,         -- verb: job.started, ingest.completed, upsert.completed
  source_system STRING NOT NULL,      -- provider (or 'lorchestra' for system events)
  connection_name STRING,             -- account (NULL for system events)
  target_object_type STRING,          -- noun: email, session, invoice (NULL for job events)
  event_schema_ref STRING,            -- iglu:com.mensio.event/ingest_completed/jsonschema/1-0-0
  correlation_id STRING,
  trace_id STRING,
  created_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  error_message STRING,
  payload JSON
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, event_type;
```

**Dropped from event_log:** `object_type` (renamed to `target_object_type`), `idem_key` (events don't have idem_keys)

### Event Types

| event_type | source_system | connection_name | target_object_type | When |
|------------|---------------|-----------------|-------------------|------|
| `job.started` | `lorchestra` | NULL | NULL | Job begins |
| `job.completed` | `lorchestra` | NULL | NULL | Job ends |
| `ingest.started` | provider | connection | object_type | Ingestion begins |
| `ingest.completed` | provider | connection | object_type | Ingestion ends |
| `ingest.failed` | provider | connection | object_type | Ingestion error |
| `upsert.started` | provider | connection | object_type | Batch upsert begins |
| `upsert.completed` | provider | connection | object_type | Batch upsert ends |
| `upsert.failed` | provider | connection | object_type | Batch upsert error |

### upsert.completed Payload

```json
{
  "total_records": 1024,
  "inserted": 800,
  "updated": 224,
  "batch_count": 5,
  "duration_seconds": 3.21
}
```

### idem_key Pattern

**Format:** `{source_system}:{connection_name}:{object_type}:{external_id}`

**Examples:**
- `gmail:gmail-acct1:email:18c5a7b2e3f4d5c6`
- `dataverse:dataverse-clinic:session:739330df-5757-f011-bec2-6045bd619595`
- `google_forms:google-forms-intake-01:form_response:ACYDBNi84NuJUO2O13A`

**Rule:** Opaque string, never parsed. All filtering uses explicit columns.

## Plan

### Step 1: Schema Design Document [G0: Design Approval]

**Prompt:**

Create detailed schema design document:
1. Document current schema for both tables
2. Document target schema (as above)
3. Write DDL for new tables
4. Write DDL for test tables
5. Document connection_name values for all existing connections

**Outputs:**
- `artifacts/migration/schema-design.md`
- `artifacts/migration/ddl-raw-objects.sql`
- `artifacts/migration/ddl-event-log.sql`

---

### Step 2: Update event_client.py [G1: Code Review]

**Prompt:**

Refactor event_client.py with new schema:

1. **Update `upsert_objects()` signature:**
   ```python
   def upsert_objects(
       *,
       objects: Iterable[Dict],
       source_system: str,        # provider: gmail, dataverse
       connection_name: str,      # account: gmail-acct1
       object_type: str,          # stream: email, session
       schema_ref: Optional[str] = None,  # iglu URI
       correlation_id: str,
       idem_key_fn: Callable,
       bq_client,
       trace_id: Optional[str] = None,
   ) -> UpsertResult:
   ```

2. **Return `UpsertResult` dataclass:**
   ```python
   @dataclass
   class UpsertResult:
       total_records: int
       inserted: int
       updated: int
       batch_count: int
       duration_seconds: float
   ```

3. **Emit `upsert.completed` event** inside `upsert_objects()` with counts

4. **Update `log_event()` signature:**
   ```python
   def log_event(
       *,
       event_type: str,
       source_system: str,
       connection_name: Optional[str] = None,
       target_object_type: Optional[str] = None,
       event_schema_ref: Optional[str] = None,
       correlation_id: str,
       status: str,
       payload: Optional[Dict] = None,
       error_message: Optional[str] = None,
       trace_id: Optional[str] = None,
       bq_client,
   ):
   ```

5. **Update temp table schema and MERGE** to include `connection_name`, `schema_ref`

6. **Update idem_key generation** in callers (not in event_client - callers provide idem_key_fn)

**Outputs:**
- `lorchestra/stack_clients/event_client.py` (updated)

---

### Step 3: Update idem_key Functions [G1: Code Review]

**Prompt:**

Update `lorchestra/idem_keys.py` with new pattern:

1. **New idem_key pattern:** `{source_system}:{connection_name}:{object_type}:{external_id}`

2. **Update all idem_key functions:**
   ```python
   def gmail_idem_key(source_system: str, connection_name: str) -> Callable:
       def compute(obj: dict) -> str:
           msg_id = obj.get("id")
           return f"{source_system}:{connection_name}:email:{msg_id}"
       return compute
   ```

3. **Update for each provider:**
   - `gmail_idem_key`
   - `exchange_idem_key`
   - `dataverse_idem_key`
   - `google_forms_idem_key`

**Outputs:**
- `lorchestra/idem_keys.py` (updated)

---

### Step 4: Update Ingestion Jobs [G1: Code Review]

**Prompt:**

Update all ingestion job modules to use new column pattern:

1. **Gmail jobs** (`ingest_gmail.py`):
   - `source_system = "gmail"`
   - `connection_name = "gmail-acct1"` (etc.)
   - `object_type = "email"`
   - Update `_get_last_sync_timestamp()` to filter on all three columns

2. **Exchange jobs** (`ingest_exchange.py`):
   - `source_system = "exchange"`
   - `connection_name = "exchange-ben-mensio"` (etc.)
   - `object_type = "email"`

3. **Dataverse jobs** (`ingest_dataverse.py`):
   - `source_system = "dataverse"`
   - `connection_name = "dataverse-clinic"`
   - `object_type = "contact"` / `"session"` / `"report"`

4. **Google Forms jobs** (`ingest_google_forms.py`):
   - `source_system = "google_forms"`
   - `connection_name = "google-forms-ipip120"` (etc.)
   - `object_type = "form_response"`

5. **Update log_event calls** to use new parameters

**Outputs:**
- `lorchestra/jobs/ingest_gmail.py` (updated)
- `lorchestra/jobs/ingest_exchange.py` (updated)
- `lorchestra/jobs/ingest_dataverse.py` (updated)
- `lorchestra/jobs/ingest_google_forms.py` (updated)

---

### Step 5: Apply BigQuery Schema Changes [G2: Pre-Release]

**Prompt:**

Execute schema migration on BigQuery:

1. **Archive existing tables (optional):**
   ```sql
   CREATE TABLE raw_objects_legacy AS SELECT * FROM raw_objects;
   CREATE TABLE event_log_legacy AS SELECT * FROM event_log;
   ```

2. **Drop and recreate tables with new schema:**
   - Use DDL from Step 1
   - Apply to both production and test tables

3. **Verify schema:**
   ```sql
   SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS
   WHERE table_name = 'raw_objects';
   ```

**Commands:**

```bash
bq query --use_legacy_sql=false < artifacts/migration/ddl-raw-objects.sql
bq query --use_legacy_sql=false < artifacts/migration/ddl-event-log.sql
```

**Outputs:**
- Schema change execution log
- Before/after schema comparison

---

### Step 6: Re-ingest All Data [G2: Pre-Release]

**Prompt:**

Re-run all ingestion jobs to populate clean data:

1. **Gmail (3 accounts):**
   ```bash
   lorchestra run gmail_ingest_acct1
   lorchestra run gmail_ingest_acct2
   lorchestra run gmail_ingest_acct3
   ```

2. **Exchange (4 accounts):**
   ```bash
   lorchestra run exchange_ingest_ben_mensio
   lorchestra run exchange_ingest_booking_mensio
   lorchestra run exchange_ingest_info_mensio
   lorchestra run exchange_ingest_ben_efs
   ```

3. **Dataverse (3 entities):**
   ```bash
   lorchestra run dataverse_ingest_contacts
   lorchestra run dataverse_ingest_sessions
   lorchestra run dataverse_ingest_reports
   ```

4. **Google Forms (4 forms):**
   ```bash
   lorchestra run google_forms_ingest_ipip120
   lorchestra run google_forms_ingest_intake_01
   lorchestra run google_forms_ingest_intake_02
   lorchestra run google_forms_ingest_followup
   ```

**Outputs:**
- Ingestion completion log with record counts
- Verification queries showing data in new format

---

### Step 7: Validation & Testing [G3: Pre-Release]

**Prompt:**

Validate the migration:

1. **Verify raw_objects data:**
   ```sql
   SELECT source_system, connection_name, object_type, COUNT(*)
   FROM raw_objects
   GROUP BY 1, 2, 3
   ORDER BY 1, 2, 3;
   ```

2. **Verify event_log data:**
   ```sql
   SELECT event_type, source_system, connection_name, target_object_type, COUNT(*)
   FROM event_log
   GROUP BY 1, 2, 3, 4
   ORDER BY 1, 2, 3, 4;
   ```

3. **Verify upsert.completed events exist:**
   ```sql
   SELECT * FROM event_log WHERE event_type = 'upsert.completed' LIMIT 10;
   ```

4. **Verify idem_key format:**
   ```sql
   SELECT idem_key FROM raw_objects LIMIT 5;
   -- Should be: gmail:gmail-acct1:email:18c5a7b2...
   ```

5. **Test queries work:**
   ```sql
   -- All Gmail emails
   SELECT COUNT(*) FROM raw_objects WHERE source_system = 'gmail' AND object_type = 'email';

   -- Specific account
   SELECT COUNT(*) FROM raw_objects WHERE connection_name = 'gmail-acct1';

   -- All emails across providers
   SELECT COUNT(*) FROM raw_objects WHERE object_type = 'email' AND source_system IN ('gmail', 'exchange');
   ```

**Outputs:**
- Validation query results
- Test results summary

---

### Step 8: Update Documentation [G3: Pre-Release]

**Prompt:**

Update documentation:

1. **ARCHITECTURE.md:**
   - Update raw_objects schema section
   - Update event_log schema section
   - Document idem_key pattern
   - Document event types

2. **setup/bigquery-setup.md** (if exists):
   - Update table DDL

**Outputs:**
- `ARCHITECTURE.md` (updated)
- `setup/bigquery-setup.md` (updated)

## Connection Name Reference

| Provider | Identity | connection_name |
|----------|----------|-----------------|
| gmail | gmail:acct1 | gmail-acct1 |
| gmail | gmail:acct2 | gmail-acct2 |
| gmail | gmail:acct3 | gmail-acct3 |
| exchange | exchange:ben-mensio | exchange-ben-mensio |
| exchange | exchange:booking-mensio | exchange-booking-mensio |
| exchange | exchange:info-mensio | exchange-info-mensio |
| exchange | exchange:ben-efs | exchange-ben-efs |
| dataverse | dataverse:clinic | dataverse-clinic |
| google_forms | google_forms:ipip120_01 | google-forms-ipip120 |
| google_forms | google_forms:intake_01 | google-forms-intake-01 |
| google_forms | google_forms:intake_02 | google-forms-intake-02 |
| google_forms | google_forms:followup | google-forms-followup |

## Orchestrator

**State Machine:** Standard (pending → running → awaiting_human → succeeded/failed)

**Tools:** bash, bq, python, git

## Repository

**Branch:** `feat/standardize-source-columns`

**Merge Strategy:** squash
