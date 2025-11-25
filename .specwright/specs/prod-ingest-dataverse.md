---
version: "0.1"
tier: C
title: Prod Ingest Dataverse (Mensio CRM)
owner: benthepsychologist
goal: Ingest Dataverse tables (contacts, sessions, reports) from Mensio CRM to BigQuery using InJest
labels: [dataverse, ingestion, crm]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-11-25T03:29:33.998460+00:00
updated: 2025-11-25T03:29:33.998460+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/prod-ingest-dataverse"
---

# Prod Ingest Dataverse (Mensio CRM)

## Objective

> Ingest Dataverse tables from Mensio CRM (contacts, client sessions, client reports) to BigQuery
> using the new InJest `dataverse.<entity>` stream adapters.

## Acceptance Criteria

- [ ] Dataverse auth working via MSAL device code flow (token cached in `.tokens/`)
- [ ] `lorchestra run dataverse_ingest_contacts` extracts contacts to BigQuery
- [ ] `lorchestra run dataverse_ingest_sessions` extracts cre92_clientsessions to BigQuery
- [ ] `lorchestra run dataverse_ingest_reports` extracts cre92_clientreports to BigQuery
- [ ] All three tables have idem_key deduplication via `@odata.etag`
- [ ] `event_log` shows `ingestion.completed` events for each table
- [ ] Daily incremental sync via `modifiedon` filter works correctly

## Context

### Background

The InJest package (`/workspace/injest`) now has a `DataverseTableStream` adapter that:
- Uses MSAL device code flow for authentication
- Caches tokens in `.tokens/dataverse_clinic.bin`
- Pulls entire tables via OData API with pagination
- Supports `modifiedon` filtering for incremental sync

The identity `dataverse:clinic` is already configured in `injest/config.py`:
```python
"dataverse:clinic": {
    "provider": "dataverse",
    "description": "Mensio CRM - Dataverse",
    "tenant_id_env": "DATAVERSE_TENANT_ID",
    "client_id_env": "DATAVERSE_CLIENT_ID",
    "dataverse_url_env": "DATAVERSE_URL",
    "token_cache_path": ".tokens/dataverse_clinic.bin",
}
```

Environment variables are set in `/workspace/injest/.env`:
```
DATAVERSE_TENANT_ID=f1b164de-53d7-4ace-8793-a89a107592d7
DATAVERSE_CLIENT_ID=af384e69-5c77-4edb-b2b6-87e7bdda5a28
DATAVERSE_URL=https://org2bd13e53.crm3.dynamics.com
```

Target Dataverse entities:
- `contacts` - Client contact records
- `cre92_clientsessions` - Therapy session records
- `cre92_clientreports` - Progress reports

### Constraints

- No edits to `lorchestra/stack_clients/event_client.py` or `lorchestra/idem_keys.py`
- Must use the existing InJest `DataverseTableStream` adapter
- Token cache must be in `/workspace/injest/.tokens/`

## Plan

### Step 1: Dataverse Auth Setup [G0: Auth Validation]

**Prompt:**

Authenticate to Dataverse via device code flow and verify OData access:

1. Ensure environment variables are loaded from `/workspace/injest/.env`
2. Run a test extraction with InJest to trigger device code flow:
   ```python
   from injest import get_stream
   from injest.auth.store import configure_store, ConfigConnectionStore
   from injest.config import load_env_file

   load_env_file()
   configure_store(ConfigConnectionStore())

   stream = get_stream("dataverse.contacts", identity="dataverse:clinic")
   for record in stream.extract():
       print(record.get("fullname"), record.get("@odata.etag"))
       break  # Just test one record
   ```
3. Complete device code flow (visit URL, enter code, authorize)
4. Verify token cache saved to `/workspace/injest/.tokens/dataverse_clinic.bin`
5. Verify contacts data accessible (should see fullname and etag)

**Outputs:**

- Token cache: `/workspace/injest/.tokens/dataverse_clinic.bin`
- Confirmation that OData API returns contact records

---

### Step 2: Add Dataverse idem_key Function [G1: Code Review]

**Prompt:**

Add a `dataverse_idem_key` function to `lorchestra/idem_keys.py`:

```python
def dataverse_idem_key(source_system: str, entity: str) -> Callable[[Dict[str, Any]], str]:
    """
    Dataverse idem_key function - uses entity primary key.

    Dataverse entities use GUIDs as primary keys. The primary key field name
    varies by entity (contactid for contacts, cre92_clientsessionid for sessions, etc).

    We use a predictable pattern: {entity}id (e.g., contactid, cre92_clientsessionid)

    Args:
        source_system: Source system identifier (e.g., "dataverse--clinic")
        entity: Dataverse entity name (e.g., "contacts", "cre92_clientsessions")

    Returns:
        Callable that computes idem_key from Dataverse record payload
    """
    # Standard Dataverse primary key naming: {entity}id (singular form)
    # contacts -> contactid, accounts -> accountid
    # Custom entities: cre92_clientsessions -> cre92_clientsessionid
    if entity.endswith("s") and not entity.startswith("cre92_"):
        pk_field = entity[:-1] + "id"  # contacts -> contactid
    else:
        pk_field = entity + "id"  # cre92_clientsessions -> cre92_clientsessionsid

    def compute_idem_key(obj: Dict[str, Any]) -> str:
        record_id = obj.get(pk_field)
        if not record_id:
            raise ValueError(f"Dataverse {entity} missing '{pk_field}' field: {list(obj.keys())[:5]}")
        return f"{entity}:{source_system}:{record_id}"

    return compute_idem_key
```

**Commands:**

```bash
pytest tests/test_idem_keys.py -v
```

**Outputs:**

- Updated `lorchestra/idem_keys.py`

---

### Step 3: Create Dataverse Ingestion Job Module [G1: Code Review]

**Prompt:**

Create `lorchestra/jobs/ingest_dataverse.py` following the Gmail/Exchange pattern:

1. Import InJest `get_stream` and configure the connection store
2. Create generic `_ingest_dataverse()` function that:
   - Takes `entity`, `source_system`, `identity` parameters
   - Uses `_get_last_sync_timestamp()` for incremental sync on `modifiedon`
   - Calls `upsert_objects()` for batch BigQuery writes
   - Logs `ingestion.completed` telemetry event
3. Create three job functions:
   - `job_ingest_dataverse_contacts(bq_client, since=None, until=None)`
   - `job_ingest_dataverse_sessions(bq_client, since=None, until=None)`
   - `job_ingest_dataverse_reports(bq_client, since=None, until=None)`

Key patterns to follow from `ingest_gmail.py`:
- Use `configure_injest()` at start of each job
- Query BigQuery for last sync if no `--since` provided
- Wrap extraction in try/except and log `ingestion.failed` on error
- Track `record_count` for telemetry payload

Source system naming: `dataverse--clinic-{entity}` (e.g., `dataverse--clinic-contacts`)

**Outputs:**

- `lorchestra/jobs/ingest_dataverse.py`

---

### Step 4: Register Job Entrypoints [G1: Code Review]

**Prompt:**

Add Dataverse job entrypoints to `pyproject.toml`:

```toml
[project.entry-points."lorchestra.jobs"]
# ... existing jobs ...
dataverse_ingest_contacts = "lorchestra.jobs.ingest_dataverse:job_ingest_dataverse_contacts"
dataverse_ingest_sessions = "lorchestra.jobs.ingest_dataverse:job_ingest_dataverse_sessions"
dataverse_ingest_reports = "lorchestra.jobs.ingest_dataverse:job_ingest_dataverse_reports"
```

Reinstall the package to register entrypoints:

```bash
pip install -e .
lorchestra jobs list
```

**Commands:**

```bash
pip install -e .
lorchestra jobs list | grep dataverse
```

**Outputs:**

- Updated `pyproject.toml`
- Verification that `lorchestra jobs list` shows `dataverse_ingest_*` jobs

---

### Step 5: Test Ingestion [G2: Pre-Release]

**Prompt:**

Run each Dataverse ingestion job and validate BigQuery results:

1. **Contacts:**
   ```bash
   lorchestra run dataverse_ingest_contacts
   ```
   Verify: `SELECT COUNT(*) FROM raw_objects WHERE source_system = 'dataverse--clinic-contacts'`

2. **Sessions:**
   ```bash
   lorchestra run dataverse_ingest_sessions
   ```
   Verify: `SELECT COUNT(*) FROM raw_objects WHERE source_system = 'dataverse--clinic-cre92_clientsessions'`

3. **Reports:**
   ```bash
   lorchestra run dataverse_ingest_reports
   ```
   Verify: `SELECT COUNT(*) FROM raw_objects WHERE source_system = 'dataverse--clinic-cre92_clientreports'`

4. Check `event_log` for `ingestion.completed` events:
   ```sql
   SELECT event_type, source_system, created_at,
          JSON_EXTRACT_SCALAR(payload, '$.records_extracted') as records
   FROM event_log
   WHERE source_system LIKE 'dataverse%'
   ORDER BY created_at DESC
   LIMIT 10
   ```

**Outputs:**

- `artifacts/test/dataverse-ingestion-results.md` with counts and event log entries

---

### Step 6: Add to Daily Ingest Script [G3: Integration]

**Prompt:**

Update `scripts/daily_ingest.sh` to include Dataverse jobs:

```bash
# Dataverse tables (CRM data)
lorchestra run dataverse_ingest_contacts
lorchestra run dataverse_ingest_sessions
lorchestra run dataverse_ingest_reports
```

**Outputs:**

- Updated `scripts/daily_ingest.sh`

## Models & Tools

**Tools:** bash, pytest, python, lorchestra

**Models:** (defaults)

## Repository

**Branch:** `feat/prod-ingest-dataverse`

**Merge Strategy:** squash
