# lorchestra Architecture

## System Overview

lorchestra is a **local-first orchestrator** that coordinates a three-stage data pipeline for PHI (Protected Health Information) processing.

```
┌──────────────────────────────────────────────────────────────────┐
│                         lorchestra Orchestrator                        │
│                                                                   │
│  ┌─────────────┐      ┌────────────┐      ┌─────────────────┐  │
│  │   Stage 1   │  →   │  Stage 2   │  →   │    Stage 3      │  │
│  │   Extract   │      │ Canonize   │      │     Index       │  │
│  └─────────────┘      └────────────┘      └─────────────────┘  │
│         ↓                     ↓                     ↓            │
└──────────────────────────────────────────────────────────────────┘
         ↓                     ↓                     ↓
    Raw JSONL          Canonical JSON          Local Store
  /phi-data/          /phi-data/            /phi-data/
  meltano-extracts/   canonical/            vector-store/
```

## Design Principles

### 1. **Separation of Concerns**

Each component has a single, well-defined responsibility:

- **meltano-ingest**: Data extraction from sources (Gmail, Exchange, Dataverse)
- **canonizer**: Pure transformation (source format → canonical format)
- **vector-projector**: Local storage and indexing
- **lorch**: Orchestration and coordination only

lorchestra does NOT:
- Extract data itself
- Transform data itself
- Store data itself

lorchestra DOES:
- Call other components via CLI/subprocess
- Validate between stages
- Handle retries and error recovery
- Manage pipeline configuration
- Provide unified logging

### 2. **Pure Functions**

Each stage is designed as a **pure function**:

```
Extract:   (config) → raw_data
Canonize:  (raw_data, transforms) → canonical_data
Index:     (canonical_data, config) → indexed_store
```

This enables:
- Easy testing (deterministic inputs/outputs)
- Parallel execution (where applicable)
- Cloud translation (each stage becomes a serverless function)

### 3. **Local-First, Cloud-Ready**

Design decisions prioritize local execution but enable cloud migration:

| Local Component | Cloud Equivalent | Migration Path |
|----------------|------------------|----------------|
| Meltano CLI | Cloud Run Job | Containerize meltano, run on schedule |
| Canonizer subprocess | Cloud Function | Package canonizer, trigger on GCS upload |
| Vector-projector files | BigQuery table | Change output from files to BQ client |
| lorchestra orchestrator | Cloud Workflows | Convert Python logic to YAML workflow |
| Subprocess calls | HTTP/PubSub | Replace `subprocess.run()` with API calls |
| File I/O | GCS buckets | Replace local paths with `gs://` URIs |

See [cloud-migration.md](cloud-migration.md) for detailed migration guide.

### 4. **Security by Default**

PHI data is protected at every stage:

- **700 permissions** on all PHI directories (owner-only access)
- **600 permissions** on all PHI files
- **No PHI in logs** - only counts, timestamps, and metadata
- **No PHI in error messages** - sanitized exceptions
- **Validation before execution** - check permissions before running

## Pipeline Stages

### Stage 1: Extract (Meltano)

**Purpose:** Pull data from configured sources

**Component:** `/home/user/meltano-ingest`

**Invocation:**
```bash
cd /home/user/meltano-ingest
source .venv/bin/activate
meltano run ingest-all-accounts
```

**Input:** Source API credentials (from `.env`)

**Output:** Raw JSONL files in `/home/user/phi-data/meltano-extracts/`

**Format:** Newline-delimited JSON (JSONL)
- One JSON object per line
- Preserves full API response structure
- UTF-8 encoded

**Example Output Files:**
```
/home/user/phi-data/meltano-extracts/
├── gmail-messages-acct1-personal-20251112.jsonl
├── exchange-messages-ben-mensio-20251112.jsonl
└── contacts-dataverse-20251112.jsonl
```

**Failure Handling:**
- Meltano maintains state bookmarks for incremental extraction
- Failed sources continue to next source
- lorchestra validates at least one file produced before proceeding

### Stage 2: Canonize (Canonizer)

**Purpose:** Transform source formats to canonical schemas

**Component:** `/home/user/canonizer`

**Invocation:**
```bash
cd /home/user/canonizer
source .venv/bin/activate
cat input.jsonl | can transform run \
  --meta /home/user/transforms/email/gmail_to_canonical_v1.meta.yaml \
  > output.jsonl
```

**Input:** Raw JSONL from Stage 1

**Output:** Canonical JSON in `/home/user/phi-data/canonical/`

**Transform Registry:** `/home/user/transforms/`
- JSONata transformation definitions
- Metadata files with checksums and versions
- Canonical schemas (Iglu SchemaVer format)

**Mapping Configuration:**
```yaml
mappings:
  - source_pattern: "*gmail*.jsonl"
    transform: email/gmail_to_canonical_v1
  - source_pattern: "*exchange*.jsonl"
    transform: email/exchange_to_canonical_v1
  - source_pattern: "*contacts*.jsonl"
    transform: contact/dataverse_contact_to_canonical_v1
```

**Example Transformation:**

Input (Gmail API format):
```json
{
  "id": "abc123",
  "payload": {
    "headers": [
      {"name": "From", "value": "sender@example.com"},
      {"name": "Subject", "value": "Test"}
    ]
  }
}
```

Output (Canonical email format):
```json
{
  "message_id": "abc123",
  "from": "sender@example.com",
  "subject": "Test",
  "schema": "iglu:org.canonical/email/jsonschema/1-0-0"
}
```

**Failure Handling:**
- Validation against input/output schemas
- Checksum verification of transform files
- Line-by-line processing (one failure doesn't stop entire file)
- Invalid records logged but skipped

### Stage 3: Index (Vector-projector)

**Purpose:** Store canonical data in queryable local store

**Component:** `/home/user/vector-projector`

**Current Status:** Stubbed (v0.1.0)

**Stub Implementation:**
```bash
# Copy canonical JSON to destination directory
cp /home/user/phi-data/canonical/*.jsonl \
   /home/user/phi-data/vector-store/
```

**Future Implementation:**

Invocation:
```bash
cd /home/user/vector-projector
source .venv/bin/activate
vector-projector pull \
  --input /home/user/phi-data/canonical/ \
  --config config.yaml
```

**Input:** Canonical JSON from Stage 2

**Output:**
```
/home/user/phi-data/vector-store/
├── objects/
│   ├── AA/AB/email_AABCDEF123456.json
│   ├── AB/CD/contact_ABCDFGH789012.json
│   └── ...
├── registry/
│   ├── email.json       # Fast lookup by type
│   └── contact.json
└── metadata.db          # SQLite index
```

**Storage Pattern:**
- Inode-style directory structure (scalable to millions of objects)
- Per-type registries for fast enumeration
- SQLite for efficient querying

**Failure Handling:**
- Transaction-based inserts (all-or-nothing per file)
- Duplicate detection (skip if object ID already indexed)
- Validation against canonical schemas

## Data Flow

### File Naming Convention

**After Extract:**
```
{source}-{entity}-{account}-{date}.jsonl
```
Examples:
- `gmail-messages-acct1-personal-20251112.jsonl`
- `exchange-messages-ben-mensio-20251112.jsonl`
- `contacts-dataverse-20251112.jsonl`

**After Canonize:**
```
{canonical_type}-{date}.jsonl
```
Examples:
- `email-20251112.jsonl` (from gmail + exchange)
- `contact-20251112.jsonl` (from dataverse)
- `clinical_session-20251112.jsonl` (from dataverse)

**After Index:**
```
objects/{hash_prefix}/{type}_{object_id}.json
```
Examples:
- `objects/AA/AB/email_AABCDEF123456.json`
- `objects/CD/EF/contact_CDEFGH789012.json`

### State Management

lorchestra maintains pipeline state in `logs/state.json`:

```json
{
  "last_run": "2025-11-12T10:30:45Z",
  "status": "SUCCESS",
  "stages": {
    "extract": {
      "status": "SUCCESS",
      "duration_seconds": 105,
      "records_extracted": 27,
      "output_files": [
        "/home/user/phi-data/meltano-extracts/gmail-messages-acct1-personal-20251112.jsonl"
      ]
    },
    "canonize": {
      "status": "SUCCESS",
      "duration_seconds": 132,
      "records_transformed": 27,
      "output_files": [
        "/home/user/phi-data/canonical/email-20251112.jsonl"
      ]
    },
    "index": {
      "status": "SUCCESS",
      "duration_seconds": 35,
      "records_indexed": 27
    }
  }
}
```

## Error Handling

### Retry Strategy

Configurable per stage:

```yaml
stages:
  extract:
    retry:
      enabled: true
      max_attempts: 3
      backoff_seconds: 60
      backoff_multiplier: 2
```

**Backoff Sequence:** 60s, 120s, 240s

### Failure Scenarios

| Scenario | Detection | Recovery |
|----------|-----------|----------|
| Meltano authentication failure | Exit code 1 | Retry with backoff |
| Invalid JSONL output | JSON parse error | Fail fast (data issue) |
| Transform not found | File not found error | Fail fast (config issue) |
| Permission denied | OS error 13 | Fail fast + warn user |
| Disk full | OS error 28 | Fail fast + alert |
| Canonization schema mismatch | Validation error | Skip record, log warning |

### Logging

**Format:** Structured JSON

**Location:** `logs/pipeline-{date}.log`

**Fields:**
```json
{
  "timestamp": "2025-11-12T10:30:45.123Z",
  "level": "INFO",
  "stage": "extract",
  "event": "stage_started",
  "metadata": {
    "config": "/home/user/meltano-ingest",
    "job": "ingest-all-accounts"
  }
}
```

**No PHI in logs** - only:
- Timestamps
- Record counts
- File paths
- Error messages (sanitized)

## Security Architecture

### PHI Directory Isolation

```
/home/user/
├── phi-data/                  # 700 (rwx------)
│   ├── meltano-extracts/     # 700
│   ├── canonical/            # 700
│   └── vector-store/         # 700
├── meltano-ingest/           # 755 (no PHI)
├── canonizer/                # 755 (no PHI)
├── vector-projector/         # 755 (no PHI)
├── transforms/               # 755 (no PHI)
└── lorchestra/                    # 755 (no PHI)
```

**Validation:** lorchestra checks permissions before each run:

```python
def validate_phi_permissions(path: Path):
    stat = path.stat()
    mode = stat.st_mode & 0o777
    if mode != 0o700:
        raise PermissionError(
            f"PHI directory {path} has insecure permissions {oct(mode)}. "
            f"Expected 0o700. Run: chmod 700 {path}"
        )
```

### Encryption

- **At rest:** Linux filesystem encryption (LUKS)
- **In transit:** N/A (local-only pipeline)
- **In cloud:** TLS + GCS server-side encryption

## Performance

### Typical Pipeline Execution

**Hardware:** Cloud Workstation (4 vCPU, 16GB RAM)

| Stage | Duration | Throughput | Bottleneck |
|-------|----------|------------|------------|
| Extract | 1-2 min | ~200 msgs/min | API rate limits |
| Canonize | 2-3 min | ~1000 records/min | JSON parsing |
| Index (stub) | <1 min | ~5000 records/min | File I/O |

**Total Pipeline:** ~4-6 minutes for 8 sources, ~1000 records

### Optimization Opportunities

1. **Parallel extraction:** Run multiple Meltano jobs concurrently
2. **Streaming transforms:** Pipe extract → canonize without intermediate files
3. **Batch indexing:** Insert records in batches vs. one-by-one

## Testing

### Unit Tests

Test individual stages in isolation:

```bash
pytest tests/stages/test_extract.py
pytest tests/stages/test_canonize.py
pytest tests/stages/test_index.py
```

### Integration Tests

Test full pipeline with fake data:

```bash
pytest tests/test_integration.py
```

Uses test fixtures from `/home/user/meltano-ingest/tests/fixtures/`

### Manual Testing

```bash
# Dry run (validation only)
lorchestra run --dry-run

# Single stage
lorchestra run --stage extract

# Full pipeline with verbose logging
lorchestra run --verbose
```

## Future Enhancements

### Phase 2: Parallel Execution

Run independent extraction jobs in parallel:

```python
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    futures = []
    for account in accounts:
        future = executor.submit(run_meltano_job, account)
        futures.append(future)

    results = [f.result() for f in futures]
```

### Phase 3: Streaming Pipeline

Eliminate intermediate files with streaming:

```bash
meltano run tap-gmail target-jsonl | \
  can transform run --meta transforms/email/gmail_to_canonical_v1.meta.yaml | \
  vector-projector ingest --streaming
```

### Phase 4: Monitoring Dashboard

Web UI for pipeline monitoring:
- Real-time stage progress
- Historical run statistics
- Error alerts
- Record count trends

### Phase 5: Cloud Migration

See [cloud-migration.md](cloud-migration.md) for detailed plan.

## Glossary

- **PHI**: Protected Health Information (HIPAA-regulated data)
- **JSONL**: Newline-delimited JSON (one JSON object per line)
- **Canonical format**: Normalized schema shared across all sources
- **Transform registry**: Centralized repository of JSONata transforms
- **Iglu SchemaVer**: Schema versioning format (MODEL-REVISION-ADDITION)
- **Inode-style storage**: Directory structure based on hash prefixes
