# Configuration Reference

Complete reference for `config/pipeline.yaml` configuration file.

## 📋 Table of Contents

- [Pipeline Section](#pipeline-section)
- [Stages Section](#stages-section)
  - [Extract Stage](#extract-stage)
  - [Canonize Stage](#canonize-stage)
  - [Index Stage](#index-stage)
- [Logging Section](#logging-section)
- [Security Section](#security-section)
- [Behavior Section](#behavior-section)
- [Environment Variables](#environment-variables)
- [Examples](#examples)

---

## Pipeline Section

Top-level pipeline metadata.

```yaml
pipeline:
  name: string              # Pipeline name (required)
  version: string           # Semantic version (required)
  description: string       # Human-readable description (optional)
```

**Example:**

```yaml
pipeline:
  name: phi-local-pipeline
  version: 1.0.0
  description: Local-first PHI data processing pipeline
```

---

## Stages Section

Configuration for each pipeline stage.

### Extract Stage

Meltano extraction configuration.

```yaml
stages:
  extract:
    type: meltano                           # Stage type (required)
    enabled: boolean                         # Enable/disable stage (default: true)
    repo_path: path                          # Path to meltano repo (required)
    venv_path: path                          # Path to meltano venv (required)
    job: string                              # Meltano job name (required)
    output_dir: path                         # Output directory (required)

    retry:
      enabled: boolean                       # Enable retries (default: false)
      max_attempts: integer                  # Max retry attempts (default: 3)
      backoff_seconds: integer               # Initial backoff (default: 60)
      backoff_multiplier: float              # Backoff multiplier (default: 2.0)

    validation:
      require_files: boolean                 # Require output files (default: true)
      min_records: integer                   # Minimum records (default: 0)
```

**Example:**

```yaml
extract:
  type: meltano
  enabled: true
  repo_path: /home/user/meltano-ingest
  venv_path: /home/user/meltano-ingest/.venv
  job: ingest-all-accounts
  output_dir: /home/user/phi-data/meltano-extracts
  retry:
    enabled: true
    max_attempts: 3
    backoff_seconds: 60
    backoff_multiplier: 2
  validation:
    require_files: true
    min_records: 1
```

### Canonize Stage

Data transformation configuration.

```yaml
stages:
  canonize:
    type: canonizer                          # Stage type (required)
    enabled: boolean                         # Enable/disable stage (default: true)
    repo_path: path                          # Path to canonizer repo (required)
    venv_path: path                          # Path to canonizer venv (required)
    transform_registry: path                 # Transform registry directory (required)
    input_dir: path                          # Input directory (required)
    output_dir: path                         # Output directory (required)

    # Transform mappings
    mappings:
      - source_pattern: glob                 # Glob pattern for input files (required)
        transform: string                    # Transform name (required)
        output_name: string                  # Output file base name (required)

    retry:
      enabled: boolean                       # Enable retries (default: false)
      max_attempts: integer                  # Max retry attempts (default: 2)
      backoff_seconds: integer               # Initial backoff (default: 30)

    validation:
      validate_schemas: boolean              # Validate against schemas (default: true)
      skip_invalid: boolean                  # Skip vs fail on invalid (default: true)
      require_checksums: boolean             # Verify transform checksums (default: true)
```

**Example:**

```yaml
canonize:
  type: canonizer
  enabled: true
  repo_path: /home/user/canonizer
  venv_path: /home/user/canonizer/.venv
  transform_registry: /home/user/transforms
  input_dir: /home/user/phi-data/meltano-extracts
  output_dir: /home/user/phi-data/canonical

  mappings:
    - source_pattern: "*gmail*.jsonl"
      transform: email/gmail_to_canonical_v1
      output_name: email

    - source_pattern: "*exchange*.jsonl"
      transform: email/exchange_to_canonical_v1
      output_name: email

    - source_pattern: "*contacts*.jsonl"
      transform: contact/dataverse_contact_to_canonical_v1
      output_name: contact

  retry:
    enabled: true
    max_attempts: 2
    backoff_seconds: 30

  validation:
    validate_schemas: true
    skip_invalid: true
    require_checksums: true
```

### Index Stage

Document store indexing configuration.

```yaml
stages:
  index:
    type: vector-projector                   # Stage type (required)
    enabled: boolean                         # Enable/disable stage (default: true)
    repo_path: path                          # Path to vector-projector repo (required)
    venv_path: path                          # Path to vector-projector venv (optional)
    input_dir: path                          # Input directory (required)
    output_dir: path                         # Output directory (required)
    mode: string                             # Mode: "stub" or "full" (default: "stub")

    retry:
      enabled: boolean                       # Enable retries (default: false)

    validation:
      require_files: boolean                 # Require input files (default: true)
```

**Example:**

```yaml
index:
  type: vector-projector
  enabled: true
  repo_path: /home/user/vector-projector
  input_dir: /home/user/phi-data/canonical
  output_dir: /home/user/phi-data/vector-store
  mode: stub  # "stub" or "full"

  retry:
    enabled: false

  validation:
    require_files: true
```

---

## Logging Section

Logging configuration.

```yaml
logging:
  level: string                 # DEBUG, INFO, WARNING, ERROR (default: INFO)
  format: string                # "structured" (JSON) or "pretty" (default: structured)
  output: string                # Log file path template (supports {date})
  console: boolean              # Also log to console (default: true)
  include_timestamps: boolean   # Include timestamps (default: true)

  include:                      # What to log
    - stage_start
    - stage_end
    - record_counts
    - file_paths
    - errors
    - warnings

  exclude:                      # What NOT to log
    - record_content            # NEVER log PHI data
```

**Example:**

```yaml
logging:
  level: INFO
  format: structured
  output: logs/pipeline-{date}.log
  console: true
  include_timestamps: true

  include:
    - stage_start
    - stage_end
    - record_counts
    - file_paths
    - errors
    - warnings

  exclude:
    - record_content  # PHI protection
```

---

## Security Section

PHI security configuration.

```yaml
security:
  phi_directories:              # List of PHI directories to validate
    - path
    - path

  enforce_permissions: boolean  # Enforce permission checks (default: true)
  required_perms: string        # Required permission mode (default: "700")
  validate_before_run: boolean  # Validate before execution (default: true)
```

**Example:**

```yaml
security:
  phi_directories:
    - /home/user/phi-data
    - /home/user/phi-data/meltano-extracts
    - /home/user/phi-data/canonical
    - /home/user/phi-data/vector-store

  enforce_permissions: true
  required_perms: "700"
  validate_before_run: true
```

---

## Behavior Section

Pipeline execution behavior.

```yaml
behavior:
  fail_fast: boolean            # Stop on first error (default: false)
  cleanup_on_success: boolean   # Clean temp files on success (default: false)
  cleanup_on_failure: boolean   # Clean temp files on failure (default: false)
  parallel_execution: boolean   # Run stages in parallel (default: false)
```

**Example:**

```yaml
behavior:
  fail_fast: false              # Continue through all stages
  cleanup_on_success: false     # Keep files for inspection
  cleanup_on_failure: false     # Keep files for debugging
  parallel_execution: false     # Sequential execution
```

---

## Environment Variables

Configuration can reference environment variables:

```yaml
stages:
  extract:
    repo_path: ${MELTANO_REPO_PATH}
    output_dir: ${PHI_DATA_DIR}/meltano-extracts
```

**Usage:**

```bash
export MELTANO_REPO_PATH=/home/user/meltano-ingest
export PHI_DATA_DIR=/home/user/phi-data

lorchestra run
```

---

## Examples

### Minimal Configuration

```yaml
pipeline:
  name: minimal-pipeline
  version: 1.0.0

stages:
  extract:
    type: meltano
    repo_path: /home/user/meltano-ingest
    venv_path: /home/user/meltano-ingest/.venv
    job: ingest-all-accounts
    output_dir: /home/user/phi-data/meltano-extracts

  canonize:
    type: canonizer
    repo_path: /home/user/canonizer
    venv_path: /home/user/canonizer/.venv
    transform_registry: /home/user/transforms
    input_dir: /home/user/phi-data/meltano-extracts
    output_dir: /home/user/phi-data/canonical
    mappings:
      - source_pattern: "*.jsonl"
        transform: email/gmail_to_canonical_v1
        output_name: email

  index:
    type: vector-projector
    repo_path: /home/user/vector-projector
    input_dir: /home/user/phi-data/canonical
    output_dir: /home/user/phi-data/vector-store
    mode: stub
```

### Production Configuration

```yaml
pipeline:
  name: phi-production-pipeline
  version: 2.0.0
  description: Production PHI data pipeline with full monitoring

stages:
  extract:
    type: meltano
    enabled: true
    repo_path: /home/user/meltano-ingest
    venv_path: /home/user/meltano-ingest/.venv
    job: ingest-all-accounts
    output_dir: /home/user/phi-data/meltano-extracts
    retry:
      enabled: true
      max_attempts: 5
      backoff_seconds: 120
      backoff_multiplier: 2
    validation:
      require_files: true
      min_records: 10

  canonize:
    type: canonizer
    enabled: true
    repo_path: /home/user/canonizer
    venv_path: /home/user/canonizer/.venv
    transform_registry: /home/user/transforms
    input_dir: /home/user/phi-data/meltano-extracts
    output_dir: /home/user/phi-data/canonical
    mappings:
      - source_pattern: "*gmail*.jsonl"
        transform: email/gmail_to_canonical_v2
        output_name: email
      - source_pattern: "*exchange*.jsonl"
        transform: email/exchange_to_canonical_v2
        output_name: email
      - source_pattern: "*contacts*.jsonl"
        transform: contact/dataverse_contact_to_canonical_v2
        output_name: contact
      - source_pattern: "*sessions*.jsonl"
        transform: clinical_session/dataverse_session_to_canonical_v1
        output_name: clinical_session
    retry:
      enabled: true
      max_attempts: 3
      backoff_seconds: 60
    validation:
      validate_schemas: true
      skip_invalid: false  # Fail on invalid data
      require_checksums: true

  index:
    type: vector-projector
    enabled: true
    repo_path: /home/user/vector-projector
    venv_path: /home/user/vector-projector/.venv
    input_dir: /home/user/phi-data/canonical
    output_dir: /home/user/phi-data/vector-store
    mode: full  # Full SQLite + inode storage
    retry:
      enabled: true
      max_attempts: 2
    validation:
      require_files: true

logging:
  level: INFO
  format: structured
  output: logs/pipeline-{date}.log
  console: true
  include_timestamps: true

security:
  phi_directories:
    - /home/user/phi-data
    - /home/user/phi-data/meltano-extracts
    - /home/user/phi-data/canonical
    - /home/user/phi-data/vector-store
  enforce_permissions: true
  required_perms: "700"
  validate_before_run: true

behavior:
  fail_fast: true  # Stop on first error in production
  cleanup_on_success: false
  cleanup_on_failure: false
  parallel_execution: false
```

### Development Configuration

```yaml
pipeline:
  name: phi-dev-pipeline
  version: 0.1.0
  description: Development pipeline with verbose logging

stages:
  extract:
    type: meltano
    enabled: false  # Skip in dev, use test fixtures
    repo_path: /home/user/meltano-ingest
    venv_path: /home/user/meltano-ingest/.venv
    job: ingest-test-account
    output_dir: /tmp/phi-data-dev/meltano-extracts

  canonize:
    type: canonizer
    enabled: true
    repo_path: /home/user/canonizer
    venv_path: /home/user/canonizer/.venv
    transform_registry: /home/user/transforms
    input_dir: /tmp/phi-data-dev/meltano-extracts
    output_dir: /tmp/phi-data-dev/canonical
    mappings:
      - source_pattern: "*.jsonl"
        transform: email/gmail_to_canonical_v1
        output_name: email
    retry:
      enabled: false  # Fail fast in dev
    validation:
      validate_schemas: true
      skip_invalid: true  # Continue on errors
      require_checksums: false  # Allow modified transforms

  index:
    type: vector-projector
    enabled: true
    repo_path: /home/user/vector-projector
    input_dir: /tmp/phi-data-dev/canonical
    output_dir: /tmp/phi-data-dev/vector-store
    mode: stub

logging:
  level: DEBUG  # Verbose in dev
  format: pretty  # Human-readable in dev
  output: logs/dev-{date}.log
  console: true

security:
  phi_directories:
    - /tmp/phi-data-dev
  enforce_permissions: false  # Skip in dev
  required_perms: "700"
  validate_before_run: false

behavior:
  fail_fast: true  # Fail fast for quick iteration
  cleanup_on_success: false
  cleanup_on_failure: false
  parallel_execution: false
```

---

## Validation

Validate your configuration:

```bash
lorchestra validate --config config/pipeline.yaml
```

**Common Errors:**

- **Missing required field:** Add the required field to config
- **Invalid path:** Check that paths exist and are accessible
- **Permission errors:** Ensure PHI directories have 700 permissions
- **Invalid YAML syntax:** Check YAML indentation and structure

---

## See Also

- [README.md](../README.md) - Project overview
- [docs/architecture.md](architecture.md) - System architecture
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Development guide
