---
version: "0.1"
tier: C
title: Add --dry-run and --test-table Flags to lorchestra CLI
owner: benthepsychologist
goal: Add safe development modes for testing new streams without affecting production BigQuery tables
labels: [cli, testing, safety]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-11-25T04:00:00.000000+00:00
updated: 2025-11-25T04:00:00.000000+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/dry-run-test-table"
---

# Add --dry-run and --test-table Flags to lorchestra CLI

## Objective

> Add `--dry-run` and `--test-table` to `lorchestra run` so we can develop new ingestion streams
> without polluting or risking production BigQuery tables.

**Modes:**

1. **`--dry-run`**
   - Executes adapters and idem-key logic
   - No BigQuery writes
   - Logs summary to console (counts + small samples)

2. **`--test-table`**
   - Executes full ingestion end-to-end
   - Writes to `test_event_log` and `test_raw_objects`
   - Never touches prod tables

**Flags are mutually exclusive.**

## Acceptance Criteria

- [ ] `lorchestra run <job> --dry-run`:
  - Calls source adapters normally
  - Performs `idem_key_fn` on each record
  - Writes zero rows to any BigQuery table
  - Prints: extracted record count, 1-3 sample idem_keys, truncated sample payload
- [ ] `lorchestra run <job> --test-table`:
  - Writes events to `test_event_log`
  - Upserts objects to `test_raw_objects`
  - Does not write to prod `event_log` / `raw_objects`
- [ ] `--dry-run` and `--test-table` cannot be combined; CLI errors fast
- [ ] Test tables auto-create if missing and match prod schema exactly
- [ ] Existing jobs do not require signature changes
- [ ] Unit tests cover:
  - dry-run: no BQ write methods invoked
  - test-table: sinks point to test tables
- [ ] CI green (ruff + pytest)

## Context

### Background

We now have real data in prod BigQuery tables. Adding new streams (Dataverse, future sources)
currently risks:
- Polluting prod with half-baked schemas
- Forced manual cleanup
- Increased reluctance to run end-to-end during development

We need safe modes at the CLI + sink layer.

### Current Table Names

Production tables (from env vars):
- `EVENT_LOG_TABLE` -> `event_log`
- `RAW_OBJECTS_TABLE` -> `raw_objects`

Test tables (matching prod naming):
- `test_event_log`
- `test_raw_objects`

### Constraints

- No schema changes to prod tables
- No edits to `lorchestra/stack_clients/event_client.py` core logic (only add mode routing)
- Keep implementation thin: switches + sink routing only
- Flags are mutually exclusive (no combination logic)

## Plan

### Step 1: Add CLI Flags [G0: Design Review]

**Prompt:**

Modify `lorchestra run` in `lorchestra/cli.py` to accept new flags:

```python
@main.command("run")
@click.argument("job")
@click.option("--account", help="Account identifier")
@click.option("--since", help="Start time (ISO or relative)")
@click.option("--until", help="End time (ISO)")
@click.option("--dry-run", is_flag=True, help="Extract records without writing to BigQuery")
@click.option("--test-table", is_flag=True, help="Write to test_event_log/test_raw_objects instead of prod")
def run(job: str, dry_run: bool, test_table: bool, **kwargs):
    ...
```

Error fast if both flags set:

```python
if dry_run and test_table:
    raise click.UsageError("--dry-run and --test-table are mutually exclusive")
```

Print clear banner at start showing active mode:
- `=== DRY RUN MODE === (no BigQuery writes)`
- `=== TEST TABLE MODE === (writing to test_event_log, test_raw_objects)`

Also update `run-job` alias with same flags.

**Outputs:**

- Updated `lorchestra/cli.py`

---

### Step 2: Add Run-Mode Context to Sink Layer [G1: Code Review]

**Prompt:**

In `lorchestra/stack_clients/event_client.py`, add run-mode setters and routing:

```python
# Module-level run mode context
_DRY_RUN_MODE = False
_TEST_TABLE_MODE = False

def set_run_mode(*, dry_run: bool = False, test_table: bool = False) -> None:
    """Set the run mode for event_client operations.

    Call this once at CLI entry before job execution.
    """
    global _DRY_RUN_MODE, _TEST_TABLE_MODE
    _DRY_RUN_MODE = dry_run
    _TEST_TABLE_MODE = test_table
```

Modify `log_event()`:

```python
if _DRY_RUN_MODE:
    logger.info(f"[DRY-RUN] Would log event {event_type} for {source_system} status={status}")
    if payload:
        logger.debug(f"[DRY-RUN] Event payload: {payload}")
    return

# Use test table if in test mode
table_name = "test_event_log" if _TEST_TABLE_MODE else "event_log"
table_ref = _get_table_ref_by_name(table_name)
# ... existing insert logic
```

Modify `upsert_objects()`:

```python
if _DRY_RUN_MODE:
    count = 0
    samples = []
    for obj in objects:
        count += 1
        if len(samples) < 3:
            samples.append(obj)

    logger.info(f"[DRY-RUN] Would upsert {count} {object_type} objects for {source_system}")
    for i, s in enumerate(samples):
        ik = idem_key_fn(s)
        logger.info(f"[DRY-RUN] Sample {i+1} idem_key={ik}")
        logger.debug(f"[DRY-RUN] Sample {i+1} payload={json.dumps(s, default=str)[:500]}")
    return

# Use test table if in test mode
table_name = "test_raw_objects" if _TEST_TABLE_MODE else "raw_objects"
# ... existing upsert logic with table_name
```

**Important:** In dry-run branch, consume the iterator once for sampling. Don't materialize twice.

**Outputs:**

- Updated `lorchestra/stack_clients/event_client.py`

---

### Step 3: Ensure Test Tables Exist [G1: Code Review]

**Prompt:**

Add helper to create test tables from prod schema:

```python
def ensure_test_tables_exist(bq_client) -> None:
    """Create test_event_log and test_raw_objects if they don't exist.

    Copies schema from production tables. Only called when --test-table is active.
    Dry-run never creates BQ resources.
    """
    dataset = os.environ.get("EVENTS_BQ_DATASET")

    _copy_schema_if_missing(
        bq_client,
        source=f"{dataset}.event_log",
        dest=f"{dataset}.test_event_log"
    )
    _copy_schema_if_missing(
        bq_client,
        source=f"{dataset}.raw_objects",
        dest=f"{dataset}.test_raw_objects"
    )


def _copy_schema_if_missing(bq_client, source: str, dest: str) -> None:
    """Copy table schema from source to dest if dest doesn't exist."""
    try:
        bq_client.get_table(dest)
        return  # Already exists
    except Exception:
        pass  # Doesn't exist, create it

    source_table = bq_client.get_table(source)
    new_table = bigquery.Table(dest, schema=source_table.schema)
    bq_client.create_table(new_table)
    logger.info(f"Created test table: {dest}")
```

**Outputs:**

- Updated `lorchestra/stack_clients/event_client.py` with helpers

---

### Step 4: Wire CLI to Run Mode [G1: Code Review]

**Prompt:**

Update `_run_job_impl()` in `lorchestra/cli.py`:

1. Call `set_run_mode()` before job execution
2. If `--test-table`: call `ensure_test_tables_exist(bq_client)`
3. After job completes, print summary line: mode, job, records, duration

```python
def _run_job_impl(job: str, dry_run: bool = False, test_table: bool = False, **kwargs):
    from lorchestra.stack_clients.event_client import set_run_mode, ensure_test_tables_exist

    # Validate mutually exclusive flags
    if dry_run and test_table:
        raise click.UsageError("--dry-run and --test-table are mutually exclusive")

    # Set run mode before any BQ operations
    set_run_mode(dry_run=dry_run, test_table=test_table)

    # Print mode banner
    if dry_run:
        click.echo("=" * 50)
        click.echo("=== DRY RUN MODE === (no BigQuery writes)")
        click.echo("=" * 50)
    elif test_table:
        click.echo("=" * 50)
        click.echo("=== TEST TABLE MODE ===")
        click.echo("Writing to: test_event_log, test_raw_objects")
        click.echo("=" * 50)

    # Create BQ client
    bq_client = bigquery.Client()

    # Ensure test tables exist (only for test-table mode)
    if test_table:
        ensure_test_tables_exist(bq_client)

    # ... rest of existing job execution logic ...

    # Summary at end
    if dry_run:
        click.echo(f"\n[DRY-RUN] {job_name} completed in {duration:.2f}s (no writes)")
    elif test_table:
        click.echo(f"\n[TEST] {job_name} completed in {duration:.2f}s (wrote to test tables)")
```

**Outputs:**

- Updated `lorchestra/cli.py`

---

### Step 5: Unit Tests [G2: Pre-Release]

**Prompt:**

Add two minimal tests:

**1. `tests/test_dry_run.py`:**
- Patch BQ `insert_rows_json` and `load_table_from_json`
- Call `set_run_mode(dry_run=True)`
- Call `log_event()` and `upsert_objects()` with test data
- Assert no BQ write methods called

**2. `tests/test_test_table.py`:**
- Patch `_get_table_ref` or table resolution
- Call `set_run_mode(test_table=True)`
- Assert tables resolved are `test_event_log` and `test_raw_objects`
- Assert prod tables not referenced

```python
# tests/test_dry_run.py
def test_dry_run_skips_bq_writes(mocker):
    from lorchestra.stack_clients.event_client import set_run_mode, log_event, upsert_objects

    mock_insert = mocker.patch("google.cloud.bigquery.Client.insert_rows_json")
    mock_load = mocker.patch("google.cloud.bigquery.Client.load_table_from_json")

    set_run_mode(dry_run=True)

    # These should not call BQ
    log_event(event_type="test", source_system="test", correlation_id="test", bq_client=None)
    upsert_objects(objects=[{"id": "1"}], source_system="test", object_type="test",
                   correlation_id="test", idem_key_fn=lambda x: x["id"], bq_client=None)

    mock_insert.assert_not_called()
    mock_load.assert_not_called()
```

**Commands:**

```bash
pytest tests/test_dry_run.py tests/test_test_table.py -v
```

**Outputs:**

- `tests/test_dry_run.py`
- `tests/test_test_table.py`

---

### Step 6: Manual Validation [G2: Pre-Release]

**Prompt:**

Test both flags manually with an existing job:

1. **Test dry-run:**
   ```bash
   lorchestra run gmail_ingest_acct1 --since 2025-11-24 --until 2025-11-25 --dry-run
   ```
   Verify:
   - Banner shows "DRY RUN MODE"
   - Record count and sample idem_keys logged
   - No new rows in event_log or raw_objects

2. **Test test-table:**
   ```bash
   lorchestra run gmail_ingest_acct1 --since 2025-11-24 --until 2025-11-25 --test-table
   ```
   Verify:
   - Banner shows "TEST TABLE MODE"
   - `test_event_log` and `test_raw_objects` tables created
   - Data written to test tables, not production

3. **Test mutual exclusion:**
   ```bash
   lorchestra run gmail_ingest_acct1 --dry-run --test-table
   ```
   Verify: CLI errors with "mutually exclusive" message

**Outputs:**

- `artifacts/test/dry-run-test-table-results.md` with validation outputs

---

### Step 7: Documentation [G3: Post-Implementation]

**Prompt:**

Update CLI help text and add examples:

```
Examples:
  # Dry run - extract and log, no writes
  lorchestra run dataverse_ingest_contacts --dry-run

  # Test tables - full run to isolated tables
  lorchestra run dataverse_ingest_contacts --since 2025-01-01 --test-table

  # These are mutually exclusive (will error)
  lorchestra run job --dry-run --test-table
```

**Outputs:**

- Updated CLI help in `lorchestra/cli.py`

## Models & Tools

**Tools:** bash, pytest, python, lorchestra

**Models:** (defaults)

## Repository

**Branch:** `feat/dry-run-test-table`

**Merge Strategy:** squash
