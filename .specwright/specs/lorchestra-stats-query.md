---
version: "0.1"
tier: C
title: lorchestra-stats-query
owner: benthepsychologist
goal: Add read-only SQL query commands to lorchestra CLI
labels: [cli, query, stats]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-12-03T14:55:17.615939+00:00
updated: 2025-12-03T14:55:17.615939+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/lorchestra-stats-query"
---

# lorchestra stats/query/sql Commands

## Objective

Add three CLI command groups to lorchestra for read-only SQL querying:

1. **`lorchestra stats`** - Built-in canned reports for common operational queries
2. **`lorchestra query <name>`** - Named queries from SQL files in `queries/` directory
3. **`lorchestra sql`** - Ad-hoc SQL via argument, stdin, or heredoc

All commands share a strict **read-only gate** that refuses any mutating SQL.

## Acceptance Criteria

- [ ] `lorchestra stats canonical` shows canonical objects by schema/source
- [ ] `lorchestra stats raw` shows raw objects by source/type/validation_status
- [ ] `lorchestra stats jobs` shows job events from last N days (default 7)
- [ ] `lorchestra query <name>` loads and executes `queries/<name>.sql`
- [ ] `lorchestra sql` accepts SQL from argument, stdin, or heredoc
- [ ] All queries use `${PROJECT}` and `${DATASET}` placeholder substitution
- [ ] Read-only gate rejects INSERT/UPDATE/DELETE/MERGE/DROP/CREATE/ALTER/TRUNCATE/GRANT/REVOKE/CALL/EXECUTE
- [ ] Read-only gate rejects multi-statement SQL (only trailing `;` allowed)
- [ ] Read-only gate requires query to start with SELECT or WITH
- [ ] `lorchestra sql` with no arg and empty stdin returns UsageError with help text
- [ ] `lorchestra query <name>` with missing file returns clear error with expected path
- [ ] Missing `GCP_PROJECT` or `EVENTS_BQ_DATASET` env vars fail loudly with UsageError
- [ ] CI green (lint + unit)

## Context

### Background

Currently there's no easy way to query lorchestra tables from the CLI. Users must write ad-hoc Python or use BigQuery console. Common operational queries (counts by schema, validation status breakdown, job history) should be one command away.

### Design Decisions

**Read-only gate implementation:**
1. Strip SQL comments (`--` and `/* */`)
2. Normalize to lowercase, collapse whitespace
3. Require query starts with `select` or `with` (after stripping)
4. Reject standalone word matches for mutating keywords
5. Reject multi-statement SQL (only trailing `;` allowed)

**Placeholder substitution:**
- `${PROJECT}` → `GCP_PROJECT` env var (**required**, no default - fail loudly)
- `${DATASET}` → `EVENTS_BQ_DATASET` env var (**required**, no default - fail loudly)
- `${DAYS}` → passed from CLI options (e.g., `--days 7`)

**Output format:**
- Default: aligned table to stdout
- Future: `--format csv` option (not in initial scope)

## Implementation

### A. `lorchestra stats` - Built-in Reports

#### A1. `lorchestra stats canonical`

```sql
SELECT
  canonical_schema,
  source_system,
  COUNT(*) AS count
FROM `${PROJECT}.${DATASET}.canonical_objects`
GROUP BY canonical_schema, source_system
ORDER BY count DESC
```

#### A2. `lorchestra stats raw`

```sql
SELECT
  source_system,
  object_type,
  validation_status,
  COUNT(*) AS count
FROM `${PROJECT}.${DATASET}.raw_objects`
GROUP BY source_system, object_type, validation_status
ORDER BY source_system, object_type, validation_status
```

#### A3. `lorchestra stats jobs`

```sql
SELECT
  event_type,
  source_system,
  status,
  COUNT(*) AS count,
  MIN(created_at) AS first_seen,
  MAX(created_at) AS last_seen
FROM `${PROJECT}.${DATASET}.event_log`
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${DAYS} DAY)
GROUP BY event_type, source_system, status
ORDER BY last_seen DESC
```

Option: `--days` (default: 7) - substituted into `${DAYS}` placeholder

### B. `lorchestra query <name>` - Named Queries

- Loads SQL from `lorchestra/queries/<name>.sql`
- Applies placeholder substitution
- Validates read-only
- Executes and prints results

### C. `lorchestra sql` - Ad-hoc SQL

Input modes:
1. **Argument:** `lorchestra sql "SELECT COUNT(*) FROM ..."`
2. **Stdin pipe:** `echo "SELECT ..." | lorchestra sql`
3. **Heredoc:** `lorchestra sql << 'EOF' ... EOF`

Same substitution and validation as `query`.

### Shared Components

#### Read-only Validator

```python
MUTATING_KEYWORDS = [
    'insert', 'update', 'delete', 'merge', 'truncate',
    'create', 'drop', 'alter',
    'grant', 'revoke',
    'call', 'execute',
]

def validate_readonly_sql(sql: str) -> None:
    """Validate SQL is read-only. Raises click.UsageError if not."""
    # 1. Strip comments
    cleaned = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)

    # 2. Normalize
    normalized = ' '.join(cleaned.lower().split())

    # 3. Check starts with SELECT or WITH
    if not (normalized.startswith('select') or normalized.startswith('with')):
        raise click.UsageError("Query must start with SELECT or WITH")

    # 4. Check for mutating keywords (whole words)
    for keyword in MUTATING_KEYWORDS:
        if re.search(rf'\b{keyword}\b', normalized):
            raise click.UsageError(
                f"Refusing to run non-read-only SQL. Detected mutating keyword: {keyword}"
            )

    # 5. Check for multi-statement (semicolon not at end)
    stripped = sql.strip()
    if ';' in stripped[:-1]:  # Allow trailing semicolon
        raise click.UsageError("Multi-statement SQL not allowed")
```

#### Placeholder Substitution

```python
def substitute_placeholders(sql: str, extra: dict[str, str] | None = None) -> str:
    """Substitute ${PLACEHOLDER} values in SQL.

    Args:
        sql: SQL with placeholders
        extra: Additional placeholders (e.g., {"DAYS": "7"})

    Returns:
        SQL with placeholders replaced

    Raises:
        click.UsageError if required env vars are missing
    """
    project = os.environ.get('GCP_PROJECT')
    dataset = os.environ.get('EVENTS_BQ_DATASET')

    if not project:
        raise click.UsageError("GCP_PROJECT environment variable is required")
    if not dataset:
        raise click.UsageError("EVENTS_BQ_DATASET environment variable is required")

    sql = sql.replace('${PROJECT}', project)
    sql = sql.replace('${DATASET}', dataset)

    if extra:
        for key, value in extra.items():
            sql = sql.replace(f'${{{key}}}', value)

    return sql
```

#### SQL Executor

```python
def run_sql_query(sql: str, extra_placeholders: dict[str, str] | None = None) -> None:
    """Execute read-only SQL and print results as table."""
    from google.cloud import bigquery

    # Substitute placeholders (fails loudly if env vars missing)
    sql = substitute_placeholders(sql, extra_placeholders)

    # Validate read-only
    validate_readonly_sql(sql)

    # Execute
    project = os.environ['GCP_PROJECT']
    client = bigquery.Client(project=project)
    result = client.query(sql).result()

    # Print as table
    rows = list(result)
    if not rows:
        click.echo("No results.")
        return

    # Calculate column widths and print aligned table
    ...
```

## Files to Touch

1. `lorchestra/cli.py` - Add stats, query, sql command groups
2. `lorchestra/sql_runner.py` - New module for shared SQL execution logic
3. `lorchestra/queries/` - New directory for named query SQL files
4. `tests/test_sql_runner.py` - Unit tests for read-only validator
5. `tests/test_cli_query.py` - CLI integration tests

## Example Queries Directory

```
lorchestra/queries/
├── canonical-summary.sql
├── raw-validation.sql
├── recent-jobs.sql
└── stale-raw-objects.sql
```

## Plan

### Step 1: Create sql_runner module with validator and executor

**Files:** `lorchestra/sql_runner.py`

Implement:
- `validate_readonly_sql(sql: str)` - the read-only gate
- `substitute_placeholders(sql: str)` - `${PROJECT}` and `${DATASET}` replacement
- `run_sql_query(sql: str)` - execute and print results

### Step 2: Add stats command group to CLI

**Files:** `lorchestra/cli.py`

Add:
- `@main.group("stats")`
- `@stats.command("canonical")`
- `@stats.command("raw")`
- `@stats.command("jobs")` with `--days` option

### Step 3: Add query command to CLI

**Files:** `lorchestra/cli.py`, `lorchestra/queries/`

Add:
- `@main.command("query")` that loads from `queries/<name>.sql`
- Create `queries/` directory with example SQL files

### Step 4: Add sql command to CLI

**Files:** `lorchestra/cli.py`

Add:
- `@main.command("sql")` that accepts arg or stdin

### Step 5: Write tests

**Files:** `tests/test_sql_runner.py`, `tests/test_cli_query.py`

Test cases for `tests/test_sql_runner.py`:

**Valid queries (should pass):**
- `SELECT 1`
- `select count(*) from foo`
- `WITH t AS (SELECT 1) SELECT * FROM t`
- `WITH t AS (SELECT 1) SELECT * FROM t;` (trailing semicolon OK)
- `/* comment */ SELECT 1`
- `-- comment\nSELECT 1`

**Invalid queries (should reject):**
- `INSERT INTO foo SELECT 1` → mutating keyword
- `SELECT 1; SELECT 2` → multi-statement
- `DROP TABLE foo` → mutating keyword
- `UPDATE foo SET x = 1` → mutating keyword
- `CREATE TABLE foo (id INT)` → mutating keyword
- `-- comment only` → must start with SELECT/WITH
- Empty string → must start with SELECT/WITH

**Edge cases:**
- `SELECT 'drop table foo'` → conservative rejection is OK (keyword in string)
- `SELECT * FROM update_log` → should pass (`update` is part of identifier, not standalone)

**Placeholder substitution:**
- `${PROJECT}` and `${DATASET}` replaced correctly
- Extra placeholders like `${DAYS}` work
- Missing env vars raise UsageError

**CLI tests for `tests/test_cli_query.py`:**
- `lorchestra stats canonical` executes without error (mocked BQ)
- `lorchestra stats jobs --days 3` substitutes days correctly
- `lorchestra query nonexistent` gives clear "file not found" error
- `lorchestra sql` with empty stdin gives UsageError

## Models & Tools

**Tools:** bash, pytest, ruff

**Models:** Default

## Repository

**Branch:** `feat/lorchestra-stats-query`

**Merge Strategy:** squash
