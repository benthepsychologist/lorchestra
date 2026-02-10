---
id: e006-04-peek
title: "Peek — Cockpit Read/Explore Commands via Lorchestra Read Jobs"
tier: B
owner: benthepsychologist
goal: "Add cockpit-level data exploration commands via lorchestra read jobs"
branch: feat/life-peek
status: draft
created: 2026-02-10T13:43:36Z
---

# e006-04-peek: Peek — Cockpit Read/Explore Commands via Lorchestra Read Jobs

**Epic:** e006-life-thinning
**Branch:** `feat/life-peek`
**Tier:** B

## Objective

Add a `life peek` command that provides read access to BigQuery domain tables
via lorchestra read jobs. This fills the critical gap exposed during e005b: no
cockpit-level way to inspect data without the BQ console or ad-hoc Python.

Peek uses the same pattern as everything else in life: `lorchestra.execute()`.
The read jobs use lorchestra's `storacle.query` native op (built in e005b-08)
to query BQ and return rows. Life has exactly ONE integration point — no direct
storacle imports, no special read path.

## Problem

1. **No cockpit query interface**: When a pipeline fails or produces unexpected results, there's no built-in way to inspect the actual data — requires switching to BQ console
2. **Common lookups require raw SQL**: "Show me this client," "what came in today," "how many observations for form X" — these are routine questions that need convenient CLI flags, not custom SQL
3. **No standardized output**: Need consistent formats (table, JSON, CSV) that work for both human inspection and downstream tooling (jq, csvkit)

## Current Capabilities

### kernel.surfaces

```yaml
# No existing peek/query commands — this is a new capability
```

### modules

```yaml
# No existing read/explore modules
# lorchestra storacle.query native op exists (e005b-08) — peek jobs will use it
```

### layout

```yaml
- path: src/life/cli.py
  role: "Main Typer app, subcommand registration (will add peek)"
- path: src/life/commands/
  role: "Command implementations (will add peek.py)"
```

## Proposed build_delta

```yaml
target: "projects/life/life.build.yaml"
summary: "Add life peek command backed by lorchestra read jobs"

adds:
  layout:
    - "src/life/commands/peek.py"
  modules:
    - name: peek_commands
      provides: ["life peek clients", "life peek sessions", "life peek form_responses", "life peek raw_objects", "life peek canonical_objects", "life peek measurement_events", "life peek observations"]
  kernel_surfaces:
    - command: "life peek clients"
      usage: "life peek clients --limit 10 --format table"
    - command: "life peek sessions"
      usage: "life peek sessions --since 2026-02-09 --format json"
    - command: "life peek form_responses"
      usage: "life peek form_responses --form PHQ9 --format csv"
    - command: "life peek raw_objects"
      usage: "life peek raw_objects --id contact_123 --format json"
    - command: "life peek canonical_objects"
      usage: "life peek canonical_objects --until 2026-02-01 --limit 5"
    - command: "life peek measurement_events"
      usage: "life peek measurement_events --since 2026-02-01 --format table"
    - command: "life peek observations"
      usage: "life peek observations --limit 20 --format csv"
modifies:
  layout:
    - "src/life/cli.py": "Add peek subcommand registration"
removes: {}
```

### Lorchestra-side build_delta

```yaml
target: "projects/lorchestra/lorchestra.build.yaml"
summary: "Add peek read job definitions using storacle.query native op"

adds:
  layout:
    - "lorchestra/jobs/definitions/peek/"
  modules:
    - name: peek_jobs
      provides: ["peek.clients", "peek.sessions", "peek.form_responses", "peek.raw_objects", "peek.canonical_objects", "peek.measurement_events", "peek.observations"]
modifies: {}
removes: {}
```

## Acceptance Criteria

- [ ] life peek <table> command with subcommands for known domain tables
- [ ] Tables: clients, sessions, form_responses, raw_objects, canonical_objects, measurement_events, observations
- [ ] Common filters: --id, --since, --until, --limit (default 20), --form
- [ ] Peek job defs in lorchestra using storacle.query native op
- [ ] life calls execute({'job_id': 'peek.<table>', 'payload': {filters...}})
- [ ] Output formats: --format table (default), --format json, --format csv
- [ ] Table format: human-readable columnar output to stdout
- [ ] JSON format: one JSON object per line (jq-friendly)
- [ ] Helpful error messages: table not found, no rows matched, connection issues
- [ ] Zero direct storacle imports in life — peek goes through lorchestra

## Constraints

- Read-only — peek jobs use storacle.query, no writes
- Same integration pattern as all other life commands: lorchestra.execute()
- No direct storacle or BigQueryClient imports in life
- Default row limit of 20 to prevent accidental large queries
- Output formatting handles null values gracefully

---

## Phase 1: Lorchestra Peek Job Definitions

### Objective
Create read-only job definitions in lorchestra that use storacle.query native op
to query domain tables with configurable filters.

### Files to Touch
- `lorchestra/jobs/definitions/peek/peek.clients.yaml` (create)
- `lorchestra/jobs/definitions/peek/peek.sessions.yaml` (create)
- `lorchestra/jobs/definitions/peek/peek.form_responses.yaml` (create)
- `lorchestra/jobs/definitions/peek/peek.raw_objects.yaml` (create)
- `lorchestra/jobs/definitions/peek/peek.canonical_objects.yaml` (create)
- `lorchestra/jobs/definitions/peek/peek.measurement_events.yaml` (create)
- `lorchestra/jobs/definitions/peek/peek.observations.yaml` (create)

### Implementation Notes
Each job def is a single-step storacle.query with parameterized SQL from @payload.*:

```yaml
job_id: peek.clients
version: "0.1.0"
steps:
  - step_id: read
    op: storacle.query
    params:
      dataset: canonical
      table: clients
      columns: "*"
      filters: "@payload.filters"
      limit: "@payload.limit"
      order_by: "created_at DESC"
```

Filters passed via payload: `{"filters": [{"column": "id", "op": "=", "value": "abc123"}], "limit": 20}`

lorchestra's query_builder.py (from e005b-08) already handles parameterized SQL
construction from these declarative params. storacle.query already surfaces rows
as step output.

### Verification
- `lorchestra exec run peek.clients --payload '{"limit": 1}' --dry-run` → compiles
- `pytest tests/test_peek_jobs.py` → passes
- All 7 job defs load successfully from registry

## Phase 2: Life Peek Command

### Objective
Add `life peek` command that builds envelopes for peek jobs and renders results
in multiple output formats.

### Files to Touch
- `src/life/commands/peek.py` (create) — Peek command with subcommands per table
- `src/life/cli.py` (modify) — Register peek subcommand
- `tests/test_peek.py` (create) — Test coverage

### Implementation Notes
Each subcommand translates CLI flags to a lorchestra envelope:

```python
from lorchestra import execute

def peek_clients(id: str = None, since: str = None, limit: int = 20, format: str = "table"):
    filters = []
    if id:
        filters.append({"column": "id", "op": "=", "value": id})
    if since:
        filters.append({"column": "created_at", "op": ">=", "value": since})

    result = execute({
        "job_id": "peek.clients",
        "payload": {"filters": filters, "limit": limit},
    })

    if result.success:
        rows = extract_rows(result)  # from step output
        render(rows, format=format)
    else:
        print_error(result)
```

Output formatters:
- `table`: Simple columnar layout, column widths from data
- `json`: One JSON object per line (JSONL) for jq compatibility
- `csv`: Standard CSV with headers, null values as empty strings

### Verification
- `life peek --help` → shows available tables
- `life peek clients --help` → shows table-specific options
- `life peek clients --limit 1 --format json` → valid JSONL output
- `life peek sessions --since 2026-02-09 --format table` → columnar output
- `life peek form_responses --form PHQ9 --format csv` → proper CSV with headers
- `ruff check src/life/commands/peek.py` → clean
- `grep -r "from storacle" src/life/` → no matches

## Critical Constraint: build_delta First

The build_delta is the REAL constraint. Everything else derives from it:
- **adds.layout** → drives Files to Touch (peek.py in life, peek/ job defs in lorchestra)
- **adds.kernel_surfaces** → drives Acceptance Criteria (7 peek subcommands)
- **adds.modules** → drives what functionality is added (peek_commands in life, peek_jobs in lorchestra)
