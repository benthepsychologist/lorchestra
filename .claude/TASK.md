---
tier: C
title: "e005b-09d: Ingest IO Purity — Replace auto_since Executor Magic"
owner: benthepsychologist
goal: "Replace auto_since executor magic with explicit storacle.query cursor step in ingest jobs"
epic: e005b-command-plane-and-performance
repo:
  name: lorchestra
  working_branch: feat/ingest-io-purity
created: 2026-02-06T00:00:00Z
---

# e005b-09d: Ingest IO Purity — Replace auto_since Executor Magic

## Status: COMPLETED

## Problem

Ingest jobs use `auto_since` executor magic for incremental sync:

```yaml
- step_id: ingest
  op: call
  params:
    callable: injest
    source: gmail_acct1
    auto_since:
      source_system: gmail
      connection_name: gmail-acct1
      object_type: email
```

The executor intercepts `auto_since`, runs a hidden BQ query (`SELECT MAX(last_seen) FROM raw_objects WHERE ...`), and injects the result into `config.since`.

This violates the principle established in e005b-08: **all BQ reads should be explicit `storacle.query` steps**. The canonize jobs were migrated to explicit reads; ingest jobs were left behind because "ingest doesn't read from BQ" — but the cursor lookup IS a BQ read.

### Why This Matters

1. **Consistency**: Canonize jobs use explicit `storacle.query`. Ingest should too.
2. **Visibility**: Hidden executor magic obscures what the job actually does.
3. **Backfill**: Can't do backward-from-now ingestion without explicit cursor control. (`last_seen` is ingestion time, not email date — need event_log cursor.)
4. **Testability**: Explicit steps are easier to mock and test.

---

## Solution

Replace `auto_since` with an explicit `storacle.query` cursor step.

### Before (executor magic)

```yaml
job_id: ingest_gmail_acct1
version: '2.0'
steps:
- step_id: ingest
  op: call
  params:
    callable: injest
    source: gmail_acct1
    auto_since:
      source_system: gmail
      connection_name: gmail-acct1
      object_type: email
- step_id: persist
  op: plan.build
  # ...
- step_id: write
  op: storacle.submit
  # ...
```

### After (explicit cursor)

```yaml
job_id: ingest_gmail_acct1
version: '2.0'
steps:
- step_id: cursor
  op: storacle.query
  params:
    dataset: raw
    table: raw_objects
    columns: ['MAX(last_seen) as since']
    filters:
      source_system: gmail
      connection_name: gmail-acct1
      object_type: email

- step_id: ingest
  op: call
  params:
    callable: injest
    source: gmail_acct1
    config:
      since: '@run.cursor.items[0].since'

- step_id: persist
  op: plan.build
  # ... unchanged

- step_id: write
  op: storacle.submit
  # ... unchanged
```

### Array Index Support Required

The executor's `_resolve_run_refs` currently only handles dict key access (`@run.step.key.subkey`). To reference the first item from a query result, we need `@run.cursor.items[0].since` — which requires **array index support**.

**Add to `_resolve_run_refs` in `executor.py`:**
```python
# Handle array indexing: items[0] -> result[0]
import re
array_match = re.match(r"(\w+)\[(\d+)\]", part)
if array_match:
    key, idx = array_match.groups()
    if isinstance(result, dict) and key in result:
        result = result[key]
    if isinstance(result, list) and int(idx) < len(result):
        result = result[int(idx)]
    continue
```

### Null Handling

When the cursor query returns `NULL` (empty table), `@run.cursor.items[0].since` resolves to `null`. The injest adapter must handle `since: null` as "fetch all" (no date filter). This is already the case — Gmail adapter's `_build_query` only adds `after:{date}` when `since` is truthy.

---

## Jobs to Migrate (20 total)

### Gmail (3)
- `ingest_gmail_acct1`
- `ingest_gmail_acct2`
- `ingest_gmail_acct3`

### Exchange (4)
- `ingest_exchange_ben_mensio`
- `ingest_exchange_ben_efs`
- `ingest_exchange_booking_mensio`
- `ingest_exchange_info_mensio`

### Dataverse (3)
- `ingest_dataverse_contacts`
- `ingest_dataverse_sessions`
- `ingest_dataverse_reports`

### Google Forms (4)
- `ingest_google_forms_intake_01`
- `ingest_google_forms_intake_02`
- `ingest_google_forms_followup`
- `ingest_google_forms_ipip120`

### Stripe (4)
- `ingest_stripe_customers`
- `ingest_stripe_invoices`
- `ingest_stripe_payment_intents`
- `ingest_stripe_refunds`

---

## Executor Cleanup

After all jobs migrated, remove the `auto_since` executor magic:

**File:** `lorchestra/executor.py`

Delete:
- `_resolve_auto_since()` method (~30 lines)
- `auto_since` handling in `_handle_call()` (~5 lines)

The executor's `_handle_call()` should become a pure dispatcher with no hidden IO.

---

## Backfill Pattern (Enabled by This Migration)

Once ingest jobs use explicit cursor steps, backfill becomes a second cursor
in the same job — working **backward from now**, one month per run.

### Why backward from now?

- Recent emails are highest value — get them first
- Forward cursor already handles "now and future"
- Each backfill chunk gets progressively less urgent
- Stops naturally when Gmail returns 0 records for a window predating the account

### Backfill cursor: event_log (no new tables)

`last_seen` in `raw_objects` is the **ingestion timestamp** (`datetime.now()`),
not the email's sent date — so `MIN(last_seen)` can't track how far back we've
gone. Instead, the backfill cursor is derived from the event_log, which already
records `window_since` for every completed ingestion run:

```sql
SELECT MIN(CAST(JSON_VALUE(payload, '$.window_since') AS TIMESTAMP)) as backfill_position
FROM event_log
WHERE event_type = 'ingestion.completed'
  AND connection_name = @connection_name
  AND status = 'success'
  AND JSON_VALUE(payload, '$.job_id') LIKE '%backfill%'
```

- **No prior runs** → `backfill_position` is NULL → `until = NOW()`, `since = NOW() - 1 month`
- **Has prior runs** → `until = backfill_position`, `since = backfill_position - 1 month`

Writes go to the same `raw_objects` table — `idem_key` (Gmail message ID) handles
dedup, no separate backfill table or merge step needed.

### Example: backfill job

```yaml
job_id: ingest_gmail_bfarmstrong_backfill
version: '2.0'
steps:
# Cursor: find how far back we've gone from event_log
- step_id: cursor
  op: storacle.query
  params:
    dataset: events
    table: event_log
    query: |
      SELECT MIN(CAST(JSON_VALUE(payload, '$.window_since') AS TIMESTAMP)) as backfill_position
      FROM event_log
      WHERE event_type = 'ingestion.completed'
        AND connection_name = 'gmail-bfarmstrong'
        AND status = 'success'
        AND JSON_VALUE(payload, '$.job_id') LIKE '%backfill%'

# Ingest: one month chunk working backward
- step_id: ingest
  op: call
  params:
    callable: injest
    source: gmail_bfarmstrong
    config:
      until: '@run.cursor.items[0].backfill_position'  # null → now
      since: '@run.cursor.items[0].backfill_since'      # position - 1 month

- step_id: persist
  op: plan.build
  params:
    items: '@run.ingest.items'
    method: bq.upsert
    dataset: raw
    table: raw_objects
    # ... same key_columns, field_defaults as forward job

- step_id: write
  op: storacle.submit
  params:
    plan: '@run.persist.plan'
```

### Backfill progression (bfarmstrong@gmail.com, ~150k emails / 20 years)

```
Run 1:   since = now - 1mo,    until = now          (~625 emails)
Run 2:   since = now - 2mo,    until = now - 1mo
Run 3:   since = now - 3mo,    until = now - 2mo
...
Run 240: since = now - 240mo,  until = now - 239mo  (≈ 20 years back)
Run 241: Gmail returns 0 records → backfill complete
```

At ~625 emails/month average, each chunk is small. Run daily alongside the
forward job — fully caught up in ~8 months. Run twice daily for ~4 months.

---

## Implementation Steps

### Step 0: Add array index support to `_resolve_run_refs`
- Update `lorchestra/executor.py` to handle `items[0]` syntax
- Add unit test for array index resolution

### Step 1: Migrate Gmail jobs (3)
- Rewrite YAML with explicit cursor step
- Test with `lorchestra exec run ingest_gmail_acct1`
- Verify emails still ingest correctly

### Step 2: Migrate remaining jobs (17)
- Exchange (4)
- Dataverse (3)
- Google Forms (4)
- Stripe (4)

### Step 3: Delete executor magic
- Remove `_resolve_auto_since()` from executor.py
- Remove `auto_since` handling from `_handle_call()`

### Step 4: Update tests
- Remove tests for `auto_since` executor behavior
- Add tests for cursor step pattern (if not already covered by e005b-08 tests)

---

## Files to Modify

| File | Change |
|------|--------|
| `lorchestra/executor.py` | Add array index support to `_resolve_run_refs` |
| `lorchestra/jobs/definitions/ingest/*.yaml` (20 files) | Add cursor step, remove auto_since |
| `lorchestra/executor.py` | Delete `_resolve_auto_since()` and auto_since handling |
| `tests/` | Update/remove auto_since tests |

---

## Complexity Assessment

- **Array index support**: ~10 lines added to `_resolve_run_refs`
- **YAML rewrites**: Mechanical, ~20 files
- **Executor cleanup**: ~35 lines deleted
- **Risk**: Low. Pattern already proven by canonize jobs. Small code addition.
- **Estimate**: 1-2 hours

---

## Acceptance Criteria

- [x] Array index support added to `_resolve_run_refs` (`@run.step.items[0].field` works)
- [x] All 18 ingest jobs use explicit `storacle.query` cursor step
- [x] No `auto_since` blocks in any job definition
- [x] `_resolve_auto_since()` deleted from executor.py
- [x] All ingest jobs pass smoke test
- [x] `pytest tests/ -v` passes
- [x] `ruff check lorchestra/` passes

---

## Deferred to Next Epic

The following performance enhancements were identified but deferred:

- Pipeline parallel execution improvements
- Executor caching optimizations
- Other lorchestra performance work

---

## Known Issues

### ✅ SOLVED: gmail_to_jmap_lite transform fails on emails with duplicate headers

**Symptom:** `canonize_gmail_jmap` job fails intermittently with:
```
Error: Failed to execute transform: email/gmail_to_jmap_lite@1.1.0 [T0410]: Argument 1 of function "split" does not match function signature
```

**Root cause:** Some Gmail emails have duplicate headers (e.g., multiple `Bcc` headers). The `$getHeader` helper used `$lookup($headers[name=$name], "value")`, which returns a JSONata sequence (array) when multiple headers match. This array was then passed to `$split` or `$parseEmailList`, which expect strings.

**Affected record:** `gmail:gmail-acct2:email:17524bcb06f2d38d` (email with duplicate `Bcc` headers)

**Why it seemed random:** Small batches often missed the one problematic record; larger batches consistently included it.

**Fix:** Updated `$getHeader` in `email/gmail_to_jmap_lite@1.1.0` to join array values:
```jsonata
$getHeader := function($headers, $name) {
  (
    $values := $lookup($headers[name=$name], "value");
    $type($values) = "array" ? $join($values, ", ") : $values
  )
};
```

**Also improved:** CLI error reporting now shows JSONata error codes and messages (JSONata throws plain objects, not Error instances).
