---
version: "0.1"
tier: C
title: Prod Ingest Stripe (Mensio Payments)
owner: benthepsychologist
goal: Ingest Stripe data (customers, invoices, payment_intents, refunds) from Mensio account to BigQuery using InJest
labels: [stripe, ingestion, payments]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-11-25T14:45:00+00:00
updated: 2025-11-25T14:45:00+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "main"
status: implemented
---

# Prod Ingest Stripe (Mensio Payments)

## Objective

> Ingest Stripe payment data from Mensio Mental Health account to BigQuery
> using the InJest Stripe stream adapters.

## Acceptance Criteria

- [x] Stripe auth working via API key (env var `STRIPE_API_KEY`)
- [x] `lorchestra run stripe_ingest_customers` extracts customers to BigQuery
- [x] `lorchestra run stripe_ingest_invoices` extracts invoices to BigQuery
- [x] `lorchestra run stripe_ingest_payment_intents` extracts payment intents to BigQuery
- [x] `lorchestra run stripe_ingest_refunds` extracts refunds to BigQuery
- [x] All streams use new idem_key pattern: `stripe:stripe-mensio:{object_type}:{id}`
- [x] `event_log` shows `ingest.completed` and `upsert.completed` events
- [x] `--dry-run` mode works correctly (no writes)
- [x] `--test-table` mode works correctly (writes to test_* tables)
- [x] Daily incremental sync via `created` timestamp filter works correctly
- [x] All 4 jobs run successfully with production data

## Context

### Background

The InJest package (`/workspace/injest`) has 4 Stripe stream adapters:
- `StripeCustomersStream` - Customer records (~150)
- `StripeInvoicesStream` - Invoice records with line items (~300)
- `StripePaymentIntentsStream` - Payment intent records (~2600+)
- `StripeRefundsStream` - Refund records (~30)

All adapters:
- Use the Stripe Python SDK (`stripe` package)
- Support `since`/`until` date filtering via `created` timestamp
- Yield plain dicts via `to_dict_recursive()`

The identity `stripe:mensio` is configured in `injest/config.py`:
```python
"stripe:mensio": {
    "provider": "stripe",
    "description": "Mensio Mental Health - Stripe",
    "api_key_env": "STRIPE_API_KEY",
}
```

### Column Standards (aligned with migration)

Following the standardized column pattern:
- `source_system`: `stripe`
- `connection_name`: `stripe-mensio`
- `object_type`: `customer`, `invoice`, `payment_intent`, `refund`

### idem_key Pattern

```
stripe:stripe-mensio:{object_type}:{stripe_id}
```

Examples:
- `stripe:stripe-mensio:customer:cus_xxx`
- `stripe:stripe-mensio:invoice:in_xxx`
- `stripe:stripe-mensio:payment_intent:pi_xxx`
- `stripe:stripe-mensio:refund:re_xxx`

### Constraints

- Jobs must use the standardized column pattern from the schema migration
- Must query BigQuery for `last_seen` if no `--since` provided
- Must emit `ingest.completed` and `upsert.completed` telemetry events

## Implementation Summary

### Files Created/Modified

1. **`lorchestra/idem_keys.py`** - Added `stripe_idem_key()` function
2. **`lorchestra/jobs/ingest_stripe.py`** - 4 job functions following Gmail/Exchange pattern
3. **`lorchestra/injest_config.py`** - Added Stripe provider support
4. **`pyproject.toml`** - Registered 4 job entrypoints

### Job Functions

```python
job_ingest_stripe_customers(bq_client, since=None, until=None)
job_ingest_stripe_invoices(bq_client, since=None, until=None)
job_ingest_stripe_payment_intents(bq_client, since=None, until=None)
job_ingest_stripe_refunds(bq_client, since=None, until=None)
```

### Date Filtering

All jobs support:
- `--since 2025-09-01` (ISO date)
- `--since -7d` (relative)
- No `--since` → queries BigQuery for `MAX(last_seen)`

## Plan

### Step 1: Add Stripe API Key [G0: Auth Validation]

**Prompt:**

Add Stripe API key to `.env`:

```bash
STRIPE_API_KEY=sk_live_xxxxxxxxxxxxxxxxxxxxx
```

Verify with dry-run:
```bash
lorchestra run stripe_ingest_customers --dry-run
```

**Outputs:**

- Updated `.env` with `STRIPE_API_KEY`
- Confirmation that dry-run reaches Stripe API

---

### Step 2: Run All Stripe Ingestion Jobs [G2: Pre-Release]

**Prompt:**

Run each Stripe ingestion job and validate BigQuery results:

1. **Customers:**
   ```bash
   lorchestra run stripe_ingest_customers
   ```

2. **Invoices:**
   ```bash
   lorchestra run stripe_ingest_invoices
   ```

3. **Payment Intents:**
   ```bash
   lorchestra run stripe_ingest_payment_intents
   ```

4. **Refunds:**
   ```bash
   lorchestra run stripe_ingest_refunds
   ```

Validate with BigQuery:
```sql
SELECT source_system, connection_name, object_type, COUNT(*) as count
FROM `events_dev.raw_objects`
WHERE source_system = 'stripe'
GROUP BY 1, 2, 3
ORDER BY 3;
```

Check telemetry events:
```sql
SELECT event_type, source_system, connection_name, target_object_type,
       JSON_EXTRACT_SCALAR(payload, '$.records_extracted') as records,
       JSON_EXTRACT_SCALAR(payload, '$.inserted') as inserted
FROM `events_dev.event_log`
WHERE source_system = 'stripe'
ORDER BY created_at DESC
LIMIT 10;
```

**Outputs:**

- `raw_objects` rows for all 4 object types
- `event_log` entries showing successful ingestion

---

### Step 3: Add to Daily Ingest Script [G3: Integration]

**Prompt:**

Update `scripts/daily_ingest.sh` to include Stripe jobs:

```bash
# Stripe payment data
lorchestra run stripe_ingest_customers
lorchestra run stripe_ingest_invoices
lorchestra run stripe_ingest_payment_intents
lorchestra run stripe_ingest_refunds
```

**Outputs:**

- Updated `scripts/daily_ingest.sh`

## Models & Tools

**Tools:** bash, python, lorchestra

**Models:** (defaults)

## Repository

**Branch:** `main` (already merged)

**Commits:**
- `afcbef7` - feat: add Stripe ingestion jobs

## Test Results

### Dry-Run Mode (2025-11-25)
```
lorchestra run stripe_ingest_customers --dry-run
[DRY-RUN] stripe_ingest_customers completed in 2.42s (no writes)
```

### Test-Table Mode (2025-11-25)
```
lorchestra run stripe_ingest_customers --test-table
[TEST] stripe_ingest_customers completed in 9.25s (wrote to test tables)

Results in test_raw_objects:
  stripe/stripe-mensio/customer: 148 records

Results in test_event_log:
  ingest.completed | stripe-mensio/customer: 148 records (148 inserted)
  upsert.completed | stripe-mensio/customer: 148 inserted

Sample idem_keys:
  stripe:stripe-mensio:customer:cus_O5XS8lyisdK2oK
  stripe:stripe-mensio:customer:cus_OXcFdol8Z9kTuT
```

### Production Run (2025-11-25)
```
lorchestra run stripe_ingest_customers    → ✓ 9.01s
lorchestra run stripe_ingest_invoices     → ✓ 16.63s
lorchestra run stripe_ingest_payment_intents → ✓ 74.77s
lorchestra run stripe_ingest_refunds      → ✓ 8.89s

Results in raw_objects:
  stripe/stripe-mensio/customer: 148 records
  stripe/stripe-mensio/invoice: 273 records
  stripe/stripe-mensio/payment_intent: 2653 records
  stripe/stripe-mensio/refund: 27 records

Telemetry events (event_log):
  ingest.completed + upsert.completed for all 4 object types
```
