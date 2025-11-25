---
version: "0.1"
tier: C
title: Migrate lorchestra to authctl
owner: benthepsychologist
goal: Update lorchestra to consume credentials via authctl package
labels: [auth, migration, cleanup]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-11-25T16:37:01.999734+00:00
updated: 2025-11-25T17:00:00+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "main"
---

# Migrate lorchestra to authctl

## Objective

> Update lorchestra to consume credentials via the `authctl` package, removing all
> legacy auth scripts and credential environment variables.

## Acceptance Criteria

- [ ] `authctl` added as dependency in `pyproject.toml`
- [ ] All jobs use authctl providers for credentials (no direct `.env` credential access)
- [ ] All `scripts/gmail_*.py` OAuth scripts deleted
- [ ] `scripts/get_gmail_refresh_token.py` deleted
- [ ] Credential env vars removed from `.env` (keep non-auth vars like `GCP_PROJECT`)
- [ ] `AUTHCTL_HOME=/workspace/.authctl` added to `.env`
- [ ] `lorchestra run gmail_ingest_acct1 --dry-run` works
- [ ] `lorchestra run exchange_ingest_ben_mensio --dry-run` works
- [ ] `lorchestra run stripe_ingest_customers --dry-run` works
- [ ] All production jobs run successfully

## Context

### Background

The `authctl` package has been implemented at `/workspace/authctl/` and provides:
- Unified credential storage at `$AUTHCTL_HOME/credentials.json`
- Provider factories that return SDK-native objects:
  - `build_gmail_credentials(account)` → `google.oauth2.credentials.Credentials`
  - `build_msgraph_app(account)` → `msal.PublicClientApplication`
  - `get_api_key(account)` → `str`

Currently, lorchestra has legacy auth artifacts:
- `scripts/gmail_*.py` - duplicated OAuth setup scripts (one per account)
- `scripts/get_gmail_refresh_token.py` - another OAuth helper
- `.env` contains credential env vars that should now come from authctl

### Prerequisites

Before running this migration:
1. `authctl` package must be installed and working
2. All accounts must be re-authorized via `authctl auth ...` commands
3. `$AUTHCTL_HOME` must be set to `/workspace/.authctl`
4. Credentials must exist in `$AUTHCTL_HOME/credentials.json`

### authctl ABI Reference

**Provider imports:**
```python
from authctl.providers.gmail import build_gmail_credentials
from authctl.providers.msgraph import build_msgraph_app
from authctl.providers.stripe import get_api_key
```

**Usage pattern:**
```python
# Gmail - returns google.oauth2.credentials.Credentials (handles refresh)
gmail_creds = build_gmail_credentials("acct1")
service = build("gmail", "v1", credentials=gmail_creds)

# MS Graph - returns msal.PublicClientApplication (MSAL handles refresh)
msgraph_app = build_msgraph_app("ben-mensio")
result = msgraph_app.acquire_token_silent(scopes, account)

# Stripe - returns API key string
stripe.api_key = get_api_key("mensio")
```

**Error handling:**
```python
from authctl.secrets import MissingCredentialsError

try:
    creds = build_gmail_credentials("unknown")
except MissingCredentialsError as e:
    # Message: "Credentials not found for gmail:unknown. Run: authctl auth gmail --account unknown"
    print(e)
```

### Constraints

- `authctl` must be installed before this migration runs
- All accounts must already be authorized in `$AUTHCTL_HOME`
- Jobs should continue to work with `--dry-run` and `--test-table` flags

## Plan

### Step 1: Add authctl Dependency [G0: Setup]

**Prompt:**

Add `authctl` as a dependency to lorchestra:

1. Update `pyproject.toml` to add authctl dependency:
   ```toml
   dependencies = [
       ...
       "authctl",
   ]
   ```

2. Add `AUTHCTL_HOME=/workspace/.authctl` to `.env`

3. Reinstall package to pick up new dependency

**Commands:**

```bash
cd /workspace/lorchestra
uv pip install -e .
source .env && echo "AUTHCTL_HOME=$AUTHCTL_HOME"
```

**Outputs:**

- Updated `/workspace/lorchestra/pyproject.toml`
- Updated `/workspace/lorchestra/.env`

---

### Step 2: Update injest_config.py [G1: Code Migration]

**Prompt:**

Update `lorchestra/injest_config.py` to use authctl for credential resolution:

1. Remove any direct credential loading from env vars
2. Keep the account identity mappings (these map lorchestra job names to authctl account names)
3. If there are functions that build credentials, replace them with authctl provider calls

Review the current implementation and determine what needs to change.

**Commands:**

```bash
# Verify authctl is accessible
python -c "from authctl.providers.gmail import build_gmail_credentials; print('OK')"
```

**Outputs:**

- Updated `/workspace/lorchestra/lorchestra/injest_config.py`

---

### Step 3: Update Job Files [G1: Code Migration]

**Prompt:**

Update job files to use authctl providers. The jobs likely call into `injest` which handles credential loading, but verify:

1. Check `lorchestra/jobs/ingest_gmail.py` - ensure it uses authctl via injest
2. Check `lorchestra/jobs/ingest_exchange.py` - ensure it uses authctl via injest
3. Check `lorchestra/jobs/ingest_stripe.py` - ensure it uses authctl via injest

If jobs directly access credentials (not via injest), update them to use:
- `from authctl.providers.gmail import build_gmail_credentials`
- `from authctl.providers.msgraph import build_msgraph_app`
- `from authctl.providers.stripe import get_api_key`

**Commands:**

```bash
# Verify imports work
python -c "from lorchestra.jobs.ingest_gmail import *; print('Gmail jobs OK')"
python -c "from lorchestra.jobs.ingest_exchange import *; print('Exchange jobs OK')"
python -c "from lorchestra.jobs.ingest_stripe import *; print('Stripe jobs OK')"
```

**Outputs:**

- Updated job files (if needed)

---

### Step 4: Delete Legacy Auth Scripts [G1: Cleanup]

**Prompt:**

Delete all legacy OAuth setup scripts from `scripts/`:

1. Delete `scripts/gmail_oauth_url.py`
2. Delete `scripts/gmail_oauth_url_acct2.py`
3. Delete `scripts/gmail_oauth_url_acct3.py`
4. Delete `scripts/gmail_exchange_code.py`
5. Delete `scripts/gmail_exchange_code_acct2.py`
6. Delete `scripts/gmail_exchange_code_acct3.py`
7. Delete `scripts/get_gmail_refresh_token.py`

Keep utility scripts that are still useful:
- `scripts/daily_ingest.sh` - cron runner
- `scripts/cleanup_test_data.py` - maintenance
- `scripts/query_events.py` - debugging

**Commands:**

```bash
ls scripts/
# After deletion, should only show: daily_ingest.sh, cleanup_test_data.py, query_events.py, etc.
```

**Outputs:**

- Deleted 7 Gmail OAuth scripts

---

### Step 5: Clean .env File [G1: Cleanup]

**Prompt:**

Remove credential environment variables from `.env`:

Variables to REMOVE (now handled by authctl):
- `TAP_GMAIL_*` (all Gmail OAuth vars)
- `TAP_MSGRAPH_*` (all MS Graph vars)
- `STRIPE_API_KEY`
- Any `*_CLIENT_ID`, `*_CLIENT_SECRET`, `*_REFRESH_TOKEN` vars

Variables to KEEP:
- `AUTHCTL_HOME=/workspace/.authctl` (added in Step 1)
- `GCP_PROJECT`
- `DATASET_NAME`
- `GOOGLE_APPLICATION_CREDENTIALS`
- Any non-credential config vars

**Commands:**

```bash
# Show what credential vars exist
grep -E "TAP_|STRIPE_|CLIENT_ID|CLIENT_SECRET|REFRESH_TOKEN" .env || echo "No credential vars found"
```

**Outputs:**

- Updated `/workspace/lorchestra/.env` with credential vars removed

---

### Step 6: Dry-Run Validation [G2: Pre-Release]

**Prompt:**

Verify all jobs work with `--dry-run`:

1. Gmail jobs
2. Exchange jobs
3. Stripe jobs

**Commands:**

```bash
source .env

# Gmail
lorchestra run gmail_ingest_acct1 --dry-run
lorchestra run gmail_ingest_acct2 --dry-run
lorchestra run gmail_ingest_acct3 --dry-run

# Exchange
lorchestra run exchange_ingest_ben_mensio --dry-run
lorchestra run exchange_ingest_booking_mensio --dry-run
lorchestra run exchange_ingest_info_mensio --dry-run

# Stripe
lorchestra run stripe_ingest_customers --dry-run
lorchestra run stripe_ingest_invoices --dry-run
```

**Outputs:**

- All dry-run jobs complete without errors

---

### Step 7: Production Validation [G2: Pre-Release]

**Prompt:**

Run one job of each type with production data to verify end-to-end:

**Commands:**

```bash
source .env

# One Gmail job
lorchestra run gmail_ingest_acct1

# One Exchange job
lorchestra run exchange_ingest_ben_mensio

# One Stripe job
lorchestra run stripe_ingest_customers

# Verify in BigQuery
python -c "
from google.cloud import bigquery
client = bigquery.Client()
for row in client.query('SELECT source_system, COUNT(*) as c FROM events_dev.raw_objects GROUP BY 1'):
    print(f'{row.source_system}: {row.c}')
"
```

**Outputs:**

- Jobs complete successfully
- Data appears in BigQuery

## Models & Tools

**Tools:** bash, lorchestra, python

**Models:** (defaults)

## Repository

**Branch:** `main`

**Merge Strategy:** squash

## Files to Delete

After migration:
- `scripts/gmail_oauth_url.py`
- `scripts/gmail_oauth_url_acct2.py`
- `scripts/gmail_oauth_url_acct3.py`
- `scripts/gmail_exchange_code.py`
- `scripts/gmail_exchange_code_acct2.py`
- `scripts/gmail_exchange_code_acct3.py`
- `scripts/get_gmail_refresh_token.py`

## Files to Keep

- `scripts/daily_ingest.sh` - cron runner (still useful)
- `scripts/cleanup_test_data.py` - maintenance utility
- `scripts/query_events.py` - debugging utility
- `scripts/debug_bq_failures.py` - debugging utility
- `scripts/nuclear_clean.py` - maintenance utility

## Dependencies

This spec depends on:
- `authctl` package being fully implemented and installed
- All accounts re-authorized via `authctl auth ...` commands
- `$AUTHCTL_HOME` set to `/workspace/.authctl`