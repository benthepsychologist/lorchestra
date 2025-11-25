---
version: "0.1"
tier: B
title: 2024-2025 Email Ingestion - Month-by-Month for All Accounts
owner: benthepsychologist
goal: Ingest all 2024-2025 emails from 7 accounts (3 Gmail + 4 Exchange) month-by-month with validation
labels: [data-ingestion, production, email]
project_slug: lorchestra
spec_version: 1.0.0
created: 2025-11-24
updated: 2025-11-24
orchestrator_contract: "standard"
repo:
  working_branch: "feat/2024-2025-email-ingestion"
---

# 2024-2025 Email Ingestion - Month-by-Month for All Accounts

## Objective

> Ingest all emails from January 2024 through November 2025 (23 months) from 7 email accounts month-by-month with comprehensive validation and monitoring

## Acceptance Criteria

- [ ] All 7 email accounts successfully authenticated and tested
  - Gmail: acct1 (ben@getmensio.com), acct2 (drben@benthepsychologist.com), acct3 (bfarmstrong@gmail.com)
  - Exchange: ben@mensio.com, booking@mensio.com, info@mensio.com, ben@efs.com
- [ ] All 23 months (Jan 2024 - Nov 2025) ingested for each working account
- [ ] BigQuery raw_objects contains deduplicated emails from all accounts
- [ ] Zero data loss - all extracted emails persisted to BigQuery
- [ ] Comprehensive validation report generated after each month
- [ ] Helper scripts created for automation and monitoring
- [ ] Stop-on-failure protocol: any account failure halts ingestion for that month

## Context

### Background

Currently we have partial email data:
- **Gmail acct1** (ben@getmensio.com): 264 emails successfully ingested (recent test runs)
- **Gmail acct3** (bfarmstrong@gmail.com): OAuth just fixed (conflicting scopes issue resolved)
- **Gmail acct2** (drben@benthepsychologist.com): Needs refresh token generation
- **Exchange accounts**: 3 emails from ben@mensio ingested, auth issues to investigate

The lorchestra → BigQuery pipeline is working correctly. The goal is to systematically ingest all 2024-2025 data month-by-month with validation checkpoints.

**Timeline:** 23 months total
- 2024: 12 months (Jan-Dec)
- 2025: 11 months (Jan-Nov, current month)

### Constraints

- No edits to core ingestion logic (`lorchestra/stack_clients/event_client.py`, `lorchestra/idem_keys.py`)
- No edits to Meltano configuration unless auth fixes required
- Must preserve audit trail in BigQuery `event_log` table
- Stop-on-failure: If any account fails for a month, halt, fix, and resume
- Manual process: Month-by-month execution with human validation between months

## Plan

### Step 1: Authentication Setup - Gmail acct2 [G0: Auth Validation]

**Prompt:**

Generate OAuth refresh token for Gmail acct2 (drben@benthepsychologist.com):

1. Use existing Mensio Workspace OAuth app (same as acct1):
   - CLIENT_ID: `73154799202-gt80uer1ou8asagrihob5ov994oekbll.apps.googleusercontent.com`
   - CLIENT_SECRET: `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`
2. Generate OOB OAuth URL with ONLY `gmail.readonly` scope (NO gmail.metadata)
3. User authorizes and provides authorization code
4. Exchange code for refresh token
5. Update both .env files (`/workspace/lorchestra/.env` and `/workspace/ingestor/.env`)
6. Export env vars in current shell:
   ```bash
   export TAP_GMAIL_ACCT2_BUSINESS1_REFRESH_TOKEN="<new_token>"
   ```
7. Test with January 2024 ingestion:
   ```bash
   lorchestra run gmail_ingest_acct2 --since 2024-01-01 --until 2024-01-31
   ```

**Commands:**

```bash
# Generate OAuth URL (using existing scripts pattern)
python3 scripts/gmail_oauth_url.py  # Modify for acct2

# After user authorization, exchange code
python3 scripts/gmail_exchange_code.py <AUTH_CODE>  # Modify for acct2

# Export new token
export TAP_GMAIL_ACCT2_BUSINESS1_REFRESH_TOKEN="<token>"

# Test ingestion
source .venv/bin/activate
lorchestra run gmail_ingest_acct2 --since 2024-01-01 --until 2024-01-31
```

**Validation:**

Before proceeding to gate review, verify:
- ✓ Refresh token successfully generated
- ✓ Both .env files updated with new token
- ✓ Test ingestion for January 2024 completes with `ingestion.completed` event
- ✓ BigQuery event_log shows status="ok"
- ✓ No 403 Forbidden errors in logs

**Outputs:**

- `.env` (updated TAP_GMAIL_ACCT2_BUSINESS1_REFRESH_TOKEN in both repos)
- `scripts/gmail_oauth_url_acct2.py` (if new script created)
- `scripts/gmail_exchange_code_acct2.py` (if new script created)

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Authentication Validation
- [ ] OAuth refresh token successfully generated
- [ ] Token has ONLY gmail.readonly scope (verified via tokeninfo API)
- [ ] Both .env files contain identical acct2 credentials
- [ ] Environment variables exported in current shell

##### Testing
- [ ] January 2024 test ingestion completed successfully
- [ ] event_log shows `ingestion.completed` with records_extracted > 0
- [ ] raw_objects table contains acct2 emails with correct idem_key format
- [ ] No auth errors in Meltano logs

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 2: Authentication Setup - Exchange Accounts [G1: Auth Validation]

**Prompt:**

Investigate and fix Exchange account authentication issues:

1. Review previous Exchange failures from event_log (403/401 errors)
2. Check Azure AD app permissions for Microsoft Graph Mail API:
   - Tenant: f1b164de-53d7-4ace-8793-a89a107592d7
   - Client: af384e69-5c77-4edb-b2b6-87e7bdda5a28
   - Required scopes: Mail.ReadWrite, Mail.Send, offline_access
3. Verify refresh tokens are not expired (check token expiration)
4. Test each Exchange account with January 2024 ingestion:
   - ben@mensio.com: `lorchestra run exchange_ingest_ben_mensio --since 2024-01-01 --until 2024-01-31`
   - booking@mensio.com: `lorchestra run exchange_ingest_booking_mensio --since 2024-01-01 --until 2024-01-31`
   - info@mensio.com: `lorchestra run exchange_ingest_info_mensio --since 2024-01-01 --until 2024-01-31`
   - ben@efs.com: `lorchestra run exchange_ingest_ben_efs --since 2024-01-01 --until 2024-01-31`
5. Document any auth fixes needed (token refresh, permission grants, etc.)
6. If ben@efs.com uses different tenant, verify tenant_id

**Commands:**

```bash
# Query event_log for Exchange failures
python3 scripts/debug_bq_failures.py | grep -A 10 "tap-msgraph-mail"

# Test each Exchange account
source .venv/bin/activate
lorchestra run exchange_ingest_ben_mensio --since 2024-01-01 --until 2024-01-31
lorchestra run exchange_ingest_booking_mensio --since 2024-01-01 --until 2024-01-31
lorchestra run exchange_ingest_info_mensio --since 2024-01-01 --until 2024-01-31
lorchestra run exchange_ingest_ben_efs --since 2024-01-01 --until 2024-01-31
```

**Validation:**

Before proceeding to gate review, verify:
- ✓ All 4 Exchange accounts successfully authenticate
- ✓ January 2024 test ingestion completes for each account
- ✓ event_log shows `ingestion.completed` for all Exchange jobs
- ✓ No 401/403 auth errors in logs
- ✓ Any required Azure AD permission grants documented

**Outputs:**

- `docs/EXCHANGE_AUTH_TROUBLESHOOTING.md` (if issues found and fixed)
- `.env` (if new tokens generated)
- Test ingestion results logged to BigQuery event_log

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Authentication Validation
- [ ] All 4 Exchange accounts successfully authenticated
- [ ] Azure AD app permissions verified and correct
- [ ] Refresh tokens valid and not expired
- [ ] ben@efs.com tenant ID verified (may be different from Mensio)

##### Testing
- [ ] January 2024 test ingestion passed for ben@mensio
- [ ] January 2024 test ingestion passed for booking@mensio
- [ ] January 2024 test ingestion passed for info@mensio
- [ ] January 2024 test ingestion passed for ben@efs
- [ ] event_log shows no auth failures

##### Documentation
- [ ] Any auth fixes documented in troubleshooting guide
- [ ] Token refresh procedures documented if needed

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 3: Create Helper Scripts [G2: Code Review]

**Prompt:**

Create three helper scripts for month-by-month ingestion automation:

**1. `scripts/run_month_all_accounts.py`**
- Accept `--month` parameter (format: "2024-01")
- Calculate month start/end dates
- Run all 7 lorchestra jobs in parallel for that month:
  - Gmail: acct1, acct2, acct3
  - Exchange: ben-mensio, booking-mensio, info-mensio, ben-efs
- Monitor output for errors
- Exit with non-zero code if ANY job fails
- Print summary: accounts completed, records extracted per account

**2. `scripts/verify_month_ingestion.py`**
- Accept `--month` parameter (format: "2024-01")
- Query BigQuery for ingestion results for that month:
  - Check event_log for all 7 accounts
  - Verify `ingestion.completed` events exist
  - Extract records_extracted counts from payload
  - Check raw_objects for email presence (count by source_system)
- Print validation report:
  - ✓ Account success/failure
  - Records extracted vs records in raw_objects
  - Any gaps or anomalies

**3. `scripts/generate_ingestion_report.py`**
- Query BigQuery for complete 2024-2025 ingestion status
- Report per account:
  - Total emails ingested
  - Date range coverage (first_seen → last_seen)
  - Months with data vs expected months (23 months)
  - Any gaps (months with 0 emails)
- Summary statistics:
  - Total emails across all accounts
  - BigQuery storage size estimate
  - Ingestion duration per account

**Commands:**

```bash
# Test the scripts
python3 scripts/run_month_all_accounts.py --month 2024-01
python3 scripts/verify_month_ingestion.py --month 2024-01
python3 scripts/generate_ingestion_report.py
```

**Validation:**

Before proceeding to gate review, verify:
- ✓ All 3 scripts execute without errors
- ✓ run_month_all_accounts.py successfully runs January 2024 for all accounts
- ✓ verify_month_ingestion.py produces accurate validation report
- ✓ generate_ingestion_report.py queries BigQuery successfully
- ✓ Scripts handle errors gracefully (connection failures, missing data)

**Outputs:**

- `scripts/run_month_all_accounts.py` (new)
- `scripts/verify_month_ingestion.py` (new)
- `scripts/generate_ingestion_report.py` (new)
- `docs/INGESTION_SCRIPTS.md` (usage documentation)

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Code Quality
- [ ] Scripts follow Python best practices (type hints, docstrings)
- [ ] Error handling is comprehensive (API failures, auth errors, missing data)
- [ ] Scripts are idempotent (safe to re-run)
- [ ] Command-line interfaces are clear and intuitive

##### Functionality
- [ ] run_month_all_accounts.py successfully runs all 7 accounts in parallel
- [ ] verify_month_ingestion.py accurately validates BigQuery data
- [ ] generate_ingestion_report.py produces comprehensive summary
- [ ] Scripts exit with correct status codes (0=success, non-zero=failure)

##### Testing
- [ ] Scripts tested with January 2024 data
- [ ] Error cases tested (missing accounts, auth failures)
- [ ] Output formatting is readable and actionable

##### Documentation
- [ ] INGESTION_SCRIPTS.md documents all 3 scripts with examples
- [ ] Usage instructions are clear for non-technical users
- [ ] Error messages are helpful and actionable

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 4: 2024 Ingestion - Months 1-6 (Jan-Jun 2024) [G3: Validation Checkpoint]

**Prompt:**

Execute ingestion for first 6 months of 2024:

For each month (Jan through Jun 2024):
1. Run: `python3 scripts/run_month_all_accounts.py --month 2024-MM`
2. Monitor for failures (script exits non-zero if any account fails)
3. STOP if any account fails, fix, re-run
4. Verify: `python3 scripts/verify_month_ingestion.py --month 2024-MM`
5. Press ENTER to continue to next month

**Commands:**

```bash
# Process Jan-Jun 2024
for month in 01 02 03 04 05 06; do
  echo "Processing 2024-$month..."
  python3 scripts/run_month_all_accounts.py --month 2024-$month
  python3 scripts/verify_month_ingestion.py --month 2024-$month
  echo "2024-$month complete. Press ENTER to continue..."
  read
done
```

**Validation:**

- ✓ All 6 months processed successfully
- ✓ All 7 accounts completed each month
- ✓ Verification reports show all ✓

**Outputs:**

- BigQuery data (event_log, raw_objects) for Jan-Jun 2024
- Validation reports: `artifacts/ingestion/2024-{01-06}-validation-report.md`

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Ingestion Completion
- [ ] January 2024 ingested successfully (all 7 accounts)
- [ ] February 2024 ingested successfully (all 7 accounts)
- [ ] March 2024 ingested successfully (all 7 accounts)
- [ ] April 2024 ingested successfully (all 7 accounts)
- [ ] May 2024 ingested successfully (all 7 accounts)
- [ ] June 2024 ingested successfully (all 7 accounts)

##### Data Quality
- [ ] No months with suspiciously low email counts
- [ ] No duplicate emails across months
- [ ] All expected accounts present in each month
- [ ] Deduplication working correctly (idem_key)

#### Approval Decision
- [ ] APPROVED - Proceed to Jul-Dec 2024
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 5: 2024 Ingestion - Months 7-12 (Jul-Dec 2024) [G4: Validation Checkpoint]

**Prompt:**

Execute ingestion for last 6 months of 2024:

For each month (Jul through Dec 2024):
1. Run: `python3 scripts/run_month_all_accounts.py --month 2024-MM`
2. Monitor for failures
3. STOP if any account fails, fix, re-run
4. Verify: `python3 scripts/verify_month_ingestion.py --month 2024-MM`
5. Press ENTER to continue to next month

**Commands:**

```bash
# Process Jul-Dec 2024
for month in 07 08 09 10 11 12; do
  echo "Processing 2024-$month..."
  python3 scripts/run_month_all_accounts.py --month 2024-$month
  python3 scripts/verify_month_ingestion.py --month 2024-$month
  echo "2024-$month complete. Press ENTER to continue..."
  read
done
```

**Validation:**

- ✓ All 6 months processed successfully
- ✓ All 7 accounts completed each month
- ✓ Verification reports show all ✓
- ✓ 2024 calendar year complete (12 months)

**Outputs:**

- BigQuery data (event_log, raw_objects) for Jul-Dec 2024
- Validation reports: `artifacts/ingestion/2024-{07-12}-validation-report.md`

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Ingestion Completion
- [ ] July 2024 ingested successfully (all 7 accounts)
- [ ] August 2024 ingested successfully (all 7 accounts)
- [ ] September 2024 ingested successfully (all 7 accounts)
- [ ] October 2024 ingested successfully (all 7 accounts)
- [ ] November 2024 ingested successfully (all 7 accounts)
- [ ] December 2024 ingested successfully (all 7 accounts)

##### Data Quality
- [ ] Email counts consistent with expected patterns
- [ ] No data quality issues observed
- [ ] All accounts functioning correctly

#### Approval Decision
- [ ] APPROVED - Proceed to 2025 ingestion
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 6: 2025 Ingestion - Months 1-11 (Jan-Nov 2025) [G5: Validation Checkpoint]

**Prompt:**

Execute ingestion for 2025 year-to-date (11 months):

For each month (Jan through Nov 2025):
1. Run: `python3 scripts/run_month_all_accounts.py --month 2025-MM`
2. Monitor for failures
3. STOP if any account fails, fix, re-run
4. Verify: `python3 scripts/verify_month_ingestion.py --month 2025-MM`
5. Press ENTER to continue to next month

**Commands:**

```bash
# Process Jan-Nov 2025
for month in 01 02 03 04 05 06 07 08 09 10 11; do
  echo "Processing 2025-$month..."
  python3 scripts/run_month_all_accounts.py --month 2025-$month
  python3 scripts/verify_month_ingestion.py --month 2025-$month
  echo "2025-$month complete. Press ENTER to continue..."
  read
done
```

**Validation:**

- ✓ All 11 months processed successfully
- ✓ All 7 accounts completed each month
- ✓ Verification reports show all ✓
- ✓ 2025 year-to-date complete

**Outputs:**

- BigQuery data (event_log, raw_objects) for Jan-Nov 2025
- Validation reports: `artifacts/ingestion/2025-{01-11}-validation-report.md`

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Ingestion Completion
- [ ] January 2025 ingested successfully (all 7 accounts)
- [ ] February 2025 ingested successfully (all 7 accounts)
- [ ] March 2025 ingested successfully (all 7 accounts)
- [ ] April 2025 ingested successfully (all 7 accounts)
- [ ] May 2025 ingested successfully (all 7 accounts)
- [ ] June 2025 ingested successfully (all 7 accounts)
- [ ] July 2025 ingested successfully (all 7 accounts)
- [ ] August 2025 ingested successfully (all 7 accounts)
- [ ] September 2025 ingested successfully (all 7 accounts)
- [ ] October 2025 ingested successfully (all 7 accounts)
- [ ] November 2025 ingested successfully (all 7 accounts)

##### Data Quality
- [ ] Recent months (Oct-Nov 2025) have expected email volumes
- [ ] No data quality issues observed
- [ ] All accounts remain authenticated

#### Approval Decision
- [ ] APPROVED - Proceed to final validation
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 7: Final Validation & Reporting [G6: Final Approval]

**Prompt:**

Generate comprehensive final report for 2024-2025 ingestion:

1. Run complete ingestion report:
   ```bash
   python3 scripts/generate_ingestion_report.py
   ```
2. Analyze results:
   - Total emails ingested per account
   - Date range coverage (should be Jan 1, 2024 - Nov 30, 2025)
   - Any months with 0 or anomalously low counts
   - Total emails across all accounts (23 months × 7 accounts)
3. Query BigQuery for data completeness:
   - Count emails per account per month (23 months × 7 accounts = 161 data points)
   - Identify any gaps
   - Compare against expected volumes
4. Document any issues encountered during entire process
5. Create summary report with:
   - Success metrics (emails ingested, accounts completed, months covered)
   - Timeline (total time for 23 months)
   - Any accounts that required special handling
   - Recommendations for future ingestions (December 2025 onwards)

**Commands:**

```bash
# Generate final report
python3 scripts/generate_ingestion_report.py > artifacts/ingestion/2024-2025-final-report.md

# Query BigQuery for complete month-by-month breakdown
python3 << 'EOF'
from google.cloud import bigquery
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspace/lorchestra/credentials/lorchestra-events-key.json'
client = bigquery.Client(project='local-orchestration')

query = '''
SELECT
  source_system,
  FORMAT_TIMESTAMP('%Y-%m', first_seen) as month,
  COUNT(*) as email_count,
  MIN(first_seen) as earliest,
  MAX(last_seen) as latest
FROM `local-orchestration.events_dev.raw_objects`
WHERE DATE(first_seen) BETWEEN '2024-01-01' AND '2025-11-30'
GROUP BY source_system, month
ORDER BY source_system, month
'''

result = client.query(query).result()
print("\n=== COMPLETE 2024-2025 INGESTION BREAKDOWN ===\n")
for row in result:
    print(f"{row.source_system:40} | {row.month} | {row.email_count:6} emails | {row.earliest} → {row.latest}")

# Summary totals
query_total = '''
SELECT
  source_system,
  COUNT(*) as total_emails,
  MIN(first_seen) as earliest,
  MAX(last_seen) as latest
FROM `local-orchestration.events_dev.raw_objects`
WHERE DATE(first_seen) BETWEEN '2024-01-01' AND '2025-11-30'
GROUP BY source_system
ORDER BY total_emails DESC
'''

result = client.query(query_total).result()
print("\n=== TOTALS BY ACCOUNT ===\n")
grand_total = 0
for row in result:
    print(f"{row.source_system:40} | {row.total_emails:7} emails | {row.earliest.date()} → {row.latest.date()}")
    grand_total += row.total_emails

print(f"\n{'GRAND TOTAL':40} | {grand_total:7} emails")
print(f"{'Coverage':40} | 23 months (Jan 2024 - Nov 2025)")
EOF
```

**Validation:**

- ✓ Final report generated successfully
- ✓ All 7 accounts have data for Jan 2024 - Nov 2025 (23 months)
- ✓ No critical gaps identified
- ✓ Total email count is reasonable (expected: thousands to tens of thousands)
- ✓ Month-by-month breakdown shows consistent patterns

**Outputs:**

- `artifacts/ingestion/2024-2025-final-report.md`
- `artifacts/ingestion/2024-2025-month-by-month-breakdown.md`
- `docs/PRODUCTION_INGESTION_SUMMARY.md`

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Completeness
- [ ] All 7 accounts have data for Jan 2024 - Nov 2025 (23 months)
- [ ] No months are missing for any account (161 expected data points)
- [ ] Total email counts are within expected ranges
- [ ] Date range coverage is complete (2024-01-01 to 2025-11-30)

##### Data Quality
- [ ] No duplicate emails (idem_key deduplication verified)
- [ ] Email payloads are complete and valid
- [ ] Timestamps are accurate
- [ ] source_system values are correct

##### Documentation
- [ ] Final report is comprehensive and accurate
- [ ] Month-by-month breakdown shows expected patterns
- [ ] Any issues encountered are documented with resolutions
- [ ] Recommendations for future ingestions are actionable

##### Process Validation
- [ ] Stop-on-failure protocol was followed throughout
- [ ] All failures were caught, fixed, and documented
- [ ] Helper scripts proved effective for automation
- [ ] BigQuery storage is within expected limits

#### Approval Decision
- [ ] APPROVED - 2024-2025 ingestion complete
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

## Models & Tools

**Tools:** bash, python3, lorchestra, BigQuery (bq CLI), git

**Models:** (to be filled by defaults)

## Repository

**Branch:** `feat/2024-2025-email-ingestion`

**Merge Strategy:** squash
