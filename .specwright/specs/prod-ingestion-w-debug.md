---
version: "0.1"
tier: B
title: Last 90 Days Email Ingestion - All Accounts
owner: benthepsychologist
goal: Ingest emails from Sep 2025 - Nov 2025 (3 months) from 6 accounts month-by-month
labels: [data-ingestion, production, email]
project_slug: lorchestra
spec_version: 1.0.1
created: 2025-11-24
updated: 2025-11-25
orchestrator_contract: "standard"
repo:
  working_branch: "main"
---

# Last 90 Days Email Ingestion - All Accounts

## Objective

> Ingest emails from September 1, 2025 through November 25, 2025 (~90 days) from 6 email accounts month-by-month with validation

## Acceptance Criteria

- [x] All 6 email accounts successfully authenticated and tested
  - Gmail: acct1 (ben@getmensio.com), acct2 (drben@benthepsychologist.com), acct3 (bfarmstrong@gmail.com)
  - Exchange: ben@mensio.com, booking@mensio.com, info@mensio.com
- [x] All 3 months (Sep-Nov 2025) ingested for each account
- [x] BigQuery raw_objects contains deduplicated emails from all accounts
- [x] Zero data loss - all extracted emails persisted to BigQuery

### Final Results (2025-11-25)
| Account | Emails | Date Range |
|---------|--------|------------|
| exchange--ben-mensio | 3,272 | 2025-09-01 to 2025-11-25 |
| exchange--booking-mensio | 36 | 2025-09-03 to 2025-11-24 |
| exchange--info-mensio | 27 | 2025-09-01 to 2025-11-23 |
| gmail--acct1-personal | 670 | 2025-09-01 to 2025-11-24 |
| gmail--acct2-business1 | 123 | 2025-09-10 to 2025-11-25 |
| gmail--acct3-bfarmstrong | 3,492 | 2025-09-01 to 2025-11-25 |
| **TOTAL** | **7,620** | Sep-Nov 2025 |

## Context

### Working Accounts (verified 2025-11-25)

| Account | Type | Status |
|---------|------|--------|
| gmail:acct1 | Gmail | ✅ Working |
| gmail:acct2 | Gmail | ✅ Working |
| gmail:acct3 | Gmail | ✅ Working |
| exchange:ben-mensio | Exchange | ✅ Working |
| exchange:booking-mensio | Exchange | ✅ Working |
| exchange:info-mensio | Exchange | ✅ Working |

**Excluded:** exchange:ben-efs (evidencefirstsolutions.com - needs new stream setup)

### Timeline

- **September 2025**: 2025-09-01 to 2025-09-30
- **October 2025**: 2025-10-01 to 2025-10-31
- **November 2025**: 2025-11-01 to 2025-11-25

## Plan

### Step 1: September 2025 Ingestion [G0: Month Complete]

**Commands:**

```bash
source .venv/bin/activate && set -a && source .env && set +a

# Gmail accounts
lorchestra run gmail_ingest_acct1 --since 2025-09-01 --until 2025-10-01
lorchestra run gmail_ingest_acct2 --since 2025-09-01 --until 2025-10-01
lorchestra run gmail_ingest_acct3 --since 2025-09-01 --until 2025-10-01

# Exchange accounts
lorchestra run exchange_ingest_ben_mensio --since 2025-09-01 --until 2025-10-01
lorchestra run exchange_ingest_booking_mensio --since 2025-09-01 --until 2025-10-01
lorchestra run exchange_ingest_info_mensio --since 2025-09-01 --until 2025-10-01
```

**Validation:**
- [ ] All 6 accounts completed successfully
- [ ] BigQuery shows ingestion.completed events for all

---

### Step 2: October 2025 Ingestion [G1: Month Complete]

**Commands:**

```bash
# Gmail accounts
lorchestra run gmail_ingest_acct1 --since 2025-10-01 --until 2025-11-01
lorchestra run gmail_ingest_acct2 --since 2025-10-01 --until 2025-11-01
lorchestra run gmail_ingest_acct3 --since 2025-10-01 --until 2025-11-01

# Exchange accounts
lorchestra run exchange_ingest_ben_mensio --since 2025-10-01 --until 2025-11-01
lorchestra run exchange_ingest_booking_mensio --since 2025-10-01 --until 2025-11-01
lorchestra run exchange_ingest_info_mensio --since 2025-10-01 --until 2025-11-01
```

**Validation:**
- [ ] All 6 accounts completed successfully
- [ ] BigQuery shows ingestion.completed events for all

---

### Step 3: November 2025 Ingestion [G2: Month Complete]

**Commands:**

```bash
# Gmail accounts
lorchestra run gmail_ingest_acct1 --since 2025-11-01 --until 2025-11-26
lorchestra run gmail_ingest_acct2 --since 2025-11-01 --until 2025-11-26
lorchestra run gmail_ingest_acct3 --since 2025-11-01 --until 2025-11-26

# Exchange accounts
lorchestra run exchange_ingest_ben_mensio --since 2025-11-01 --until 2025-11-26
lorchestra run exchange_ingest_booking_mensio --since 2025-11-01 --until 2025-11-26
lorchestra run exchange_ingest_info_mensio --since 2025-11-01 --until 2025-11-26
```

**Validation:**
- [ ] All 6 accounts completed successfully
- [ ] BigQuery shows ingestion.completed events for all

---

### Step 4: Final Validation [G3: Complete]

**Validation Query:**

```sql
SELECT source_system, COUNT(*) as emails,
       MIN(DATE(first_seen)) as earliest,
       MAX(DATE(last_seen)) as latest
FROM `events_dev.raw_objects`
GROUP BY source_system
ORDER BY source_system
```

**Expected:**
- 6 source systems with data from Sep-Nov 2025
- No failures in event_log

## Models & Tools

**Tools:** lorchestra CLI, BigQuery

**Models:** N/A
