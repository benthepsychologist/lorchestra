#!/bin/bash
# Daily email ingestion for all accounts
#
# Each job auto-detects its last sync from BigQuery and only fetches new emails.
# Run with: ./scripts/daily_ingest.sh
#
# For cron (example - daily at 6am):
#   0 6 * * * cd /path/to/lorchestra && ./scripts/daily_ingest.sh >> logs/daily_ingest.log 2>&1

set -e  # Exit on first failure

# Activate environment
source .venv/bin/activate
set -a && source .env && set +a

echo "=== Daily Email Ingestion: $(date -Iseconds) ==="

# Gmail accounts
lorchestra run gmail_ingest_acct1
lorchestra run gmail_ingest_acct2
lorchestra run gmail_ingest_acct3

# Exchange accounts
lorchestra run exchange_ingest_ben_mensio
lorchestra run exchange_ingest_booking_mensio
lorchestra run exchange_ingest_info_mensio

echo "=== Complete: $(date -Iseconds) ==="
