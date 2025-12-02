#!/bin/bash
# Daily ingestion for all accounts
#
# Each job auto-detects its last sync from BigQuery and only fetches new data.
# Run with: ./scripts/daily_ingest.sh
#
# For cron (example - daily at 6am):
#   0 6 * * * cd /path/to/lorchestra && ./scripts/daily_ingest.sh >> logs/daily_ingest.log 2>&1

# Track failures but continue
FAILED_JOBS=""

run_job() {
    if ! lorchestra run "$1"; then
        FAILED_JOBS="$FAILED_JOBS $1"
    fi
}

# Activate environment
source .venv/bin/activate
set -a && source .env && set +a

echo "=== Daily Ingestion: $(date -Iseconds) ==="

# Gmail accounts
echo "--- Gmail ---"
run_job ingest_gmail_acct1
run_job ingest_gmail_acct2
run_job ingest_gmail_acct3

# Exchange accounts
echo "--- Exchange ---"
run_job ingest_exchange_ben_mensio
run_job ingest_exchange_booking_mensio
run_job ingest_exchange_info_mensio

# Dataverse CRM
echo "--- Dataverse ---"
run_job ingest_dataverse_contacts
run_job ingest_dataverse_sessions
run_job ingest_dataverse_reports

# Stripe payment data
echo "--- Stripe ---"
run_job ingest_stripe_customers
run_job ingest_stripe_invoices
run_job ingest_stripe_payment_intents
run_job ingest_stripe_refunds

# Google Forms
echo "--- Google Forms ---"
run_job ingest_google_forms_intake_01
run_job ingest_google_forms_intake_02
run_job ingest_google_forms_followup
run_job ingest_google_forms_ipip120

echo "=== Complete: $(date -Iseconds) ==="

# Report failures
if [ -n "$FAILED_JOBS" ]; then
    echo ""
    echo "!!! FAILED JOBS:$FAILED_JOBS"
    exit 1
fi
