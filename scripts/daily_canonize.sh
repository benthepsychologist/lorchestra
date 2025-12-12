#!/bin/bash
# DEPRECATED: This script is replaced by lorchestra composite jobs.
# Use: lorchestra run pipeline.canonize
# Or:  life pipeline canonize (after life-cli integration)
#
# This script will be removed in a future release.
# See: /workspace/lorchestra/.specwright/specs/pipeline-composite-jobs.md
#
# ============================================================
# LEGACY SCRIPT - DO NOT ADD NEW JOBS HERE
# The composite job definitions are now the source of truth.
# ============================================================
#
# Daily canonization for all validated records
#
# Transforms validated raw records into canonical format.
# Only processes records with validation_status='pass'.
# Uses deduplication to skip already-canonized records.
#
# Run with: ./scripts/daily_canonize.sh
#
# For cron (example - daily at 7am, after ingestion):
#   0 7 * * * cd /path/to/lorchestra && ./scripts/daily_canonize.sh >> logs/daily_canonize.log 2>&1

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

echo "=== Daily Canonization: $(date -Iseconds) ==="

# ============================================================
# PHASE 1: EMAIL CANONIZATION
# ============================================================

echo "--- Gmail → JMAP Lite ---"
run_job canonize_gmail_jmap

echo "--- Exchange → JMAP Lite ---"
run_job canonize_exchange_jmap

# ============================================================
# PHASE 2: DATAVERSE CANONIZATION
# ============================================================

echo "--- Dataverse Contacts → Contact ---"
run_job canonize_dataverse_contacts

echo "--- Dataverse Sessions → Clinical Session ---"
run_job canonize_dataverse_sessions

echo "--- Dataverse Sessions → Session Transcripts ---"
run_job canonize_dataverse_transcripts

echo "--- Dataverse Sessions → Session Notes ---"
run_job canonize_dataverse_session_notes

echo "--- Dataverse Sessions → Session Summaries ---"
run_job canonize_dataverse_session_summaries

echo "--- Dataverse Reports → Clinical Document ---"
run_job canonize_dataverse_reports

# ============================================================
# PHASE 3: STRIPE CANONIZATION
# ============================================================

echo "--- Stripe Customers → Customer ---"
run_job canonize_stripe_customers

echo "--- Stripe Invoices → Invoice ---"
run_job canonize_stripe_invoices

echo "--- Stripe Payments → Payment ---"
run_job canonize_stripe_payment_intents

echo "--- Stripe Refunds → Refund ---"
run_job canonize_stripe_refunds

# ============================================================
# PHASE 4: GOOGLE FORMS CANONIZATION
# ============================================================

echo "--- Google Forms → Form Response ---"
run_job canonize_google_forms

echo "=== Complete: $(date -Iseconds) ==="

# Report failures
if [ -n "$FAILED_JOBS" ]; then
    echo ""
    echo "!!! FAILED JOBS:$FAILED_JOBS"
    exit 1
fi
