#!/bin/bash
# Daily formation for all canonical form responses
#
# Transforms canonical form_response objects into measurement_events and observations.
# Only processes records not yet formed or re-canonized since last formation.
# Uses MERGE by idem_key for idempotent writes.
#
# Run with: ./scripts/daily_form.sh
#
# For cron (example - daily at 8am, after canonization):
#   0 8 * * * cd /path/to/lorchestra && ./scripts/daily_form.sh >> logs/daily_form.log 2>&1

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

echo "=== Daily Formation: $(date -Iseconds) ==="

# ============================================================
# FORM RESPONSE → MEASUREMENT EVENTS + OBSERVATIONS
# ============================================================

echo "--- Intake Form #1 (PHQ-9, GAD-7, SAFE, etc.) ---"
run_job form_intake_01

echo "--- Intake Form #2 (FSCRS, IPIP-NEO-60-C, PSS-10, PHLMS-10) ---"
run_job form_intake_02

echo "--- Follow-up Form (GAD-7, PHQ-9, IPIP-NEO-60-C, FSCRS) ---"
run_job form_followup

echo "=== Complete: $(date -Iseconds) ==="

# Report failures
if [ -n "$FAILED_JOBS" ]; then
    echo ""
    echo "!!! FAILED JOBS:$FAILED_JOBS"
    exit 1
fi
