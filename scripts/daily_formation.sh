#!/bin/bash
# DEPRECATED: This script is replaced by lorchestra composite jobs.
# Use: lorchestra run pipeline.formation
# Or:  life pipeline formation (after life-cli integration)
#
# This script will be removed in a future release.
# See: /workspace/lorchestra/.specwright/specs/pipeline-composite-jobs.md
#
# ============================================================
# LEGACY SCRIPT - DO NOT ADD NEW JOBS HERE
# The composite job definitions are now the source of truth.
# ============================================================
#
# Daily formation pipeline - projects canonical form responses to measurement_events and observations
#
# This is a two-stage projection pipeline:
# 1. proj_me_* - Project canonical form_responses → measurement_events table
# 2. proj_obs_* - Project measurement_events → observations table (with final-form scoring)
#
# Both stages are incremental (only process new/updated records) and idempotent (MERGE by idem_key).
#
# Run with: ./scripts/daily_formation.sh
#
# For cron (example - daily at 8am, after canonization):
#   0 8 * * * cd /path/to/lorchestra && ./scripts/daily_formation.sh >> logs/daily_formation.log 2>&1

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Load environment
source .venv/bin/activate
set -a && source .env && set +a

# Track failures but continue
FAILED_JOBS=""

run_job() {
    echo "Running: $1"
    if ! lorchestra run "$1"; then
        FAILED_JOBS="$FAILED_JOBS $1"
    fi
}

echo "=== Daily Formation Pipeline ==="
echo "Started: $(date -Iseconds)"
echo ""

# ============================================================
# STAGE 1: CANONICAL → MEASUREMENT_EVENTS
# Projects canonical form_responses to measurement_events table
# ============================================================

echo "--- Stage 1: Projecting to measurement_events ---"
run_job proj_me_intake_01
run_job proj_me_intake_02
run_job proj_me_followup

# ============================================================
# STAGE 2: MEASUREMENT_EVENTS → OBSERVATIONS
# Projects measurement_events to observations with final-form scoring
# ============================================================

echo ""
echo "--- Stage 2: Projecting to observations (with scoring) ---"
run_job proj_obs_intake_01
run_job proj_obs_intake_02
run_job proj_obs_followup

echo ""
echo "=== Formation Complete ==="
echo "Finished: $(date -Iseconds)"

# Report failures
if [ -n "$FAILED_JOBS" ]; then
    echo ""
    echo "!!! FAILED JOBS:$FAILED_JOBS"
    exit 1
fi
