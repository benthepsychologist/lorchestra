#!/bin/bash
# DEPRECATED: This script is replaced by lorchestra composite jobs.
# Use: lorchestra run pipeline.project
# Or:  life pipeline project (after life-cli integration)
#
# This script will be removed in a future release.
# See: /workspace/lorchestra/.specwright/specs/pipeline-composite-jobs.md
#
# ============================================================
# LEGACY SCRIPT - DO NOT ADD NEW JOBS HERE
# The composite job definitions are now the source of truth.
# ============================================================
#
# Daily local projection pipeline - syncs BQ to SQLite and projects to markdown files
#
# This is a two-stage local projection pipeline:
# 1. sync_proj_* - Sync BQ views/tables → SQLite tables
# 2. file_proj_* - Render SQLite tables → markdown files
#
# Usage: ./scripts/daily_local_projection.sh [--full-refresh]
#
# Options:
#   --full-refresh  Clear local_views and rebuild from scratch
#
# Prerequisites:
#   - .env file with GOOGLE_APPLICATION_CREDENTIALS
#   - Python venv at .venv/
#   - BQ projections already created (run create_views.sh first)
#   - Formation already run (run daily_formation.sh first for measurement data)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Load environment
source .venv/bin/activate
source .env

run_job() {
    local job_name="$1"
    echo "Running: $job_name"
    lorchestra run "$job_name"
}

# Parse args
FULL_REFRESH=false
if [[ "$1" == "--full-refresh" ]]; then
    FULL_REFRESH=true
fi

echo "=== Daily Local Projection Pipeline ==="
echo "Started: $(date)"
echo ""

# ============================================================
# STAGE 1: BQ → SQLITE
# Sync BQ views to local SQLite tables
# ============================================================

echo "--- Stage 1: Syncing BQ projections to SQLite ---"
run_job sync_proj_clients
run_job sync_proj_sessions
run_job sync_proj_transcripts
run_job sync_proj_clinical_documents
run_job sync_proj_form_responses
run_job sync_proj_contact_events
run_job sync_measurement_events
run_job sync_observations

# ============================================================
# FULL REFRESH (optional)
# ============================================================

if [[ "$FULL_REFRESH" == true ]]; then
    echo ""
    echo "--- Full refresh: clearing local_views ---"
    rm -rf ~/clinical-vault/views/*
fi

# ============================================================
# STAGE 2: SQLITE → MARKDOWN FILES
# Render SQLite data to markdown files
# ============================================================

echo ""
echo "--- Stage 2: Projecting to markdown files ---"
run_job file_proj_clients
run_job file_proj_transcripts
run_job file_proj_session_notes
run_job file_proj_session_summaries
run_job file_proj_reports

echo ""
echo "=== Pipeline Complete ==="
echo "Finished: $(date)"

# Print stats
echo ""
echo "--- Stats ---"
VIEWS_DIR="${CLINICAL_VAULT_PATH:-~/clinical-vault}/views"
echo "Clients: $(ls "$VIEWS_DIR" 2>/dev/null | wc -l)"
echo "Sessions: $(find "$VIEWS_DIR" -type d -name 'session-*' 2>/dev/null | wc -l)"
echo "Transcripts: $(find "$VIEWS_DIR" -name 'transcript.md' 2>/dev/null | wc -l)"
echo "Session notes: $(find "$VIEWS_DIR" -name 'session-note.md' 2>/dev/null | wc -l)"
echo "Session summaries: $(find "$VIEWS_DIR" -name 'session-summary.md' 2>/dev/null | wc -l)"
echo "Reports: $(find "$VIEWS_DIR" -name '*-progress-report.md' 2>/dev/null | wc -l)"
