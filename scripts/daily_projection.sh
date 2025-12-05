#!/bin/bash
# Daily projection pipeline - syncs BQ views to SQLite and projects to markdown files
#
# Usage: ./scripts/daily_projection.sh [--full-refresh]
#
# Options:
#   --full-refresh  Clear local_views and rebuild from scratch
#
# Prerequisites:
#   - .env file with GOOGLE_APPLICATION_CREDENTIALS
#   - Python venv at .venv/
#   - BQ projections already created (run create_projections.sh first)

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
    python -c "from lorchestra.job_runner import run_job; run_job('$job_name')"
}

# Parse args
FULL_REFRESH=false
if [[ "$1" == "--full-refresh" ]]; then
    FULL_REFRESH=true
fi

echo "=== Daily Projection Pipeline ==="
echo "Started: $(date)"
echo ""

# Step 1: Sync BQ projections to SQLite
echo "--- Syncing BQ projections to SQLite ---"
run_job sync_proj_clients
run_job sync_proj_sessions
run_job sync_proj_transcripts
run_job sync_proj_clinical_documents
run_job sync_proj_form_responses

# Step 2: Clear existing files if full refresh
if [[ "$FULL_REFRESH" == true ]]; then
    echo ""
    echo "--- Full refresh: clearing local_views ---"
    rm -rf ~/lifeos/local_views/*
fi

# Step 3: Project to markdown files
echo ""
echo "--- Projecting to markdown files ---"
run_job project_contacts
run_job project_transcripts
run_job project_session_notes
run_job project_session_summaries
run_job project_reports

echo ""
echo "=== Pipeline Complete ==="
echo "Finished: $(date)"

# Print stats
echo ""
echo "--- Stats ---"
echo "Clients: $(ls ~/lifeos/local_views/ 2>/dev/null | wc -l)"
echo "Sessions: $(find ~/lifeos/local_views -type d -name 'session-*' 2>/dev/null | wc -l)"
echo "Transcripts: $(find ~/lifeos/local_views -name 'transcript.md' 2>/dev/null | wc -l)"
echo "Session notes: $(find ~/lifeos/local_views -name 'session-note.md' 2>/dev/null | wc -l)"
echo "Session summaries: $(find ~/lifeos/local_views -name 'session-summary.md' 2>/dev/null | wc -l)"
echo "Reports: $(find ~/lifeos/local_views -name '*-progress-report.md' 2>/dev/null | wc -l)"
