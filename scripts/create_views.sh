#!/bin/bash
# DEPRECATED: This script is replaced by lorchestra composite jobs.
# Use: lorchestra run pipeline.views
# Or:  life pipeline views (after life-cli integration)
#
# This script will be removed in a future release.
# See: /workspace/lorchestra/.specwright/specs/pipeline-composite-jobs.md
#
# ============================================================
# LEGACY SCRIPT - DO NOT ADD NEW JOBS HERE
# The composite job definitions are now the source of truth.
# ============================================================
#
# Create/recreate all BQ projection views
#
# Usage: ./scripts/create_views.sh
#
# This script creates the BigQuery views that projections depend on.
# Run this once initially, or when projection SQL changes.
#
# Prerequisites:
#   - .env file with GOOGLE_APPLICATION_CREDENTIALS
#   - Python venv at .venv/

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Load environment
source .venv/bin/activate
source .env

run_job() {
    local job_name="$1"
    echo "Creating: $job_name"
    lorchestra run "$job_name"
}

echo "=== Creating BQ Projection Views ==="
echo "Started: $(date)"
echo ""

run_job view_proj_clients
run_job view_proj_sessions
run_job view_proj_transcripts
run_job view_proj_clinical_documents
run_job view_proj_form_responses
run_job view_proj_contact_events

echo ""
echo "=== Views Created ==="
echo "Finished: $(date)"
