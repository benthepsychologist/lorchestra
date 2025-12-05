#!/bin/bash
# Create/recreate all BQ projection views
#
# Usage: ./scripts/create_projections.sh
#
# This script creates the BigQuery views that the projection pipeline depends on.
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
    python -c "from lorchestra.job_runner import run_job; run_job('$job_name')"
}

echo "=== Creating BQ Projection Views ==="
echo "Started: $(date)"
echo ""

run_job create_proj_clients
run_job create_proj_sessions
run_job create_proj_transcripts
run_job create_proj_clinical_documents
run_job create_proj_form_responses

echo ""
echo "=== Views Created ==="
echo "Finished: $(date)"
