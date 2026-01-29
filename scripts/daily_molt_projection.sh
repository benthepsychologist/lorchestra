#!/bin/bash
# Molt Context Projections — curate canonical data to molt-chatbot BQ dataset
#
# Projects pending emails, today's sessions, and queued actions from
# local-orchestration.canonical.canonical_objects to molt-chatbot.molt.context_*
#
# Prerequisites:
#   - lorchestra SA has bigquery.dataEditor on molt-chatbot.molt dataset
#   - .env file with GOOGLE_APPLICATION_CREDENTIALS
#   - Python venv at .venv/
#
# Usage: ./scripts/daily_molt_projection.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Load environment
source .venv/bin/activate
source .env

echo "=== Molt Context Projections ==="
echo "Started: $(date)"
echo ""

lorchestra run sync_molt_emails
lorchestra run sync_molt_calendar
lorchestra run sync_molt_actions

echo ""
echo "=== Done ==="
echo "Finished: $(date)"
