#!/bin/bash
# Recanonize sessions - deletes existing canonical sessions and re-runs canonization
#
# Usage: ./scripts/recanonize_sessions.sh
#
# Use this when the clinical_session transform changes and you need to regenerate
# canonical session objects with new fields (e.g., session_num, contact_id).
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

echo "=== Recanonizing Sessions ==="
echo "Started: $(date)"
echo ""

# Step 1: Delete existing canonical sessions
echo "--- Deleting existing canonical sessions ---"
python -c "
from google.cloud import bigquery
client = bigquery.Client()
q = '''
DELETE FROM \`local-orchestration.events_dev.canonical_objects\`
WHERE canonical_schema = 'iglu:org.canonical/clinical_session/jsonschema/2-0-0'
'''
result = client.query(q).result()
print('Deleted canonical sessions')
"

# Step 2: Re-run canonization
echo ""
echo "--- Re-canonizing sessions ---"
python -c "from lorchestra.job_runner import run_job; run_job('canonize_dataverse_sessions')"

echo ""
echo "=== Recanonization Complete ==="
echo "Finished: $(date)"

# Verify
echo ""
echo "--- Verification ---"
python -c "
from google.cloud import bigquery
client = bigquery.Client()
q = '''
SELECT COUNT(*) as cnt,
       COUNTIF(JSON_VALUE(payload, '\$.session_num') IS NOT NULL) as with_session_num,
       COUNTIF(JSON_VALUE(payload, '\$.contact_id') IS NOT NULL) as with_contact_id
FROM \`local-orchestration.events_dev.canonical_objects\`
WHERE canonical_schema = 'iglu:org.canonical/clinical_session/jsonschema/2-0-0'
'''
for row in client.query(q).result():
    print(f'Total sessions: {row.cnt}')
    print(f'With session_num: {row.with_session_num}')
    print(f'With contact_id: {row.with_contact_id}')
"
