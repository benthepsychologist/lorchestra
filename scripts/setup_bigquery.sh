#!/bin/bash
#
# BigQuery Setup Script for lorchestra Event Pipeline
#
# This script sets up the BigQuery tables and service account for the
# three-layer ingestion architecture (ingestor → lorc → event_client).
#
# Prerequisites:
# - gcloud CLI installed and authenticated
# - GCP project with BigQuery API enabled
# - Sufficient permissions to create datasets, tables, and service accounts
#
# Usage:
#   export GCP_PROJECT=your-project-id
#   export DATASET_NAME=events_dev  # or events_prod
#   bash scripts/setup_bigquery.sh

set -e  # Exit on error

# Configuration
GCP_PROJECT="${GCP_PROJECT:-}"
DATASET_NAME="${DATASET_NAME:-events_dev}"
LOCATION="US"
SERVICE_ACCOUNT_NAME="lorchestra-events"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
error() {
    echo -e "${RED}ERROR: $1${NC}" >&2
    exit 1
}

success() {
    echo -e "${GREEN}✓ $1${NC}"
}

info() {
    echo -e "${YELLOW}→ $1${NC}"
}

# Validate prerequisites
if [ -z "$GCP_PROJECT" ]; then
    error "GCP_PROJECT environment variable not set. Set it with: export GCP_PROJECT=your-project-id"
fi

if ! command -v bq &> /dev/null; then
    error "bq command not found. Please install Google Cloud SDK: https://cloud.google.com/sdk/install"
fi

if ! command -v gcloud &> /dev/null; then
    error "gcloud command not found. Please install Google Cloud SDK: https://cloud.google.com/sdk/install"
fi

# Verify authentication
info "Verifying gcloud authentication..."
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
    error "Not authenticated with gcloud. Run: gcloud auth login"
fi
success "Authenticated"

# Set project
info "Setting GCP project to: $GCP_PROJECT"
gcloud config set project "$GCP_PROJECT" --quiet
success "Project set"

echo ""
echo "=========================================="
echo "BigQuery Setup for lorchestra"
echo "=========================================="
echo "Project: $GCP_PROJECT"
echo "Dataset: $DATASET_NAME"
echo "Location: $LOCATION"
echo "=========================================="
echo ""

# Step 1: Create dataset
info "Creating BigQuery dataset: $DATASET_NAME"
if bq ls | grep -q "$DATASET_NAME"; then
    echo "  Dataset already exists, skipping creation"
else
    bq mk --dataset --location="$LOCATION" \
        --description="Event pipeline storage for lorchestra" \
        "$GCP_PROJECT:$DATASET_NAME"
    success "Dataset created"
fi

# Step 2: Create event_log table
info "Creating event_log table..."
bq query --use_legacy_sql=false <<EOF
CREATE TABLE IF NOT EXISTS \`$GCP_PROJECT.$DATASET_NAME.event_log\` (
  event_id STRING NOT NULL,
  event_type STRING NOT NULL,
  source_system STRING NOT NULL,
  object_type STRING NOT NULL,
  idem_key STRING NOT NULL,
  correlation_id STRING,
  subject_id STRING,
  created_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  error_message STRING
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, object_type, event_type
OPTIONS(
  description="Event audit trail - one row per emit() call, no payload"
);
EOF
success "event_log table created"

# Step 3: Create raw_objects table
info "Creating raw_objects table..."
bq query --use_legacy_sql=false <<EOF
CREATE TABLE IF NOT EXISTS \`$GCP_PROJECT.$DATASET_NAME.raw_objects\` (
  idem_key STRING NOT NULL,
  source_system STRING NOT NULL,
  object_type STRING NOT NULL,
  external_id STRING,
  payload JSON NOT NULL,
  first_seen TIMESTAMP NOT NULL,
  last_seen TIMESTAMP NOT NULL
)
CLUSTER BY source_system, object_type
OPTIONS(
  description="Deduped object store - one row per idem_key, with payload"
);
EOF
success "raw_objects table created"

# Step 4: Add primary key constraint
info "Adding primary key constraint to raw_objects..."
bq query --use_legacy_sql=false <<EOF
ALTER TABLE \`$GCP_PROJECT.$DATASET_NAME.raw_objects\`
ADD PRIMARY KEY (idem_key) NOT ENFORCED;
EOF
success "Primary key constraint added"

# Step 5: Create service account
info "Creating service account: $SERVICE_ACCOUNT_NAME"
if gcloud iam service-accounts list --filter="email:$SERVICE_ACCOUNT_NAME@$GCP_PROJECT.iam.gserviceaccount.com" --format="value(email)" | grep -q "@"; then
    echo "  Service account already exists, skipping creation"
else
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --display-name="lorchestra Event Emitter" \
        --description="Service account for lorchestra event pipeline to write to BigQuery" \
        --project="$GCP_PROJECT"
    success "Service account created"
fi

# Step 6: Grant BigQuery permissions
info "Granting BigQuery permissions..."
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$GCP_PROJECT.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor" \
    --quiet

gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$GCP_PROJECT.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser" \
    --quiet
success "Permissions granted"

# Step 7: Create and download service account key
KEY_FILE="$HOME/lorchestra-events-key.json"
info "Creating service account key..."
if [ -f "$KEY_FILE" ]; then
    echo "  Key file already exists at $KEY_FILE"
    read -p "  Overwrite? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        info "Skipping key creation"
    else
        gcloud iam service-accounts keys create "$KEY_FILE" \
            --iam-account="$SERVICE_ACCOUNT_NAME@$GCP_PROJECT.iam.gserviceaccount.com"
        success "Service account key created: $KEY_FILE"
    fi
else
    gcloud iam service-accounts keys create "$KEY_FILE" \
        --iam-account="$SERVICE_ACCOUNT_NAME@$GCP_PROJECT.iam.gserviceaccount.com"
    success "Service account key created: $KEY_FILE"
fi

# Step 8: Verify tables exist
echo ""
info "Verifying table creation..."
bq show --schema "$GCP_PROJECT:$DATASET_NAME.event_log" > /dev/null
success "event_log table verified"
bq show --schema "$GCP_PROJECT:$DATASET_NAME.raw_objects" > /dev/null
success "raw_objects table verified"

echo ""
echo "=========================================="
echo "✓ BigQuery Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Add these environment variables to your .env file:"
echo ""
echo "   GOOGLE_APPLICATION_CREDENTIALS=$KEY_FILE"
echo "   EVENTS_BQ_DATASET=$DATASET_NAME"
echo "   EVENT_LOG_TABLE=event_log"
echo "   RAW_OBJECTS_TABLE=raw_objects"
echo "   GCP_PROJECT=$GCP_PROJECT"
echo ""
echo "2. Test credentials:"
echo ""
echo "   python3 -c 'from google.cloud import bigquery; client = bigquery.Client(); print(\"✓ Connected\")'"
echo ""
echo "3. Run your first ingestion job:"
echo ""
echo "   lorchestra run-job lorchestra gmail_ingest_acct1"
echo ""
