# Cloud Migration Guide

This guide explains how to translate the local lorch pipeline to Google Cloud Platform while preserving the architecture and design decisions.

## Migration Strategy

**Principle:** Each local component maps cleanly to a cloud service.

```
Local                          Cloud
─────────────────────────────────────────────────────────────
lorch orchestrator       →     Cloud Workflows / Cloud Composer
meltano CLI              →     Cloud Run Job (scheduled)
canonizer subprocess     →     Cloud Function (event-triggered)
vector-projector files   →     BigQuery table
/phi-data/ directories   →     GCS buckets
Subprocess calls         →     HTTP/PubSub messages
File I/O                 →     GCS client library
```

## Component Migration

### 1. Extract Stage: Meltano → Cloud Run Job

**Current (Local):**
```python
# lorch/stages/extract.py
def run_extract(config):
    subprocess.run([
        "/home/user/meltano-ingest/.venv/bin/meltano",
        "run",
        "ingest-all-accounts"
    ], cwd="/home/user/meltano-ingest")
```

**Migrated (Cloud):**

**Dockerfile** (`/home/user/meltano-ingest/Dockerfile`):
```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY . /app

RUN pip install meltano
RUN meltano install

# Use GCS for output instead of local filesystem
ENV MELTANO_ENVIRONMENT=prod

CMD ["meltano", "run", "ingest-all-accounts"]
```

**Deploy:**
```bash
# Build and push
gcloud builds submit --tag gcr.io/PROJECT_ID/meltano-ingest

# Create Cloud Run Job
gcloud run jobs create meltano-ingest \
  --image gcr.io/PROJECT_ID/meltano-ingest \
  --region us-central1 \
  --memory 4Gi \
  --cpu 2 \
  --max-retries 3 \
  --task-timeout 30m \
  --set-env-vars MELTANO_ENVIRONMENT=prod \
  --set-secrets=TAP_GMAIL_CLIENT_ID=gmail-client-id:latest,... \
  --execute-now
```

**Schedule:**
```bash
# Cloud Scheduler triggers Cloud Run Job daily
gcloud scheduler jobs create http meltano-daily-extract \
  --location us-central1 \
  --schedule "0 2 * * *" \
  --uri "https://run.googleapis.com/v1/namespaces/PROJECT_ID/jobs/meltano-ingest:run" \
  --http-method POST \
  --oauth-service-account-email PROJECT_ID@appspot.gserviceaccount.com
```

**meltano.yml changes:**
```yaml
environments:
  prod:
    config:
      plugins:
        loaders:
        - name: target-jsonl
          config:
            destination_path: gs://org1-raw-prod/extracts/  # ← GCS bucket
```

**Output:** `gs://org1-raw-prod/extracts/gmail-messages-acct1-20251112.jsonl`

---

### 2. Canonize Stage: Canonizer → Cloud Function

**Current (Local):**
```python
# lorch/stages/canonize.py
def run_canonize(input_file, transform_meta):
    subprocess.run([
        "/home/user/canonizer/.venv/bin/can",
        "transform", "run",
        "--meta", transform_meta,
        "--input", input_file,
        "--output", output_file
    ])
```

**Migrated (Cloud):**

**Cloud Function** (`functions/canonize/main.py`):
```python
import json
from google.cloud import storage
from canonizer.transform import run_transform

def canonize_on_upload(event, context):
    """Triggered when file uploaded to gs://org1-raw-prod/extracts/"""

    bucket_name = event['bucket']
    file_name = event['name']

    # Determine transform based on file pattern
    if 'gmail' in file_name:
        transform = 'email/gmail_to_canonical_v1'
    elif 'exchange' in file_name:
        transform = 'email/exchange_to_canonical_v1'
    elif 'contacts' in file_name:
        transform = 'contact/dataverse_contact_to_canonical_v1'
    else:
        print(f"No transform for {file_name}")
        return

    # Download raw file from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    raw_jsonl = blob.download_as_text()

    # Transform line by line
    canonical_lines = []
    for line in raw_jsonl.split('\n'):
        if not line.strip():
            continue

        raw_obj = json.loads(line)
        canonical_obj = run_transform(raw_obj, transform)
        canonical_lines.append(json.dumps(canonical_obj))

    # Upload canonical file to GCS
    canonical_blob = bucket.blob(f"canonical/{transform.split('/')[0]}-{file_name}")
    canonical_blob.upload_from_string('\n'.join(canonical_lines))

    print(f"Transformed {len(canonical_lines)} records")
```

**Deploy:**
```bash
gcloud functions deploy canonize-on-upload \
  --runtime python312 \
  --trigger-event google.storage.object.finalize \
  --trigger-resource gs://org1-raw-prod/extracts \
  --region us-central1 \
  --memory 2Gi \
  --timeout 540s \
  --set-env-vars TRANSFORM_REGISTRY_BUCKET=org1-transforms
```

**Transform Registry in GCS:**
```bash
# Upload transforms to GCS
gsutil -m cp -r /home/user/transforms/* gs://org1-transforms/
```

**Output:** `gs://org1-raw-prod/canonical/email-gmail-messages-acct1-20251112.jsonl`

---

### 3. Index Stage: Vector-projector → BigQuery

**Current (Local):**
```python
# lorch/stages/index.py (stub)
def run_index(canonical_dir, output_dir):
    # Copy files (placeholder)
    shutil.copytree(canonical_dir, output_dir)
```

**Migrated (Cloud):**

**Cloud Function** (`functions/index/main.py`):
```python
from google.cloud import bigquery

def index_to_bigquery(event, context):
    """Triggered when canonical file uploaded"""

    bucket_name = event['bucket']
    file_name = event['name']

    # Determine BigQuery table from file name
    if 'email' in file_name:
        table_id = 'PROJECT_ID.canonical.email'
    elif 'contact' in file_name:
        table_id = 'PROJECT_ID.canonical.contact'
    else:
        return

    # Load JSONL into BigQuery
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    )

    uri = f"gs://{bucket_name}/{file_name}"
    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # Wait for job to complete

    print(f"Loaded {load_job.output_rows} rows into {table_id}")
```

**Deploy:**
```bash
gcloud functions deploy index-to-bigquery \
  --runtime python312 \
  --trigger-event google.storage.object.finalize \
  --trigger-resource gs://org1-raw-prod/canonical \
  --region us-central1 \
  --memory 512Mi \
  --timeout 300s
```

**BigQuery Schema:**
```sql
-- Canonical email table
CREATE TABLE `PROJECT_ID.canonical.email` (
  message_id STRING NOT NULL,
  subject STRING,
  from_address STRING,
  to_addresses ARRAY<STRING>,
  sent_at TIMESTAMP,
  schema_version STRING,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  source_system STRING,
  source_record_id STRING
);

-- Canonical contact table
CREATE TABLE `PROJECT_ID.canonical.contact` (
  contact_id STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  schema_version STRING,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

**Output:** Rows in `PROJECT_ID.canonical.email` and `PROJECT_ID.canonical.contact`

---

### 4. Orchestrator: lorch → Cloud Workflows

**Current (Local):**
```python
# lorch/pipeline.py
def run_pipeline():
    extract_result = run_stage('extract')
    validate(extract_result)

    canonize_result = run_stage('canonize')
    validate(canonize_result)

    index_result = run_stage('index')
    validate(index_result)

    return PipelineResult(success=True)
```

**Migrated (Cloud):**

**Cloud Workflows YAML** (`workflows/phi-pipeline.yaml`):
```yaml
main:
  params: [input]
  steps:
    - extract:
        call: googleapis.run.v1.namespaces.jobs.run
        args:
          name: projects/PROJECT_ID/locations/us-central1/jobs/meltano-ingest
        result: extract_result

    - validate_extract:
        call: validate_stage
        args:
          result: ${extract_result}

    - wait_for_canonize:
        # Cloud Function is event-triggered, so just wait
        call: sys.sleep
        args:
          seconds: 300  # Wait 5min for canonization to complete

    - validate_canonize:
        call: check_gcs_files
        args:
          bucket: org1-raw-prod
          prefix: canonical/

    - wait_for_index:
        # Cloud Function is event-triggered, so just wait
        call: sys.sleep
        args:
          seconds: 60

    - validate_index:
        call: check_bigquery_rows
        args:
          table: PROJECT_ID.canonical.email

    - return_result:
        return: "Pipeline completed successfully"

# Subworkflows
validate_stage:
  params: [result]
  steps:
    - check_status:
        switch:
          - condition: ${result.status == "SUCCEEDED"}
            next: success
        next: failure
    - failure:
        raise: "Stage failed"
    - success:
        return: "Stage succeeded"

check_gcs_files:
  params: [bucket, prefix]
  steps:
    - list_files:
        call: googleapis.storage.v1.objects.list
        args:
          bucket: ${bucket}
          prefix: ${prefix}
        result: files
    - validate:
        switch:
          - condition: ${len(files.items) > 0}
            return: "Files found"
        raise: "No files found"

check_bigquery_rows:
  params: [table]
  steps:
    - query:
        call: googleapis.bigquery.v2.jobs.query
        args:
          projectId: PROJECT_ID
          body:
            query: ${"SELECT COUNT(*) as count FROM `" + table + "`"}
        result: query_result
    - validate:
        switch:
          - condition: ${query_result.rows[0].f[0].v > 0}
            return: "Rows found"
        raise: "No rows in table"
```

**Deploy:**
```bash
gcloud workflows deploy phi-pipeline \
  --source workflows/phi-pipeline.yaml \
  --location us-central1
```

**Schedule:**
```bash
gcloud scheduler jobs create http trigger-phi-pipeline \
  --location us-central1 \
  --schedule "0 3 * * *" \  # 3am daily
  --uri "https://workflowexecutions.googleapis.com/v1/projects/PROJECT_ID/locations/us-central1/workflows/phi-pipeline/executions" \
  --http-method POST \
  --oauth-service-account-email PROJECT_ID@appspot.gserviceaccount.com
```

---

## Configuration Migration

### Local Config → Cloud Config

**Local (`config/pipeline.yaml`):**
```yaml
stages:
  extract:
    repo_path: /home/user/meltano-ingest
    output_dir: /home/user/phi-data/meltano-extracts
```

**Cloud (Environment Variables + Secrets Manager):**
```bash
# Meltano Cloud Run Job
gcloud run jobs update meltano-ingest \
  --set-env-vars MELTANO_ENVIRONMENT=prod \
  --set-secrets TAP_GMAIL_CLIENT_ID=gmail-client-id:latest
```

**Secrets in GCP Secrets Manager:**
```bash
echo -n "YOUR_CLIENT_ID" | gcloud secrets create gmail-client-id --data-file=-
echo -n "YOUR_CLIENT_SECRET" | gcloud secrets create gmail-client-secret --data-file=-
echo -n "YOUR_REFRESH_TOKEN" | gcloud secrets create gmail-refresh-token --data-file=-
```

---

## Data Flow Comparison

### Local Flow
```
meltano → /home/user/phi-data/meltano-extracts/*.jsonl
          ↓
canonizer → /home/user/phi-data/canonical/*.jsonl
          ↓
vector-projector → /home/user/phi-data/vector-store/
```

### Cloud Flow
```
meltano (Cloud Run Job) → gs://org1-raw-prod/extracts/*.jsonl
                          ↓ (GCS trigger)
canonizer (Cloud Function) → gs://org1-raw-prod/canonical/*.jsonl
                             ↓ (GCS trigger)
index (Cloud Function) → BigQuery tables
```

---

## Cost Estimates (Monthly)

| Component | Service | Cost |
|-----------|---------|------|
| Extract (1x/day, 5min) | Cloud Run Job | $5 |
| Canonize (8 files/day) | Cloud Functions | $10 |
| Index (8 files/day) | Cloud Functions | $5 |
| Storage (raw + canonical) | GCS Standard | $20 |
| BigQuery storage (1TB) | BigQuery | $20 |
| BigQuery queries | BigQuery | $50 (usage-based) |
| Orchestration | Cloud Workflows | $1 |
| Secrets | Secrets Manager | $1 |
| **Total** | | **~$112/month** |

Compare to local: Cloud Workstation (n1-standard-4) = $180/month

---

## Security Changes

### Local Security
- 700 permissions on directories
- 600 permissions on files
- Encrypted filesystem (LUKS)
- No network exposure

### Cloud Security
- **IAM**: Service accounts with minimal permissions
- **VPC**: Private Google Access (no public IPs)
- **Encryption**: GCS/BigQuery server-side encryption
- **Secrets**: Secrets Manager (not in code/env vars)
- **Audit Logs**: All access logged to Cloud Logging
- **VPC-SC**: Perimeter around resources

**Service Account Example:**
```bash
# Create service account for Meltano
gcloud iam service-accounts create meltano-ingest

# Grant minimal permissions
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member serviceAccount:meltano-ingest@PROJECT_ID.iam.gserviceaccount.com \
  --role roles/storage.objectCreator \  # Write to GCS only
  --condition="resource.name.startsWith('projects/_/buckets/org1-raw-prod/objects/extracts/')"
```

---

## Testing Cloud Pipeline

### 1. Test Extract Stage

```bash
# Trigger Cloud Run Job manually
gcloud run jobs execute meltano-ingest --region us-central1

# Check output
gsutil ls gs://org1-raw-prod/extracts/
```

### 2. Test Canonize Stage

```bash
# Upload test file to trigger function
gsutil cp test/gmail-messages.jsonl gs://org1-raw-prod/extracts/

# Check Cloud Function logs
gcloud functions logs read canonize-on-upload --region us-central1 --limit 50

# Verify output
gsutil ls gs://org1-raw-prod/canonical/
```

### 3. Test Index Stage

```bash
# Upload canonical file to trigger function
gsutil cp test/email-canonical.jsonl gs://org1-raw-prod/canonical/

# Check BigQuery
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `PROJECT_ID.canonical.email`'
```

### 4. Test Full Workflow

```bash
# Trigger workflow manually
gcloud workflows run phi-pipeline --location us-central1

# Watch execution
gcloud workflows executions describe EXECUTION_ID \
  --workflow phi-pipeline --location us-central1
```

---

## Migration Checklist

### Pre-Migration
- [ ] Create GCP project
- [ ] Enable APIs (Cloud Run, Cloud Functions, BigQuery, Cloud Workflows)
- [ ] Create GCS buckets (org1-raw-prod, org1-transforms)
- [ ] Create BigQuery dataset and tables
- [ ] Set up service accounts and IAM
- [ ] Migrate secrets to Secrets Manager
- [ ] Upload transforms to GCS

### Migration
- [ ] Containerize meltano-ingest
- [ ] Deploy meltano Cloud Run Job
- [ ] Test extract stage
- [ ] Deploy canonize Cloud Function
- [ ] Test canonize stage
- [ ] Deploy index Cloud Function
- [ ] Test index stage
- [ ] Deploy Cloud Workflow
- [ ] Test full pipeline

### Post-Migration
- [ ] Set up Cloud Scheduler for daily runs
- [ ] Configure alerting (Cloud Monitoring)
- [ ] Set up log analysis (Cloud Logging)
- [ ] Document runbooks
- [ ] Train team on cloud tools
- [ ] Decommission local pipeline

---

## Rollback Plan

If cloud migration fails, local pipeline remains functional:

1. Update lorch config to point to local paths
2. Restart lorch on local workstation
3. Copy any cloud data back to local: `gsutil -m cp -r gs://org1-raw-prod/ /home/user/phi-data/`

---

## Additional Resources

- [Cloud Run Jobs docs](https://cloud.google.com/run/docs/create-jobs)
- [Cloud Functions event triggers](https://cloud.google.com/functions/docs/calling/storage)
- [Cloud Workflows syntax](https://cloud.google.com/workflows/docs/reference/syntax)
- [BigQuery loading data](https://cloud.google.com/bigquery/docs/loading-data)
- [GCS IAM permissions](https://cloud.google.com/storage/docs/access-control/iam-permissions)
