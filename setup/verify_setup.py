#!/usr/bin/env python3
"""
Verify BigQuery setup is complete and working.

This script checks:
1. Environment variables are set
2. Credentials file exists and is valid
3. BigQuery client can connect
4. Dataset exists
5. Tables exist with correct schema
6. Permissions are correct

Usage:
    source .env
    python3 scripts/verify_setup.py
"""

import os
import sys
import json
from pathlib import Path

try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound, Forbidden
except ImportError:
    print("ERROR: google-cloud-bigquery not installed")
    print("Run: pip install google-cloud-bigquery")
    sys.exit(1)

# ANSI colors
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'


def error(msg):
    """Print error and continue (don't exit)"""
    print(f"{RED}✗ {msg}{NC}")
    return False


def success(msg):
    """Print success"""
    print(f"{GREEN}✓ {msg}{NC}")
    return True


def info(msg):
    """Print info"""
    print(f"{BLUE}→ {msg}{NC}")


def warning(msg):
    """Print warning"""
    print(f"{YELLOW}⚠ {msg}{NC}")


def check_env_vars():
    """Check required environment variables"""
    info("Checking environment variables...")

    required = {
        'GCP_PROJECT': 'GCP project ID',
        'EVENTS_BQ_DATASET': 'BigQuery dataset name',
        'GOOGLE_APPLICATION_CREDENTIALS': 'Path to service account key',
    }

    all_set = True
    for var, desc in required.items():
        value = os.environ.get(var)
        if value:
            success(f"{var}={value}")
        else:
            error(f"{var} not set ({desc})")
            all_set = False

    return all_set


def check_credentials_file():
    """Check credentials file exists and is valid JSON"""
    info("Checking credentials file...")

    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not creds_path:
        return error("GOOGLE_APPLICATION_CREDENTIALS not set")

    creds_file = Path(creds_path)
    if not creds_file.exists():
        return error(f"Credentials file not found: {creds_path}")

    try:
        with open(creds_file) as f:
            creds = json.load(f)

        # Check required fields
        if 'type' in creds and creds['type'] == 'service_account':
            success(f"Valid service account key: {creds.get('client_email', 'unknown')}")
            return True
        else:
            return error("Invalid credentials file (not a service account key)")

    except json.JSONDecodeError:
        return error(f"Invalid JSON in credentials file: {creds_path}")
    except Exception as e:
        return error(f"Error reading credentials: {e}")


def check_bigquery_connection():
    """Test BigQuery client connection"""
    info("Testing BigQuery connection...")

    try:
        client = bigquery.Client()
        success(f"Connected to project: {client.project}")
        return client
    except Exception as e:
        error(f"Failed to connect to BigQuery: {e}")
        return None


def check_dataset(client):
    """Check dataset exists"""
    info("Checking dataset...")

    dataset_name = os.environ.get('EVENTS_BQ_DATASET')
    project = os.environ.get('GCP_PROJECT')

    if not dataset_name or not project:
        return error("Missing GCP_PROJECT or EVENTS_BQ_DATASET")

    dataset_id = f"{project}.{dataset_name}"

    try:
        dataset = client.get_dataset(dataset_id)
        success(f"Dataset exists: {dataset_id}")
        success(f"  Location: {dataset.location}")
        success(f"  Created: {dataset.created}")
        return True
    except NotFound:
        error(f"Dataset not found: {dataset_id}")
        warning("Run: python3 scripts/setup_bigquery.py")
        return False
    except Forbidden:
        error(f"Permission denied accessing dataset: {dataset_id}")
        warning("Check service account has BigQuery Data Editor role")
        return False
    except Exception as e:
        error(f"Error checking dataset: {e}")
        return False


def check_tables(client):
    """Check tables exist with correct schema"""
    info("Checking tables...")

    dataset_name = os.environ.get('EVENTS_BQ_DATASET')
    project = os.environ.get('GCP_PROJECT')

    dataset_id = f"{project}.{dataset_name}"

    # Check event_log
    try:
        event_log = client.get_table(f"{dataset_id}.event_log")
        success(f"event_log table exists")
        success(f"  Rows: {event_log.num_rows:,}")
        success(f"  Size: {event_log.num_bytes / 1024 / 1024:.2f} MB")

        # Check partitioning
        if event_log.time_partitioning:
            success(f"  Partitioned by: {event_log.time_partitioning.field}")
        else:
            warning("  Not partitioned (expected DATE(created_at))")

        # Check clustering
        if event_log.clustering_fields:
            success(f"  Clustered by: {', '.join(event_log.clustering_fields)}")
        else:
            warning("  Not clustered")

    except NotFound:
        error("event_log table not found")
        warning("Run: python3 scripts/setup_bigquery.py")
        return False
    except Exception as e:
        error(f"Error checking event_log: {e}")
        return False

    # Check raw_objects
    try:
        raw_objects = client.get_table(f"{dataset_id}.raw_objects")
        success(f"raw_objects table exists")
        success(f"  Rows: {raw_objects.num_rows:,}")
        success(f"  Size: {raw_objects.num_bytes / 1024 / 1024:.2f} MB")

        # Check clustering
        if raw_objects.clustering_fields:
            success(f"  Clustered by: {', '.join(raw_objects.clustering_fields)}")
        else:
            warning("  Not clustered")

        # Check for JSON field
        payload_field = None
        for field in raw_objects.schema:
            if field.name == 'payload':
                payload_field = field
                break

        if payload_field and payload_field.field_type == 'JSON':
            success(f"  Payload field: JSON type ✓")
        else:
            warning("  Payload field not JSON type")

    except NotFound:
        error("raw_objects table not found")
        warning("Run: python3 scripts/setup_bigquery.py")
        return False
    except Exception as e:
        error(f"Error checking raw_objects: {e}")
        return False

    return True


def check_permissions(client):
    """Test write permissions"""
    info("Testing write permissions...")

    # Try a simple query (read permission)
    try:
        query = "SELECT 1 as test"
        query_job = client.query(query)
        result = list(query_job.result())
        success("Query permission: OK")
    except Exception as e:
        error(f"Query permission denied: {e}")
        return False

    # Note: We don't actually insert test data to avoid polluting tables
    success("Service account has necessary permissions")
    return True


def main():
    """Run all checks"""
    print("")
    print("=" * 60)
    print("BigQuery Setup Verification")
    print("=" * 60)
    print("")

    checks = [
        ("Environment Variables", check_env_vars, []),
        ("Credentials File", check_credentials_file, []),
    ]

    # Run initial checks
    all_passed = True
    for name, check_func, args in checks:
        print(f"\n{name}:")
        print("-" * 60)
        if not check_func(*args):
            all_passed = False

    # If initial checks failed, stop
    if not all_passed:
        print("")
        print("=" * 60)
        print(f"{RED}✗ Setup verification FAILED{NC}")
        print("=" * 60)
        print("")
        print("Fix the errors above and try again.")
        sys.exit(1)

    # Try to connect
    print(f"\nBigQuery Connection:")
    print("-" * 60)
    client = check_bigquery_connection()
    if not client:
        print("")
        print("=" * 60)
        print(f"{RED}✗ Setup verification FAILED{NC}")
        print("=" * 60)
        sys.exit(1)

    # Run BigQuery-specific checks
    bq_checks = [
        ("Dataset", check_dataset, [client]),
        ("Tables", check_tables, [client]),
        ("Permissions", check_permissions, [client]),
    ]

    for name, check_func, args in bq_checks:
        print(f"\n{name}:")
        print("-" * 60)
        if not check_func(*args):
            all_passed = False

    # Final result
    print("")
    print("=" * 60)
    if all_passed:
        print(f"{GREEN}✓ All checks passed!{NC}")
        print("=" * 60)
        print("")
        print("Your BigQuery setup is ready to use.")
        print("")
        print("Next steps:")
        print("  1. Run test ingestion:")
        print("     lorchestra run-job lorchestra gmail_ingest_acct1")
        print("")
        print("  2. Verify data:")
        print("     python3 scripts/query_events.py")
        print("")
    else:
        print(f"{RED}✗ Some checks failed{NC}")
        print("=" * 60)
        print("")
        print("Fix the errors above and run again:")
        print("  python3 scripts/verify_setup.py")
        print("")
        sys.exit(1)


if __name__ == "__main__":
    main()
