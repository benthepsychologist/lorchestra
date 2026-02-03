#!/usr/bin/env python3
"""Equivalence smoke test: run real V1 + V2 jobs against live data, diff BQ output.

Runs every ingest or canonize job through BOTH paths, writing to isolated smoke
datasets.  Then queries both datasets and diffs the rows.

  V1 writes to: smoke_equiv_v1  (tables prefixed smoke_equiv_v1__)
  V2 writes to: smoke_equiv_v2  (tables prefixed smoke_equiv_v2__)

Usage:
    # All ingest jobs:
    python scripts/smoke_equiv.py ingest

    # All canonize jobs:
    python scripts/smoke_equiv.py canonize

    # Single job:
    python scripts/smoke_equiv.py ingest --job ingest_stripe_customers

    # Keep smoke datasets after run (default: clean up):
    python scripts/smoke_equiv.py ingest --keep

Requires: GCP credentials, config.yaml, canonizer registry.
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Storacle needs this env var for BQ operations
os.environ.setdefault("STORACLE_NAMESPACE_SALT", "storacle-dev")

logger = logging.getLogger(__name__)

DEFINITIONS_DIR = Path(__file__).resolve().parent.parent / "lorchestra" / "jobs" / "definitions"
ARTIFACTS_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "equiv"

NS_V1 = "equiv_v1"
NS_V2 = "equiv_v2"

TIMESTAMP_COLS = {"canonicalized_at", "created_at", "first_seen", "last_seen"}
# Columns that are structurally different between V1/V2 (different ID formats)
IGNORE_COLS = {"correlation_id"}
# Payload keys with ephemeral/session-specific values (e.g. Stripe hosted URLs, Gmail attachment IDs)
VOLATILE_PAYLOAD_KEYS = {"hosted_invoice_url", "invoice_pdf", "attachmentId"}

# Tables we need in each smoke dataset (V1 writes event_log + target table)
SMOKE_TABLES = {
    "ingest": ["event_log", "raw_objects"],
    "canonize": ["event_log", "raw_objects", "canonical_objects"],
}

# Source datasets for each table (to copy schema from)
TABLE_SOURCE_DATASET = {
    "event_log": "dataset_raw",
    "raw_objects": "dataset_raw",
    "canonical_objects": "dataset_canonical",
}


def ensure_smoke_tables(bq_client, project: str, config, job_type: str) -> None:
    """Create smoke datasets and copy table schemas from production."""
    from google.cloud import bigquery

    tables_needed = SMOKE_TABLES.get(job_type, [])
    created_any = False

    for ns in [NS_V1, NS_V2]:
        smoke_dataset = f"smoke_{ns}"
        dataset_ref = bigquery.DatasetReference(project, smoke_dataset)
        try:
            bq_client.get_dataset(dataset_ref)
        except Exception:
            ds = bigquery.Dataset(dataset_ref)
            ds.location = "US"
            bq_client.create_dataset(ds, exists_ok=True)
            print(f"  Created dataset: {smoke_dataset}")

        for table_base in tables_needed:
            smoke_table = f"smoke_{ns}__{table_base}"
            dest = f"{project}.{smoke_dataset}.{smoke_table}"
            try:
                bq_client.get_table(dest)
                continue  # Already exists
            except Exception:
                pass

            # Copy schema from production table
            source_dataset_attr = TABLE_SOURCE_DATASET.get(table_base, "dataset_raw")
            source_dataset = getattr(config, source_dataset_attr)
            source = f"{project}.{source_dataset}.{table_base}"
            try:
                source_table = bq_client.get_table(source)
                new_table = bigquery.Table(dest, schema=source_table.schema)
                bq_client.create_table(new_table)
                created_any = True
                print(f"  Created table: {smoke_dataset}.{smoke_table}")
            except Exception as e:
                print(f"  WARNING: Could not create {dest}: {e}")

    if created_any:
        # BQ streaming insert needs tables to be fully propagated
        print("  Waiting for table propagation...", end="", flush=True)
        time.sleep(10)
        print(" done")


# ============================================================================
# Run V1 (legacy job_runner path)
# ============================================================================

DEFAULT_LIMIT = 10


def run_v1(job_id: str, limit: int = DEFAULT_LIMIT) -> None:
    """Run a V1 job writing to smoke_equiv_v1 dataset."""
    from lorchestra.config import load_config
    from lorchestra.job_runner import run_job
    from google.cloud import bigquery

    config = load_config()
    bq_client = bigquery.Client(project=config.project)

    run_job(
        job_id,
        config=config,
        dry_run=False,
        test_table=False,
        smoke_namespace=NS_V1,
        definitions_dir=DEFINITIONS_DIR,
        bq_client=bq_client,
        limit=limit,
    )


# ============================================================================
# Run V2 (executor path)
# ============================================================================

def run_v2(job_id: str, limit: int = DEFAULT_LIMIT) -> None:
    """Run a V2 job writing to smoke_equiv_v2 dataset."""
    from lorchestra.executor import execute
    from lorchestra.run_store import InMemoryRunStore

    envelope = {
        "job_id": job_id,
        "definitions_dir": DEFINITIONS_DIR,
        "store": InMemoryRunStore(),
        "smoke_namespace": NS_V2,
        "limit": limit,
    }

    result = execute(envelope)

    if not result.success:
        errors = [
            o.error for o in result.attempt.step_outcomes if o.error
        ]
        raise RuntimeError(f"V2 execution failed: {errors}")


# ============================================================================
# Query + Compare
# ============================================================================

def query_smoke_table(bq_client, project: str, namespace: str, table_base: str) -> list[dict]:
    """Query all rows from a smoke table."""
    dataset = f"smoke_{namespace}"
    table = f"smoke_{namespace}__{table_base}"
    table_ref = f"`{project}.{dataset}.{table}`"

    # Check if table exists
    try:
        bq_client.get_table(f"{project}.{dataset}.{table}")
    except Exception:
        return []

    sql = f"SELECT * FROM {table_ref} ORDER BY idem_key"
    rows = []
    for row in bq_client.query(sql).result():
        d = dict(row)
        # Normalize: convert datetime objects to ISO strings
        for k, v in d.items():
            if hasattr(v, 'isoformat'):
                d[k] = v.isoformat()
            elif isinstance(v, bytes):
                d[k] = v.decode('utf-8')
        rows.append(d)
    return rows


def _strip_volatile(obj):
    """Recursively remove volatile keys and normalize numbers in a parsed JSON object."""
    if isinstance(obj, dict):
        return {k: _strip_volatile(v) for k, v in obj.items() if k not in VOLATILE_PAYLOAD_KEYS}
    elif isinstance(obj, list):
        return [_strip_volatile(v) for v in obj]
    elif isinstance(obj, float) and obj.is_integer():
        # Normalize 250.0 -> 250 for int/float equivalence
        return int(obj)
    return obj


def normalize_row(row: dict) -> dict:
    """Normalize a row for comparison: sort payload, blank timestamps."""
    out = {}
    for k, v in sorted(row.items()):
        if k in TIMESTAMP_COLS:
            out[k] = "__TS__"
        elif k in IGNORE_COLS:
            out[k] = "__IGNORED__"
        elif isinstance(v, dict):
            out[k] = json.dumps(_strip_volatile(v), sort_keys=True, default=str)
        elif isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, (dict, list)):
                    out[k] = json.dumps(_strip_volatile(parsed), sort_keys=True, default=str)
                else:
                    out[k] = v
            except (json.JSONDecodeError, TypeError):
                out[k] = v
        else:
            out[k] = str(v) if v is not None else v
    return out


def diff_rows(v1_rows: list[dict], v2_rows: list[dict]) -> list[str]:
    """Diff two lists of rows by idem_key. Returns human-readable diffs."""
    diffs = []

    if len(v1_rows) != len(v2_rows):
        diffs.append(f"Row count: V1={len(v1_rows)} vs V2={len(v2_rows)}")

    v1_by_key = {r.get("idem_key", f"?{i}"): r for i, r in enumerate(v1_rows)}
    v2_by_key = {r.get("idem_key", f"?{i}"): r for i, r in enumerate(v2_rows)}

    all_keys = sorted(set(v1_by_key.keys()) | set(v2_by_key.keys()))

    for key in all_keys:
        if key not in v1_by_key:
            diffs.append(f"V2-only: {key}")
            continue
        if key not in v2_by_key:
            diffs.append(f"V1-only: {key}")
            continue

        n1 = normalize_row(v1_by_key[key])
        n2 = normalize_row(v2_by_key[key])

        if n1 != n2:
            for col in sorted(set(n1.keys()) | set(n2.keys())):
                v1_val = n1.get(col, "__MISSING__")
                v2_val = n2.get(col, "__MISSING__")
                if v1_val != v2_val:
                    diffs.append(
                        f"  {key}: {col}\n"
                        f"    V1: {str(v1_val)[:200]}\n"
                        f"    V2: {str(v2_val)[:200]}"
                    )

    return diffs


def truncate_smoke_table(bq_client, project: str, namespace: str, table_base: str) -> None:
    """Delete all rows from a smoke table (so we get clean per-job results)."""
    dataset = f"smoke_{namespace}"
    table = f"smoke_{namespace}__{table_base}"
    try:
        bq_client.get_table(f"{project}.{dataset}.{table}")
        bq_client.query(f"DELETE FROM `{project}.{dataset}.{table}` WHERE TRUE").result()
    except Exception:
        pass  # Table doesn't exist yet, fine


def cleanup_smoke_datasets(bq_client, project: str) -> None:
    """Delete both smoke datasets."""
    for ns in [NS_V1, NS_V2]:
        dataset = f"smoke_{ns}"
        try:
            bq_client.delete_dataset(
                f"{project}.{dataset}",
                delete_contents=True,
                not_found_ok=True,
            )
            print(f"  Cleaned up {dataset}")
        except Exception as e:
            print(f"  Failed to clean up {dataset}: {e}")


# ============================================================================
# Main
# ============================================================================

def list_jobs(job_type: str) -> list[str]:
    """List available V2 YAML jobs for a given type."""
    yaml_dir = DEFINITIONS_DIR / job_type
    if not yaml_dir.exists():
        raise ValueError(f"No definitions directory for: {job_type}")
    return sorted(p.stem for p in yaml_dir.glob("*.yaml"))


def table_for_job_type(job_type: str) -> str:
    """Return the BQ table base name for a job type."""
    if job_type == "ingest":
        return "raw_objects"
    elif job_type == "canonize":
        return "canonical_objects"
    else:
        raise ValueError(f"Unknown job type: {job_type}")


def run_one(job_type: str, job_id: str, bq_client, project: str, dump: bool = False) -> bool:
    """Run equivalence for one job. Returns True if equivalent."""
    table_base = table_for_job_type(job_type)

    print(f"\n{'=' * 60}")
    print(f"  {job_id}")
    print(f"{'=' * 60}")

    # Truncate smoke tables so this run starts clean
    for ns in [NS_V1, NS_V2]:
        truncate_smoke_table(bq_client, project, ns, table_base)

    # Run V1
    print(f"  V1 running...", end="", flush=True)
    try:
        run_v1(job_id)
        print(f" done", flush=True)
    except Exception as e:
        print(f" ERROR: {e}")
        import traceback; traceback.print_exc()
        return False

    # Run V2
    print(f"  V2 running...", end="", flush=True)
    try:
        run_v2(job_id)
        print(f" done", flush=True)
    except Exception as e:
        print(f" ERROR: {e}")
        import traceback; traceback.print_exc()
        return False

    # Query results
    print(f"  Querying results...", end="", flush=True)
    v1_rows = query_smoke_table(bq_client, project, NS_V1, table_base)
    v2_rows = query_smoke_table(bq_client, project, NS_V2, table_base)
    print(f" V1={len(v1_rows)}, V2={len(v2_rows)}")

    # Compare
    diffs = diff_rows(v1_rows, v2_rows)

    if diffs:
        print(f"  DIFFERENCES ({len(diffs)}):")
        for d in diffs[:20]:
            print(f"    {d}")
        if len(diffs) > 20:
            print(f"    ... and {len(diffs) - 20} more")
    else:
        print(f"  EQUIVALENT ({len(v1_rows)} rows)")

    # Dump JSONL
    if dump:
        ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
        for label, rows in [("v1", v1_rows), ("v2", v2_rows)]:
            path = ARTIFACTS_DIR / f"{job_id}.{label}.jsonl"
            sorted_rows = sorted(rows, key=lambda r: r.get("idem_key", ""))
            with open(path, "w") as f:
                for row in sorted_rows:
                    f.write(json.dumps(normalize_row(row), sort_keys=True, default=str) + "\n")
            print(f"  Dumped: {path}")

    return len(diffs) == 0


def main():
    parser = argparse.ArgumentParser(description="V1 vs V2 equivalence smoke test")
    parser.add_argument("job_type", choices=["ingest", "canonize"], help="Job type to test")
    parser.add_argument("--job", help="Single job ID (default: all)")
    parser.add_argument("--dump", action="store_true", help="Dump JSONL files to artifacts/equiv/")
    parser.add_argument("--keep", action="store_true", help="Keep smoke datasets after run")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

    from lorchestra.config import load_config
    from google.cloud import bigquery

    config = load_config()
    bq_client = bigquery.Client(project=config.project)
    project = config.project

    if args.job:
        job_ids = [args.job]
    else:
        job_ids = list_jobs(args.job_type)

    print(f"Running {len(job_ids)} {args.job_type} job(s)")
    print(f"  V1 -> smoke_{NS_V1}")
    print(f"  V2 -> smoke_{NS_V2}")

    # Ensure smoke datasets and tables exist
    ensure_smoke_tables(bq_client, project, config, args.job_type)

    results = {}
    for job_id in job_ids:
        try:
            passed = run_one(args.job_type, job_id, bq_client, project, dump=args.dump)
            results[job_id] = passed
        except Exception as e:
            print(f"  CRASHED: {e}")
            import traceback; traceback.print_exc()
            results[job_id] = False

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  SUMMARY -- {args.job_type}")
    print(f"{'=' * 60}")
    for job_id, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {status}  {job_id}")

    total = len(results)
    passed_count = sum(1 for v in results.values() if v)
    print(f"\n  {passed_count}/{total} equivalent")

    # Cleanup
    if not args.keep:
        print(f"\nCleaning up smoke datasets...")
        cleanup_smoke_datasets(bq_client, project)

    if passed_count < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
