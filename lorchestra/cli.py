"""
CLI interface for lorchestra job orchestrator.

Provides commands to discover, inspect, and run jobs.

Jobs are defined as JSON files in jobs/definitions/**/*.json (organized by
processor type in subdirectories) and dispatched via the JobRunner to typed
processors (ingest, canonize, formation, projection).
"""

from __future__ import annotations

import sys

import click
from pathlib import Path

from lorchestra import __version__


# Job definitions directory
DEFINITIONS_DIR = Path(__file__).parent / "jobs" / "definitions"

# Queries directory for named SQL queries
QUERIES_DIR = Path(__file__).parent / "queries"


def _get_available_jobs() -> list[str]:
    """Get list of available job IDs from all subdirectories (excluding _deprecated)."""
    if not DEFINITIONS_DIR.exists():
        return []
    # Search root and all subdirectories, excluding _deprecated
    return sorted([
        f.stem for f in DEFINITIONS_DIR.glob("**/*.json")
        if "_deprecated" not in str(f)
    ])


def _cleanup_smoke_dataset(bq_client, project: str, dataset: str) -> bool:
    """Delete a smoke dataset with guarded safety check.

    Args:
        bq_client: BigQuery client instance.
        project: GCP project ID.
        dataset: Dataset name to delete.

    Returns:
        True if deleted, False if not found.

    Raises:
        ValueError: If dataset doesn't match smoke_* pattern.
    """
    from google.cloud.exceptions import NotFound

    # Safety guard: only delete smoke_* datasets
    if not dataset.startswith("smoke_"):
        raise ValueError(
            f"Refusing to delete dataset '{dataset}': "
            f"only smoke_* datasets can be deleted"
        )

    dataset_ref = f"{project}.{dataset}"
    try:
        bq_client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=False)
        return True
    except NotFound:
        return False


@click.group()
@click.version_option(version=__version__, prog_name="lorchestra")
@click.pass_context
def main(ctx):
    """
    lorchestra - Lightweight job orchestrator.

    Run jobs defined as JSON specs via typed processors.
    """
    from lorchestra.config import load_config

    ctx.ensure_object(dict)
    try:
        ctx.obj["config"] = load_config()
    except Exception as e:
        # Fallback for init command or when config is missing/invalid
        # Just log a warning or silently ignore if we assume init command will handle it
        # But for 'run' command it will fail later if config is missing.
        # Let's effectively silence it here but allow commands to check ctx.obj.get("config")
        ctx.obj["config_error"] = str(e)



def _run_job_impl(
    ctx,
    job: str,
    dry_run: bool = False,
    test_table: bool = False,
    smoke_namespace: str | None = None,
    clean_up: bool = False,
    **kwargs,
):
    """Run a job by ID using the JobRunner."""
    import os
    from google.cloud import bigquery
    from lorchestra.job_runner import run_job
    from lorchestra.stack_clients.event_client import ensure_test_tables_exist

    # Check config
    if "config" not in ctx.obj:
        click.echo(f"✗ Config not loaded: {ctx.obj.get('config_error', 'Unknown error')}", err=True)
        click.echo("Run 'lorchestra init' to create a configuration file.", err=True)
        raise SystemExit(1)

    config = ctx.obj["config"]

    # Validate mutually exclusive flags
    if dry_run and test_table:
        raise click.UsageError("--dry-run and --test-table are mutually exclusive")
    if smoke_namespace and test_table:
        raise click.UsageError("--smoke-namespace and --test-table are mutually exclusive")
    if dry_run and smoke_namespace:
        raise click.UsageError("--dry-run and --smoke-namespace are mutually exclusive")
    if clean_up and not smoke_namespace:
        raise click.UsageError("--clean-up requires --smoke-namespace")

    # Print mode banner
    if dry_run:
        click.echo("=" * 50)
        click.echo("=== DRY RUN MODE === (no BigQuery writes)")
        click.echo("=" * 50)
    elif test_table:
        click.echo("=" * 50)
        click.echo("=== TEST TABLE MODE ===")
        click.echo("Writing to: test_event_log, test_raw_objects")
        click.echo("=" * 50)
    elif smoke_namespace:
        smoke_dataset = f"smoke_{smoke_namespace}"
        smoke_prefix = f"smoke_{smoke_namespace}__"
        click.echo("=" * 50)
        click.echo("=== SMOKE TEST MODE ===")
        click.echo(f"Dataset:      {smoke_dataset}")
        click.echo(f"Table prefix: {smoke_prefix}")
        if clean_up:
            click.echo("Cleanup:      enabled (will delete dataset after run)")
        click.echo("=" * 50)
        # Set env var for storacle to pick up
        os.environ["STORACLE_SMOKE_NAMESPACE"] = smoke_namespace

    # Check if job definition exists
    available_jobs = _get_available_jobs()

    # Handle job_<name> -> <name> aliasing for backwards compat
    job_id = job
    if job_id.startswith("job_"):
        job_id = job_id[4:]  # Remove "job_" prefix

    if job_id not in available_jobs:
        click.echo(f"✗ Unknown job: {job}", err=True)
        click.echo("\nAvailable jobs:", err=True)
        for jid in available_jobs:
            click.echo(f"  {jid}", err=True)
        raise SystemExit(1)

    # Create BQ client
    bq_client = bigquery.Client()

    # Ensure test tables exist (only for test-table mode)
    if test_table:
        ensure_test_tables_exist(bq_client, dataset=config.dataset_raw)

    # Run job via JobRunner
    job_failed = False
    try:
        run_job(
            job_id,
            config=config,
            dry_run=dry_run,
            test_table=test_table,
            smoke_namespace=smoke_namespace,
            definitions_dir=DEFINITIONS_DIR,
            bq_client=bq_client,
        )

        # Success message
        if dry_run:
            click.echo(f"\n[DRY-RUN] {job_id} completed (no writes)")
        elif test_table:
            click.echo(f"\n[TEST] {job_id} completed (wrote to test tables)")
        elif smoke_namespace:
            click.echo(f"\n[SMOKE] {job_id} completed (wrote to smoke_{smoke_namespace})")
        else:
            click.echo(f"✓ {job_id} completed")

    except Exception as e:
        job_failed = True
        click.echo(f"✗ {job_id} failed: {e}", err=True)
    finally:
        # Cleanup smoke dataset if requested (on success or failure)
        if clean_up and smoke_namespace:
            smoke_dataset = f"smoke_{smoke_namespace}"
            click.echo(f"\nCleaning up smoke dataset: {smoke_dataset}")
            try:
                _cleanup_smoke_dataset(bq_client, config.project, smoke_dataset)
                click.echo(f"✓ Deleted dataset {smoke_dataset}")
            except Exception as cleanup_err:
                click.echo(f"⚠ Cleanup failed: {cleanup_err}", err=True)

    if job_failed:
        raise SystemExit(1)


@main.command("run")
@click.argument("job")
@click.option("--dry-run", is_flag=True, help="Execute without writing to BigQuery")
@click.option("--test-table", is_flag=True, help="Write to test tables instead of prod")
@click.option(
    "--smoke-namespace",
    type=str,
    default=None,
    help="Smoke test namespace (creates isolated dataset smoke_<namespace> with prefixed tables)",
)
@click.option(
    "--clean-up",
    is_flag=True,
    help="Delete smoke dataset after run (requires --smoke-namespace)",
)
@click.pass_context
def run(
    ctx,
    job: str,
    dry_run: bool,
    test_table: bool,
    smoke_namespace: str | None,
    clean_up: bool,
):
    """
    Run a job by ID.

    JOB is the job ID (filename without .json extension).

    Examples:

        lorchestra run ingest_gmail_acct1

        lorchestra run validate_gmail_source

        lorchestra run canonize_gmail_jmap

        lorchestra run ingest_gmail_acct1 --dry-run

        lorchestra run ingest_gmail_acct1 --test-table

        lorchestra run ingest_gmail_acct1 --smoke-namespace run_20260128

        lorchestra run ingest_gmail_acct1 --smoke-namespace run_20260128 --clean-up
    """
    _run_job_impl(
        ctx,
        job,
        dry_run=dry_run,
        test_table=test_table,
        smoke_namespace=smoke_namespace,
        clean_up=clean_up,
    )


@main.command("init")
@click.option("--force", is_flag=True, help="Overwrite existing configuration")
def init(force: bool):
    """Initialize lorchestra configuration."""
    from lorchestra.config import get_lorchestra_home
    import yaml

    home = get_lorchestra_home()
    if not home.exists():
        home.mkdir(parents=True)

    cfg_path = home / "config.yaml"
    if cfg_path.exists() and not force:
        click.echo(f"Config already exists at {cfg_path}. Use --force to overwrite.", err=True)
        raise SystemExit(1)

    default_cfg = {
        "project": "lifeos-dev",
        "dataset_raw": "raw",
        "dataset_canonical": "canonical",
        "dataset_derived": "derived",
        "sqlite_path": "~/lifeos/local.db",
        "local_views_root": "~/lifeos/local_views",
        "canonizer_registry_root": "~/.local/share/canonizer/registry",
        "formation_registry_root": "~/.local/share/formation/registry",
        "env_file": str(home / ".env"),
        "google_application_credentials": None,
    }
    cfg_path.write_text(yaml.safe_dump(default_cfg, sort_keys=False))

    env_path = home / ".env"
    if not env_path.exists():
        env_path.write_text("# GCP_PROJECT=...\n# BIGQUERY_LOCATION=...\n")

    click.echo(f"Initialized lorchestra config at {cfg_path}")
    click.echo("Remember to run `can init` and `form init` for canonizer/formation if needed.")


@main.command("cleanup-smoke")
@click.argument("namespace")
@click.option("--dry-run", is_flag=True, help="Show what would be deleted without deleting")
@click.pass_context
def cleanup_smoke(ctx, namespace: str, dry_run: bool):
    """Delete a smoke test dataset.

    NAMESPACE is the smoke namespace (the dataset smoke_<NAMESPACE> will be deleted).

    Examples:

        lorchestra cleanup-smoke run_20260128

        lorchestra cleanup-smoke run_20260128 --dry-run
    """
    from google.cloud import bigquery

    # Check config
    if "config" not in ctx.obj:
        click.echo(f"✗ Config not loaded: {ctx.obj.get('config_error', 'Unknown error')}", err=True)
        click.echo("Run 'lorchestra init' to create a configuration file.", err=True)
        raise SystemExit(1)

    config = ctx.obj["config"]
    smoke_dataset = f"smoke_{namespace}"

    click.echo(f"Target dataset: {config.project}.{smoke_dataset}")

    if dry_run:
        click.echo("[DRY-RUN] Would delete dataset (not actually deleting)")
        return

    bq_client = bigquery.Client()
    try:
        deleted = _cleanup_smoke_dataset(bq_client, config.project, smoke_dataset)
        if deleted:
            click.echo(f"✓ Deleted dataset {smoke_dataset}")
        else:
            click.echo(f"Dataset {smoke_dataset} not found (nothing to delete)")
    except ValueError as e:
        click.echo(f"✗ {e}", err=True)
        raise SystemExit(1)
    except Exception as e:
        click.echo(f"✗ Cleanup failed: {e}", err=True)
        raise SystemExit(1)


@main.group("jobs")
def jobs_group():
    """Manage and inspect jobs."""
    pass


@jobs_group.command("list")
@click.option("--type", "job_type", help="Filter by job type (ingest, canonize, projection)")
def list_jobs(job_type: str = None):
    """List available jobs."""
    import json

    # Find all job definition files (excluding _deprecated)
    job_files = [
        f for f in DEFINITIONS_DIR.glob("**/*.json")
        if "_deprecated" not in str(f)
    ]

    if not job_files:
        click.echo("No job definitions found.")
        return

    # Group by job_type
    by_type: dict[str, list[str]] = {}
    for def_path in job_files:
        job_id = def_path.stem
        with open(def_path) as f:
            job_def = json.load(f)
        jt = job_def.get("job_type", "unknown")
        if jt not in by_type:
            by_type[jt] = []
        by_type[jt].append(job_id)

    # Filter if requested
    if job_type:
        if job_type not in by_type:
            click.echo(f"No jobs of type '{job_type}'. Available types: {', '.join(by_type.keys())}")
            return
        by_type = {job_type: by_type[job_type]}

    # Print grouped list
    for jt in sorted(by_type.keys()):
        click.echo(f"{jt}:")
        for job_id in sorted(by_type[jt]):
            click.echo(f"  {job_id}")


@jobs_group.command("show")
@click.argument("job")
def show_job(job: str):
    """Show job definition details."""
    import json

    job_id = job
    if job_id.startswith("job_"):
        job_id = job_id[4:]

    # Search root and subdirectories
    filename = f"{job_id}.json"
    def_path = DEFINITIONS_DIR / filename
    if not def_path.exists():
        found = list(DEFINITIONS_DIR.glob(f"**/{filename}"))
        if not found:
            click.echo(f"✗ Unknown job: {job}", err=True)
            raise SystemExit(1)
        def_path = found[0]

    with open(def_path) as f:
        job_def = json.load(f)

    click.echo(f"Job: {job_id}")
    click.echo(f"Type: {job_def.get('job_type', 'unknown')}")
    click.echo(f"Definition: {def_path}")
    click.echo()
    click.echo(json.dumps(job_def, indent=2))


# =============================================================================
# Stats Commands - Built-in operational reports
# =============================================================================

@main.group("stats")
def stats_group():
    """Built-in operational statistics reports."""
    pass


@stats_group.command("canonical")
@click.pass_context
def stats_canonical(ctx):
    """Show canonical objects by schema and source.

    Displays a count of canonical objects grouped by canonical_schema
    and source_system.

    Example:

        lorchestra stats canonical
    """
    from lorchestra.sql_runner import run_sql_query

    config = ctx.obj["config"]

    sql = """
SELECT
  canonical_schema,
  source_system,
  COUNT(*) AS count
FROM `${PROJECT}.${DATASET_CANONICAL}.canonical_objects`
GROUP BY canonical_schema, source_system
ORDER BY count DESC
"""
    run_sql_query(sql, config=config)


@stats_group.command("raw")
@click.pass_context
def stats_raw(ctx):
    """Show raw objects by source, type, and validation status.

    Displays a count of raw objects grouped by source_system,
    object_type, and validation_status.

    Example:

        lorchestra stats raw
    """
    from lorchestra.sql_runner import run_sql_query

    config = ctx.obj["config"]

    sql = """
SELECT
  source_system,
  object_type,
  validation_status,
  COUNT(*) AS count
FROM `${PROJECT}.${DATASET_RAW}.raw_objects`
GROUP BY source_system, object_type, validation_status
ORDER BY source_system, object_type, validation_status
"""
    run_sql_query(sql, config=config)


@stats_group.command("jobs")
@click.option(
    "--days",
    default=7,
    show_default=True,
    type=int,
    help="Lookback window in days for job statistics.",
)
@click.pass_context
def stats_jobs(ctx, days: int):
    """Show job events from the event log.

    Displays job events grouped by event_type, source_system, and status
    for the specified lookback period.

    Examples:

        lorchestra stats jobs

        lorchestra stats jobs --days 30
    """
    from lorchestra.sql_runner import run_sql_query

    config = ctx.obj["config"]

    sql = """
SELECT
  event_type,
  source_system,
  status,
  COUNT(*) AS count,
  MIN(created_at) AS first_seen,
  MAX(created_at) AS last_seen
FROM `${PROJECT}.${DATASET_RAW}.event_log`
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${DAYS} DAY)
GROUP BY event_type, source_system, status
ORDER BY last_seen DESC
"""
    run_sql_query(sql, config=config, extra_placeholders={"DAYS": str(days)})


# =============================================================================
# Query Command - Named queries from SQL files
# =============================================================================

def _get_available_queries() -> list[str]:
    """Get list of available query names."""
    if not QUERIES_DIR.exists():
        return []
    return sorted([f.stem for f in QUERIES_DIR.glob("*.sql")])


@main.command("query")
@click.argument("name")
@click.pass_context
def query_named(ctx, name: str):
    """Run a named SQL query from the queries directory.

    NAME is the query name (filename without .sql extension).
    Queries are loaded from lorchestra/queries/<name>.sql.

    The query must be read-only (SELECT or WITH only).
    Placeholders ${PROJECT} and ${DATASET} are substituted from
    environment variables.

    Examples:

        lorchestra query canonical-summary

        lorchestra query raw-validation

    Available queries can be listed with:

        ls lorchestra/queries/
    """
    from lorchestra.sql_runner import run_sql_query

    config = ctx.obj["config"]
    query_path = QUERIES_DIR / f"{name}.sql"

    if not query_path.exists():
        available = _get_available_queries()
        click.echo(f"Query '{name}' not found.", err=True)
        click.echo(f"Expected: {query_path}", err=True)
        if available:
            click.echo("\nAvailable queries:", err=True)
            for q in available:
                click.echo(f"  {q}", err=True)
        raise SystemExit(1)

    sql = query_path.read_text()
    run_sql_query(sql, config=config)


# =============================================================================
# SQL Command - Ad-hoc read-only SQL
# =============================================================================

@main.command("sql")
@click.argument("query", required=False)
@click.pass_context
def sql_adhoc(ctx, query: str | None):
    """Run ad-hoc read-only SQL.

    SQL can be provided as an argument, via stdin (pipe), or heredoc.
    The query must be read-only (SELECT or WITH only).

    Placeholders ${PROJECT} and ${DATASET} are substituted from
    environment variables.

    Examples:

        echo "SELECT COUNT(*) FROM raw_objects" | lorchestra sql

        lorchestra sql < my-query.sql
    """
    from lorchestra.sql_runner import run_sql_query

    config = ctx.obj["config"]

    # Get SQL from argument or stdin
    if query:
        sql = query
    elif not sys.stdin.isatty():
        # Reading from pipe or redirect
        sql = sys.stdin.read().strip()
    else:
        raise click.UsageError(
            "No SQL provided. Usage:\n"
            "  lorchestra sql \"SELECT ...\"\n"
            "  echo \"SELECT ...\" | lorchestra sql\n"
            "  lorchestra sql < query.sql"
        )

    if not sql:
        raise click.UsageError("Empty SQL query provided")

    run_sql_query(sql, config=config)


# =============================================================================
# Executor Commands - V2 orchestration engine (e005-02)
# =============================================================================


def _exec_cleanup_smoke(smoke_namespace: str, ctx) -> None:
    """Clean up smoke dataset after an exec run.

    Args:
        smoke_namespace: The smoke namespace to clean up.
        ctx: Click context with config.
    """
    from google.cloud import bigquery

    smoke_dataset = f"smoke_{smoke_namespace}"
    click.echo(f"\nCleaning up smoke dataset: {smoke_dataset}")
    try:
        config = ctx.obj.get("config")
        if config:
            project = config.project
        else:
            import os
            project = os.environ.get("GCP_PROJECT", "")

        bq_client = bigquery.Client()
        deleted = _cleanup_smoke_dataset(bq_client, project, smoke_dataset)
        if deleted:
            click.echo(f"Deleted dataset {smoke_dataset}")
        else:
            click.echo(f"Dataset {smoke_dataset} not found (nothing to delete)")
    except Exception as cleanup_err:
        click.echo(f"Cleanup failed: {cleanup_err}", err=True)


@main.group("exec")
def exec_group():
    """V2 executor commands for JobDef orchestration.

    These commands use the new orchestration layer:
    JobDef -> JobInstance -> RunRecord -> StepManifest
    """
    pass


@exec_group.command("compile")
@click.argument("job_id")
@click.option("--ctx", "ctx_json", default="{}", help="Context JSON for @ctx.* resolution")
@click.option("--payload", "payload_json", default="{}", help="Payload JSON for @payload.* resolution")
@click.option("--output", "-o", type=click.Path(), help="Output path for JobInstance JSON")
def exec_compile(job_id: str, ctx_json: str, payload_json: str, output: str = None):
    """Compile a JobDef into a JobInstance.

    Resolves @ctx.* and @payload.* references and evaluates if conditions.

    Examples:

        lorchestra exec compile my_job

        lorchestra exec compile my_job --ctx '{"env": "prod"}'

        lorchestra exec compile my_job -o instance.json
    """
    import json
    from lorchestra.executor import compile as compile_envelope

    # Parse context and payload
    try:
        compile_ctx = json.loads(ctx_json)
    except json.JSONDecodeError as e:
        click.echo(f"Invalid --ctx JSON: {e}", err=True)
        raise SystemExit(1)

    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError as e:
        click.echo(f"Invalid --payload JSON: {e}", err=True)
        raise SystemExit(1)

    # Compile via the public API
    try:
        envelope = {
            "job_id": job_id,
            "ctx": compile_ctx,
            "payload": payload,
            "definitions_dir": DEFINITIONS_DIR,
        }
        instance = compile_envelope(envelope)
    except Exception as e:
        click.echo(f"Compilation failed: {e}", err=True)
        raise SystemExit(1)

    # Output
    instance_dict = instance.to_dict()
    if output:
        with open(output, "w") as f:
            json.dump(instance_dict, f, indent=2)
        click.echo(f"JobInstance written to: {output}")
    else:
        click.echo(json.dumps(instance_dict, indent=2))


@exec_group.command("run")
@click.argument("job_id")
@click.option("--ctx", "ctx_json", default="{}", help="Context JSON for @ctx.* resolution")
@click.option("--payload", "payload_json", default="{}", help="Payload JSON for @payload.* resolution")
@click.option("--envelope", "envelope_json", default="{}", help="Runtime envelope JSON for @run.* resolution")
@click.option("--dry-run", is_flag=True, help="Execute with no-op backends (no actual I/O)")
@click.option("--store-dir", type=click.Path(), help="Directory for run artifacts (default: in-memory)")
@click.option(
    "--smoke-namespace",
    type=str,
    default=None,
    help="Smoke test namespace (routes BQ writes to isolated smoke_<namespace> dataset)",
)
@click.option(
    "--clean-up",
    is_flag=True,
    help="Delete smoke dataset after run (requires --smoke-namespace)",
)
@click.pass_context
def exec_run(ctx, job_id: str, ctx_json: str, payload_json: str, envelope_json: str,
             dry_run: bool, store_dir: str = None, smoke_namespace: str = None,
             clean_up: bool = False):
    """Compile and execute a JobDef via execute(envelope).

    Builds an envelope from CLI args and calls lorchestra.execute(envelope) --
    the same entry point that production (life) will use.

    Examples:

        lorchestra exec run my_job

        lorchestra exec run my_job --ctx '{"env": "prod"}' --envelope '{"id": 123}'

        lorchestra exec run my_job --dry-run

        lorchestra exec run my_job --store-dir ./runs

        lorchestra exec run my_job --smoke-namespace run_20260129

        lorchestra exec run my_job --smoke-namespace run_20260129 --clean-up
    """
    import json
    from lorchestra.executor import execute
    from lorchestra.run_store import InMemoryRunStore, FileRunStore

    # Validate flag combinations
    if clean_up and not smoke_namespace:
        raise click.UsageError("--clean-up requires --smoke-namespace")
    if dry_run and smoke_namespace:
        raise click.UsageError("--dry-run and --smoke-namespace are mutually exclusive")

    # Parse JSON inputs
    try:
        compile_ctx = json.loads(ctx_json)
    except json.JSONDecodeError as e:
        click.echo(f"Invalid --ctx JSON: {e}", err=True)
        raise SystemExit(1)

    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError as e:
        click.echo(f"Invalid --payload JSON: {e}", err=True)
        raise SystemExit(1)

    try:
        runtime_envelope = json.loads(envelope_json)
    except json.JSONDecodeError as e:
        click.echo(f"Invalid --envelope JSON: {e}", err=True)
        raise SystemExit(1)

    # Create store
    if store_dir:
        store = FileRunStore(Path(store_dir))
    else:
        store = InMemoryRunStore()

    # Print mode banner
    if smoke_namespace:
        smoke_dataset = f"smoke_{smoke_namespace}"
        smoke_prefix = f"smoke_{smoke_namespace}__"
        click.echo("=" * 50)
        click.echo("=== SMOKE TEST MODE ===")
        click.echo(f"Dataset:      {smoke_dataset}")
        click.echo(f"Table prefix: {smoke_prefix}")
        if clean_up:
            click.echo("Cleanup:      enabled (will delete dataset after run)")
        click.echo("=" * 50)
        click.echo()

    # Build envelope -- the single entry point for execution
    envelope = {
        "job_id": job_id,
        "ctx": compile_ctx,
        "payload": payload,
        "definitions_dir": DEFINITIONS_DIR,
        "store": store,
    }

    # Add smoke namespace if provided
    if smoke_namespace:
        envelope["smoke_namespace"] = smoke_namespace

    # Merge runtime envelope fields (user-provided --envelope JSON)
    if runtime_envelope:
        # runtime_envelope goes into the envelope for @run.envelope.* resolution
        # but does NOT override top-level keys like job_id, ctx, etc.
        envelope.setdefault("runtime", runtime_envelope)

    # Execute via the public API
    try:
        if dry_run:
            click.echo("=== DRY RUN MODE === (no-op backends)")
            click.echo()

        result = execute(envelope)

        # Display results
        click.echo(f"Run ID: {result.run_id}")
        click.echo(f"Status: {'SUCCESS' if result.success else 'FAILED'}")
        click.echo()

        click.echo("Step Outcomes:")
        for outcome in result.attempt.step_outcomes:
            status_symbol = {
                "completed": click.style("✓", fg="green"),
                "failed": click.style("✗", fg="red"),
                "skipped": click.style("-", fg="yellow"),
                "pending": click.style("○", fg="white"),
                "running": click.style("…", fg="blue"),
            }.get(outcome.status.value, "?")

            duration = f" ({outcome.duration_ms}ms)" if outcome.duration_ms else ""
            click.echo(f"  {status_symbol} {outcome.step_id}{duration}")

            if outcome.error:
                click.echo(f"      Error: {outcome.error.get('message', 'Unknown error')}")

        if smoke_namespace:
            click.echo(f"\n[SMOKE] {job_id} completed (wrote to smoke_{smoke_namespace})")

        if not result.success:
            raise SystemExit(1)

    except SystemExit:
        raise
    except Exception as e:
        click.echo(f"Execution failed: {e}", err=True)
        raise SystemExit(1)
    finally:
        # Cleanup smoke dataset if requested
        if clean_up and smoke_namespace:
            _exec_cleanup_smoke(smoke_namespace, ctx)


@exec_group.command("status")
@click.argument("run_id")
@click.option("--store-dir", type=click.Path(exists=True), required=True, help="Run artifacts directory")
def exec_status(run_id: str, store_dir: str):
    """Show status of a run.

    Displays the run record and latest attempt status.

    Example:

        lorchestra exec status 01HXYZ123ABC --store-dir ./runs
    """
    import json

    from lorchestra.run_store import FileRunStore

    store = FileRunStore(Path(store_dir))

    # Get run record
    run = store.get_run(run_id)
    if run is None:
        click.echo(f"Run not found: {run_id}", err=True)
        raise SystemExit(1)

    click.echo(f"Run: {run.run_id}")
    click.echo(f"  Job ID:     {run.job_id}")
    click.echo(f"  Job Hash:   {run.job_def_sha256[:16]}...")
    click.echo(f"  Started:    {run.started_at}")
    click.echo(f"  Envelope:   {json.dumps(run.envelope)}")
    click.echo()

    # Get latest attempt
    attempt = store.get_latest_attempt(run_id)
    if attempt:
        click.echo(f"Latest Attempt: #{attempt.attempt_n}")
        click.echo(f"  Status:   {attempt.status.value}")
        click.echo(f"  Started:  {attempt.started_at}")
        if attempt.completed_at:
            click.echo(f"  Completed: {attempt.completed_at}")
            click.echo(f"  Duration: {attempt.duration_ms}ms")
        click.echo(f"  Steps:    {len(attempt.step_outcomes)}")


if __name__ == "__main__":
    main()
