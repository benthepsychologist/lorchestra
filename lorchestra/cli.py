"""
CLI interface for lorchestra job orchestrator.

Provides commands to discover, inspect, and run jobs.

Jobs are defined as JSON files in jobs/definitions/**/*.json (organized by
processor type in subdirectories) and dispatched via the JobRunner to typed
processors (ingest, canonize, formation, projection).
"""


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



def _run_job_impl(ctx, job: str, dry_run: bool = False, test_table: bool = False, **kwargs):
    """Run a job by ID using the JobRunner."""
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
    try:
        run_job(
            job_id,
            config=config,
            dry_run=dry_run,
            test_table=test_table,
            definitions_dir=DEFINITIONS_DIR,
            bq_client=bq_client,
        )

        # Success message
        if dry_run:
            click.echo(f"\n[DRY-RUN] {job_id} completed (no writes)")
        elif test_table:
            click.echo(f"\n[TEST] {job_id} completed (wrote to test tables)")
        else:
            click.echo(f"✓ {job_id} completed")

    except Exception as e:
        click.echo(f"✗ {job_id} failed: {e}", err=True)
        raise SystemExit(1)


@main.command("run")
@click.argument("job")
@click.option("--dry-run", is_flag=True, help="Execute without writing to BigQuery")
@click.option("--test-table", is_flag=True, help="Write to test tables instead of prod")
@click.pass_context
def run(ctx, job: str, dry_run: bool, test_table: bool):
    """
    Run a job by ID.

    JOB is the job ID (filename without .json extension).

    Examples:

        lorchestra run ingest_gmail_acct1

        lorchestra run validate_gmail_source

        lorchestra run canonize_gmail_jmap

        lorchestra run ingest_gmail_acct1 --dry-run

        lorchestra run ingest_gmail_acct1 --test-table
    """
    _run_job_impl(ctx, job, dry_run=dry_run, test_table=test_table)


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


if __name__ == "__main__":
    main()
