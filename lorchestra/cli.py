"""
CLI interface for lorchestra job orchestrator.

Provides commands to discover, inspect, and run jobs.

Jobs are defined as JSON files in jobs/definitions/*.json and dispatched
via the JobRunner to typed processors (ingest, canonize, final_form).
"""


import click
from pathlib import Path

from lorchestra import __version__


# Job definitions directory
DEFINITIONS_DIR = Path(__file__).parent / "jobs" / "definitions"


def _get_available_jobs() -> list[str]:
    """Get list of available job IDs."""
    if not DEFINITIONS_DIR.exists():
        return []
    return sorted([f.stem for f in DEFINITIONS_DIR.glob("*.json")])


@click.group()
@click.version_option(version=__version__, prog_name="lorchestra")
def main():
    """
    lorchestra - Lightweight job orchestrator.

    Run jobs defined as JSON specs via typed processors.
    """
    pass


def _run_job_impl(job: str, dry_run: bool = False, test_table: bool = False, **kwargs):
    """Run a job by ID using the JobRunner."""
    from google.cloud import bigquery
    from lorchestra.job_runner import run_job
    from lorchestra.stack_clients.event_client import ensure_test_tables_exist

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
        ensure_test_tables_exist(bq_client)

    # Run job via JobRunner
    try:
        run_job(
            job_id,
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
def run(job: str, dry_run: bool, test_table: bool):
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
    _run_job_impl(job, dry_run=dry_run, test_table=test_table)


@main.group("jobs")
def jobs_group():
    """Manage and inspect jobs."""
    pass


@jobs_group.command("list")
@click.option("--type", "job_type", help="Filter by job type (ingest, canonize, final_form)")
def list_jobs(job_type: str = None):
    """List available jobs."""
    import json

    jobs = _get_available_jobs()

    if not jobs:
        click.echo("No job definitions found.")
        return

    # Group by job_type
    by_type: dict[str, list[str]] = {}
    for job_id in jobs:
        def_path = DEFINITIONS_DIR / f"{job_id}.json"
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

    def_path = DEFINITIONS_DIR / f"{job_id}.json"
    if not def_path.exists():
        click.echo(f"✗ Unknown job: {job}", err=True)
        raise SystemExit(1)

    with open(def_path) as f:
        job_def = json.load(f)

    click.echo(f"Job: {job_id}")
    click.echo(f"Type: {job_def.get('job_type', 'unknown')}")
    click.echo(f"Definition: {def_path}")
    click.echo()
    click.echo(json.dumps(job_def, indent=2))


if __name__ == "__main__":
    main()
