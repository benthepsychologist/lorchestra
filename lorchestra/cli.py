"""
CLI interface for lorchestra job orchestrator.

Provides commands to discover, inspect, and run jobs from installed packages.
"""


import click

from lorchestra import __version__


@click.group()
@click.version_option(version=__version__, prog_name="lorchestra")
def main():
    """
    lorchestra - Lightweight job orchestrator.

    Discovers and runs jobs from installed packages via entrypoints.
    """
    pass


def _run_job_impl(job: str, dry_run: bool = False, test_table: bool = False, **kwargs):
    """Shared implementation for run/run-job commands."""
    from google.cloud import bigquery
    from lorchestra.jobs import execute_job, discover_jobs
    from lorchestra.stack_clients.event_client import (
        log_event,
        set_run_mode,
        ensure_test_tables_exist,
    )
    from datetime import datetime, timezone
    import time

    # Validate mutually exclusive flags
    if dry_run and test_table:
        raise click.UsageError("--dry-run and --test-table are mutually exclusive")

    # Set run mode BEFORE any BQ operations
    set_run_mode(dry_run=dry_run, test_table=test_table)

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

    # Parse job argument - support PACKAGE/JOB or just JOB
    if "/" in job:
        package, job_name = job.split("/", 1)
    else:
        # Auto-discover package
        all_jobs = discover_jobs()
        matching_packages = [pkg for pkg, jobs in all_jobs.items() if job in jobs]

        if not matching_packages:
            click.echo(f"✗ Unknown job: {job}", err=True)
            click.echo("\nAvailable jobs:", err=True)
            for pkg in sorted(all_jobs.keys()):
                for job_name in sorted(all_jobs[pkg].keys()):
                    click.echo(f"  {job_name}", err=True)
            raise SystemExit(1)

        if len(matching_packages) > 1:
            click.echo(f"✗ Ambiguous job name '{job}' found in multiple packages:", err=True)
            for pkg in matching_packages:
                click.echo(f"  {pkg}/{job}", err=True)
            click.echo("\nPlease specify: lorchestra run PACKAGE/JOB", err=True)
            raise SystemExit(1)

        package = matching_packages[0]
        job_name = job

    # Create BQ client once
    bq_client = bigquery.Client()

    # Ensure test tables exist (only for test-table mode)
    if test_table:
        ensure_test_tables_exist(bq_client)

    # Generate run_id for job execution tracking
    run_id = f"{job_name}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    # IMPORTANT: Only pass known, explicit options to jobs
    # Don't pass the entire kwargs blob - keeps job interface stable
    known_options = ["account", "since", "until"]
    job_kwargs = {k: v for k, v in kwargs.items() if k in known_options and v is not None}

    # Log job.started event
    log_event(
        event_type="job.started",
        source_system="lorchestra",
        correlation_id=run_id,
        object_type="job_run",
        status="ok",
        payload={
            "job_name": job_name,
            "package_name": package,
            "parameters": job_kwargs,
        },
        bq_client=bq_client,
    )

    # Execute
    start_time = time.time()
    try:
        execute_job(package, job_name, bq_client, **job_kwargs)
        duration_seconds = time.time() - start_time

        # Log job.completed event
        log_event(
            event_type="job.completed",
            source_system="lorchestra",
            correlation_id=run_id,
            object_type="job_run",
            status="ok",
            payload={
                "job_name": job_name,
                "package_name": package,
                "duration_seconds": round(duration_seconds, 2),
            },
            bq_client=bq_client,
        )

        # Summary message based on mode
        if dry_run:
            click.echo(f"\n[DRY-RUN] {job_name} completed in {duration_seconds:.2f}s (no writes)")
        elif test_table:
            click.echo(f"\n[TEST] {job_name} completed in {duration_seconds:.2f}s (wrote to test tables)")
        else:
            click.echo(f"✓ {package}/{job_name} completed in {duration_seconds:.2f}s")

    except Exception as e:
        duration_seconds = time.time() - start_time

        # Log job.failed event
        log_event(
            event_type="job.failed",
            source_system="lorchestra",
            correlation_id=run_id,
            object_type="job_run",
            status="failed",
            error_message=str(e),
            payload={
                "job_name": job_name,
                "package_name": package,
                "error_type": type(e).__name__,
                "duration_seconds": round(duration_seconds, 2),
            },
            bq_client=bq_client,
        )

        click.echo(f"✗ {package}/{job_name} failed: {e}", err=True)
        raise SystemExit(1)


@main.command("run")
@click.argument("job")
@click.option("--account", help="Account identifier")
@click.option("--since", help="Start time (ISO or relative)")
@click.option("--until", help="End time (ISO)")
@click.option("--dry-run", is_flag=True, help="Extract records without writing to BigQuery")
@click.option("--test-table", is_flag=True, help="Write to test_event_log/test_raw_objects instead of prod")
def run(job: str, dry_run: bool, test_table: bool, **kwargs):
    """
    Run a job by name.

    Examples:
        lorchestra run gmail_ingest_acct1 --since "2025-11-23"
        lorchestra run gmail_ingest_acct2
        lorchestra run gmail_ingest_acct1 --dry-run
        lorchestra run gmail_ingest_acct1 --test-table
    """
    _run_job_impl(job, dry_run=dry_run, test_table=test_table, **kwargs)


@main.command("run-job")
@click.argument("job")
@click.option("--account", help="Account identifier")
@click.option("--since", help="Start time (ISO or relative)")
@click.option("--until", help="End time (ISO)")
@click.option("--dry-run", is_flag=True, help="Extract records without writing to BigQuery")
@click.option("--test-table", is_flag=True, help="Write to test_event_log/test_raw_objects instead of prod")
def run_job(job: str, dry_run: bool, test_table: bool, **kwargs):
    """
    Run a job by name (alias for 'run').

    Examples:
        lorchestra run-job gmail_ingest_acct1 --since "2025-11-23"
        lorchestra run-job gmail_ingest_acct2
        lorchestra run-job gmail_ingest_acct1 --dry-run
        lorchestra run-job gmail_ingest_acct1 --test-table
    """
    _run_job_impl(job, dry_run=dry_run, test_table=test_table, **kwargs)


@main.group("jobs")
def jobs_group():
    """Manage and inspect jobs."""
    pass


@jobs_group.command("list")
@click.argument("package", required=False)
def list_jobs(package: str = None):
    """List available jobs."""
    from lorchestra.jobs import discover_jobs

    all_jobs = discover_jobs()

    if package:
        if package not in all_jobs:
            click.echo(f"Unknown package: {package}", err=True)
            raise SystemExit(1)
        click.echo(f"Jobs in {package}:")
        for job_name in sorted(all_jobs[package].keys()):
            click.echo(f"  {job_name}")
    else:
        for pkg in sorted(all_jobs.keys()):
            click.echo(f"{pkg}:")
            for job_name in sorted(all_jobs[pkg].keys()):
                click.echo(f"  {job_name}")


@jobs_group.command("show")
@click.argument("package")
@click.argument("job")
def show_job(package: str, job: str):
    """Show job details."""
    from lorchestra.jobs import get_job
    import inspect

    job_func = get_job(package, job)
    click.echo(f"{package}/{job}")
    click.echo(f"Location: {inspect.getfile(job_func)}")
    click.echo(f"Signature: {inspect.signature(job_func)}")
    if job_func.__doc__:
        click.echo(f"\n{job_func.__doc__}")


if __name__ == "__main__":
    main()
