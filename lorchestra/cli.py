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


@main.command("run-job")
@click.argument("package")
@click.argument("job")
@click.option("--account", help="Account identifier")
@click.option("--since", help="Start time (ISO or relative)")
@click.option("--until", help="End time (ISO)")
def run_job(package: str, job: str, **kwargs):
    """
    Run a job from an installed package.

    Examples:
        lorchestra run-job ingester extract_gmail --account acct1-personal
        lorchestra run-job canonizer canonicalize_email
    """
    from google.cloud import bigquery
    from lorchestra.jobs import execute_job

    # Create BQ client once
    bq_client = bigquery.Client()

    # IMPORTANT: Only pass known, explicit options to jobs
    # Don't pass the entire kwargs blob - keeps job interface stable
    known_options = ["account", "since", "until"]
    job_kwargs = {k: v for k, v in kwargs.items() if k in known_options and v is not None}

    # Execute
    try:
        execute_job(package, job, bq_client, **job_kwargs)
        click.echo(f"✓ {package}/{job} completed")
    except Exception as e:
        click.echo(f"✗ {package}/{job} failed: {e}", err=True)
        raise SystemExit(1)


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
