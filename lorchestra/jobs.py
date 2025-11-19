"""Job discovery and execution via entrypoints."""
from importlib.metadata import entry_points
from typing import Dict, Callable
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)


def discover_jobs() -> Dict[str, Dict[str, Callable]]:
    """
    Discover all jobs from lorchestra.jobs entrypoints.

    Returns:
        {
            "ingester": {"extract_gmail": <func>, ...},
            "canonizer": {"canonicalize_email": <func>, ...},
        }
    """
    jobs = {}

    # Use .select() for Python 3.10+ compatibility
    eps = entry_points()
    for ep in eps.select(group="lorchestra.jobs"):
        # ep.name: "extract_gmail"
        # ep.value: "ingester.jobs.email:extract_gmail"

        # Extract package name from module path (convention)
        module_path, _, _ = ep.value.partition(":")
        package_name = module_path.split(".")[0]  # "ingester.jobs.email" → "ingester"

        job_func = ep.load()

        if package_name not in jobs:
            jobs[package_name] = {}
        jobs[package_name][ep.name] = job_func

    return jobs


def get_job(package: str, job_name: str) -> Callable:
    """Get a specific job function."""
    all_jobs = discover_jobs()
    if package not in all_jobs:
        raise ValueError(f"Unknown package: {package}")
    if job_name not in all_jobs[package]:
        raise ValueError(f"Unknown job: {package}/{job_name}")
    return all_jobs[package][job_name]


def execute_job(
    package: str,
    job_name: str,
    bq_client: bigquery.Client,
    **kwargs
) -> None:
    """Execute a job with error handling."""
    job_func = get_job(package, job_name)

    logger.info(f"Starting job: {package}/{job_name}")
    try:
        job_func(bq_client=bq_client, **kwargs)
        logger.info(f"Job completed: {package}/{job_name}")
    except Exception:
        logger.error(
            f"Job failed: {package}/{job_name}",
            exc_info=True,
            extra={"package": package, "job": job_name}
        )
        raise
