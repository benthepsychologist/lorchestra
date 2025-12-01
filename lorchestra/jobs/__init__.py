"""
lorchestra jobs package

Jobs are defined as JSON specs in jobs/specs/*.json and executed via the JobRunner.
The job_runner dispatches to typed processors (IngestProcessor, CanonizeProcessor, FinalFormProcessor).

Legacy job discovery (discover_jobs/get_job/execute_job) is deprecated.
Use job_runner.run_job() instead.
"""

from importlib.metadata import entry_points
from typing import Dict, Callable
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)


# Legacy API - kept for backward compatibility with tests
# New code should use job_runner.run_job() instead

def discover_jobs() -> Dict[str, Dict[str, Callable]]:
    """
    Discover all jobs from lorchestra.jobs entrypoints.

    DEPRECATED: Use job_runner.run_job() with JSON specs instead.

    Returns:
        {
            "ingester": {"extract_gmail": <func>, ...},
            "canonizer": {"canonicalize_email": <func>, ...},
        }
    """
    jobs = {}

    eps = entry_points()
    for ep in eps.select(group="lorchestra.jobs"):
        module_path, _, _ = ep.value.partition(":")
        package_name = module_path.split(".")[0]

        job_func = ep.load()

        if package_name not in jobs:
            jobs[package_name] = {}
        jobs[package_name][ep.name] = job_func

    return jobs


def get_job(package: str, job_name: str) -> Callable:
    """Get a specific job function.

    DEPRECATED: Use job_runner.run_job() with JSON specs instead.
    """
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
    """Execute a job with error handling.

    DEPRECATED: Use job_runner.run_job() with JSON specs instead.
    """
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


__all__ = ["discover_jobs", "get_job", "execute_job"]
