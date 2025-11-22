"""
lorchestra jobs package

This package contains:
1. Job discovery and execution logic (imported from parent jobs.py)
2. Job implementations (ingest_gmail.py, etc.)

Job implementations follow the three-layer architecture:
1. Call ingestor.extract_to_jsonl() to get raw data from Meltano
2. Read JSONL records
3. Emit events via event_client to BigQuery

Jobs in this package are the ONLY place where event_client.emit() is called.
"""

# Import job discovery functions from the parent jobs.py module
# This resolves the naming conflict between jobs.py and jobs/ package
import sys
from pathlib import Path

# Import from lorchestra.jobs module (the jobs.py file)
# We need to use importlib to import from a sibling module with the same name
import importlib.util

jobs_py_path = Path(__file__).parent.parent / "jobs.py"
spec = importlib.util.spec_from_file_location("_lorchestra_jobs_module", jobs_py_path)
_jobs_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_jobs_module)

# Re-export the functions
discover_jobs = _jobs_module.discover_jobs
get_job = _jobs_module.get_job
execute_job = _jobs_module.execute_job

__all__ = ["discover_jobs", "get_job", "execute_job"]
