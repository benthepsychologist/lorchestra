"""molt_projector callable — generate CTAS DDL for cross-project sync.

Replaces CrossProjectSyncProcessor (e005b-05b). Produces a single SQL
string item (CREATE OR REPLACE TABLE ... AS SELECT) for submission to
storacle via bq.execute.

Params:
    query_name: str — key in lorchestra.sql.molt_projections.MOLT_PROJECTIONS
        (e.g., "context_emails", "context_calendar", "context_actions")
    sink_project: str — target GCP project (e.g., "molt-chatbot")
    sink_dataset: str — target BQ dataset (e.g., "molt")
    sink_table: str — target BQ table (e.g., "context_emails")

Returns:
    CallableResult with items=[{"sql": "CREATE OR REPLACE TABLE ... AS SELECT ..."}]
"""

from __future__ import annotations

from typing import Any

from lorchestra.callable.result import CallableResult
from lorchestra.sql.molt_projections import get_molt_projection_sql


def execute(params: dict[str, Any]) -> dict[str, Any]:
    """Generate CTAS SQL for a cross-project molt projection.

    Args:
        params: Dictionary containing:
            - query_name: str — registered molt projection name
            - sink_project: str — target GCP project
            - sink_dataset: str — target BQ dataset
            - sink_table: str — target BQ table

    Returns:
        CallableResult dict with items=[{"sql": ctas_string}]

    Raises:
        ValueError: If required params are missing
        KeyError: If query_name is not in the registry
    """
    from lorchestra.config import load_config

    query_name = params.get("query_name")
    sink_project = params.get("sink_project")
    sink_dataset = params.get("sink_dataset")
    sink_table = params.get("sink_table")

    missing = [
        k for k, v in [
            ("query_name", query_name),
            ("sink_project", sink_project),
            ("sink_dataset", sink_dataset),
            ("sink_table", sink_table),
        ] if not v
    ]
    if missing:
        raise ValueError(f"Missing required params: {missing}")

    config = load_config()
    source_sql = get_molt_projection_sql(
        name=query_name,
        project=config.project,
        dataset=config.dataset_canonical,
    )

    fq_table = f"`{sink_project}.{sink_dataset}.{sink_table}`"
    ctas = f"CREATE OR REPLACE TABLE {fq_table} AS\n{source_sql}"

    result = CallableResult(
        items=[{"sql": ctas}],
        stats={"input": 1, "output": 1, "skipped": 0, "errors": 0},
    )
    return result.to_dict()
