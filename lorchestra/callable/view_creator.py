"""view_creator callable — generate CREATE VIEW DDL for BQ projection views.

Replaces CreateProjectionProcessor (e005b-05b). Produces a single SQL
string item for submission to storacle via bq.execute.

Params:
    projection_name: str — key in lorchestra.sql.projections.PROJECTIONS
        (e.g., "proj_clients", "proj_sessions")

Returns:
    CallableResult with items=[{"sql": "CREATE OR REPLACE VIEW ..."}]
"""

from __future__ import annotations

from typing import Any

from lorchestra.callable.result import CallableResult
from lorchestra.sql.projections import get_projection_sql


def execute(params: dict[str, Any]) -> dict[str, Any]:
    """Generate CREATE VIEW SQL for a named projection.

    Args:
        params: Dictionary containing:
            - projection_name: str — registered projection name
            - dataset: str (optional) — target dataset override (default: config.dataset_canonical)

    Returns:
        CallableResult dict with items=[{"sql": ddl_string}]

    Raises:
        ValueError: If projection_name is missing
        KeyError: If projection_name is not in the registry
    """
    from lorchestra.config import load_config

    projection_name = params.get("projection_name")
    if not projection_name:
        raise ValueError("Missing required param: 'projection_name'")

    config = load_config()
    dataset = params.get("dataset") or config.dataset_canonical
    sql = get_projection_sql(
        name=projection_name,
        project=config.project,
        dataset=dataset,
        wal_dataset=config.dataset_wal,
    )

    result = CallableResult(
        items=[{"sql": sql}],
        stats={"input": 1, "output": 1, "skipped": 0, "errors": 0},
    )
    return result.to_dict()
