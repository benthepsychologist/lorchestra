"""bq_reader callable — read BQ view/table and package for sqlite.sync.

Replaces SyncSqliteProcessor's BQ read logic (e005b-05c). Reads all rows
from a BQ projection view and packages them as a single sqlite.sync item.

Params:
    projection: str — BQ view/table name (e.g., "proj_clients")
    sqlite_path: str — target SQLite file path (e.g., "~/clinical-vault/local.db")
    table: str — target SQLite table name (e.g., "clients")
    dataset: str (optional) — "canonical" (default) or "derived"

Returns:
    CallableResult with items=[{
        "sqlite_path": "/expanded/path/to/local.db",
        "table": "clients",
        "columns": ["col1", "col2", ..., "projected_at"],
        "rows": [{"col1": "val", ..., "projected_at": "2024-..."}, ...]
    }]
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lorchestra.callable.result import CallableResult


def execute(params: dict[str, Any]) -> dict[str, Any]:
    """Read BQ view/table and return rows packaged for sqlite.sync.

    Args:
        params: Dictionary containing:
            - projection: str — BQ view/table name
            - sqlite_path: str — target SQLite path
            - table: str — target SQLite table name
            - dataset: str (optional) — "canonical" or "derived"

    Returns:
        CallableResult dict with single item for sqlite.sync

    Raises:
        ValueError: If required params are missing
    """
    from lorchestra.config import load_config

    projection = params.get("projection")
    sqlite_path = params.get("sqlite_path")
    table = params.get("table")
    dataset_key = params.get("dataset", "canonical")

    missing = [
        k for k, v in [
            ("projection", projection),
            ("sqlite_path", sqlite_path),
            ("table", table),
        ] if not v
    ]
    if missing:
        raise ValueError(f"Missing required params: {missing}")

    config = load_config()
    project = config.project

    if dataset_key == "derived":
        dataset = config.dataset_derived
    elif dataset_key == "canonical":
        dataset = config.dataset_canonical
    else:
        dataset = dataset_key

    # Resolve sqlite_path (expand ~)
    resolved_sqlite_path = str(Path(sqlite_path).expanduser())

    # Build fully qualified BQ view name
    view_name = f"`{project}.{dataset}.{projection}`"
    sql = f"SELECT * FROM {view_name}"

    # Execute BQ query via storacle BigQueryClient
    from storacle.clients.bigquery import BigQueryClient
    bq_client = BigQueryClient(project=project)
    query_result = bq_client.query(sql)
    rows = query_result.rows

    if not rows:
        result = CallableResult(
            items=[{
                "sqlite_path": resolved_sqlite_path,
                "table": table,
                "columns": [],
                "rows": [],
            }],
            stats={"input": 0, "output": 0, "skipped": 0, "errors": 0},
        )
        return result.to_dict()

    # Capture projection timestamp
    projected_at = datetime.now(timezone.utc).isoformat()

    # Get columns from first row + add projected_at
    original_columns = list(rows[0].keys())
    columns = original_columns + ["projected_at"]

    # Build row dicts with projected_at and stringify values
    sync_rows = []
    for row in rows:
        sync_row = {}
        for col in original_columns:
            val = row.get(col)
            sync_row[col] = str(val) if val is not None else None
        sync_row["projected_at"] = projected_at
        sync_rows.append(sync_row)

    result = CallableResult(
        items=[{
            "sqlite_path": resolved_sqlite_path,
            "table": table,
            "columns": columns,
            "rows": sync_rows,
        }],
        stats={
            "input": len(rows),
            "output": len(sync_rows),
            "skipped": 0,
            "errors": 0,
        },
    )
    return result.to_dict()
