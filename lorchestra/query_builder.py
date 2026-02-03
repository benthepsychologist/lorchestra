"""
Query Builder - Construct parameterized BQ SQL from declarative YAML params.

Used by the storacle.query native op in the executor. SQL construction
lives in lorchestra (here); storacle just executes it (bq.query RPC).

Supports:
- Simple queries with equality filters
- Incremental queries (left_anti, not_exists) with optional join_key_suffix
- Parameterized queries (no string interpolation of filter values)
"""

from dataclasses import dataclass
from typing import Callable


@dataclass
class QueryParam:
    """A single parameterized query parameter for BQ."""
    name: str
    type: str
    value: str


def build_query(
    params: dict,
    *,
    resolve_dataset: Callable[[str], str],
) -> tuple[str, list[QueryParam]]:
    """Build a BQ SQL query from declarative storacle.query params.

    Returns (sql_string, query_parameters) for parameterized execution.

    Args:
        params: Declarative query params from YAML step definition.
        resolve_dataset: Resolves logical dataset names (raw, canonical, derived)
            to actual BQ dataset names.

    Returns:
        Tuple of (SQL string, list of QueryParam for parameterized execution).
    """
    dataset = resolve_dataset(params["dataset"])
    table = params["table"]
    columns = params.get("columns", ["*"])
    filters = params.get("filters", {})
    limit = params.get("limit")
    incremental = params.get("incremental")

    query_params: list[QueryParam] = []

    # Build WHERE from filters (parameterized)
    where_clauses: list[str] = []
    for col, val in filters.items():
        prefix = "s." if incremental else ""
        where_clauses.append(f"{prefix}{col} = @{col}")
        query_params.append(QueryParam(name=col, type="STRING", value=str(val)))

    source = f"`{dataset}.{table}`"

    limit_clause = f" LIMIT {int(limit)}" if limit else ""

    if not incremental:
        col_list = ", ".join(columns)
        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"
        # Add ORDER BY for deterministic results with LIMIT
        order_clause = " ORDER BY idem_key" if limit else ""
        return f"SELECT {col_list} FROM {source} WHERE {where_sql}{order_clause}{limit_clause}", query_params

    # Incremental: LEFT JOIN or NOT EXISTS against target
    col_list = ", ".join(f"s.{c}" for c in columns)

    target_dataset = resolve_dataset(incremental["target_dataset"])
    target_table = incremental["target_table"]
    source_key = incremental["source_key"]
    target_key = incremental["target_key"]
    suffix = incremental.get("join_key_suffix")
    mode = incremental["mode"]
    target = f"`{target_dataset}.{target_table}`"

    # JOIN condition
    if suffix:
        join_cond = f"t.{target_key} = CONCAT(s.{source_key}, '#', @_join_suffix)"
        query_params.append(QueryParam(name="_join_suffix", type="STRING", value=suffix))
    else:
        join_cond = f"t.{target_key} = s.{source_key}"

    source_ts = incremental.get("source_ts", "last_seen")
    target_ts = incremental.get("target_ts", "canonicalized_at")

    if mode == "left_anti":
        where_clauses.append(
            f"(t.{target_key} IS NULL OR s.{source_ts} > t.{target_ts})"
        )
        where_sql = " AND ".join(where_clauses)
        return (
            f"SELECT {col_list} FROM {source} s "
            f"LEFT JOIN {target} t ON {join_cond} "
            f"WHERE {where_sql} "
            f"ORDER BY s.{source_key}{limit_clause}",
            query_params,
        )

    elif mode == "not_exists":
        where_clauses.append(f"NOT EXISTS (SELECT 1 FROM {target} t WHERE {join_cond})")
        where_sql = " AND ".join(where_clauses)
        return (
            f"SELECT {col_list} FROM {source} s WHERE {where_sql}{limit_clause}",
            query_params,
        )

    raise ValueError(f"Unknown incremental mode: {mode}")
