"""
Query Builder - Construct parameterized BQ SQL from declarative YAML params.

Used by the storacle.query native op in the executor. SQL construction
lives in lorchestra (here); storacle just executes it (bq.query RPC).

Supports:
- Simple queries with equality filters (dict format: {column: value})
- Extended filters with operators (list format: [{column, op, value}])
- Incremental queries (left_anti, not_exists) with optional join_key_suffix
- Parameterized queries (no string interpolation of filter values)
"""

from dataclasses import dataclass
from typing import Callable


# Allowed comparison operators (SQL-safe)
ALLOWED_OPS = {"=", "!=", "<", ">", "<=", ">="}


@dataclass
class QueryParam:
    """A single parameterized query parameter for BQ."""
    name: str
    type: str
    value: str


def _build_where_clauses(
    filters,
    query_params: list[QueryParam],
    prefix: str = "",
) -> list[str]:
    """Build WHERE clauses from filters.

    Supports two filter formats:
    1. Dict format (legacy): {column: value} - equality only
    2. List format (extended): [{column, op, value}] - with operators

    Args:
        filters: Either a dict or list of filter specs.
        query_params: List to append QueryParam objects to.
        prefix: Column prefix (e.g., "s." for incremental queries).

    Returns:
        List of WHERE clause strings.
    """
    where_clauses: list[str] = []

    if isinstance(filters, dict):
        # Legacy dict format: {column: value}
        for col, val in filters.items():
            where_clauses.append(f"{prefix}{col} = @{col}")
            query_params.append(QueryParam(name=col, type="STRING", value=str(val)))
    elif isinstance(filters, list):
        # Extended list format: [{column, op, value}]
        for i, f in enumerate(filters):
            col = f["column"]
            op = f.get("op", "=")
            val = f["value"]

            # Validate operator
            if op not in ALLOWED_OPS:
                raise ValueError(f"Invalid operator '{op}'. Allowed: {ALLOWED_OPS}")

            # Use indexed param name to avoid collisions
            param_name = f"{col}_{i}"
            where_clauses.append(f"{prefix}{col} {op} @{param_name}")
            query_params.append(QueryParam(name=param_name, type="STRING", value=str(val)))

    return where_clauses


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
    order_by = params.get("order_by")

    query_params: list[QueryParam] = []

    # Build WHERE from filters (parameterized)
    prefix = "s." if incremental else ""
    where_clauses = _build_where_clauses(filters, query_params, prefix)

    source = f"`{dataset}.{table}`"

    limit_clause = f" LIMIT {int(limit)}" if limit else ""

    if not incremental:
        col_list = ", ".join(columns)
        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"
        # Use custom order_by if provided, else fallback to idem_key when limit is set
        if order_by:
            order_clause = f" ORDER BY {order_by}"
        elif limit:
            order_clause = " ORDER BY idem_key"
        else:
            order_clause = ""
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
