"""file_renderer callable — query SQLite and render markdown files.

Replaces FileProjectionProcessor (e005b-05d). Reads rows from a local
SQLite database, renders each row through path/content/front_matter
templates, and returns file.write items for storacle.

Params:
    sqlite_path: str — path to SQLite database (e.g., "~/clinical-vault/local.db")
    query: str — SQL query to execute against SQLite
    base_path: str — base directory for output files
    path_template: str — Python format string for file path (e.g., "{client_folder}/contact.md")
    content_template: str — Python format string for file content body
    front_matter: dict (optional) — YAML front matter template (values are format strings)

Returns:
    CallableResult with items=[{"path": "/full/path.md", "content": "---\\n...\\n---\\n\\nbody"}]
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from lorchestra.callable.result import CallableResult


def execute(params: dict[str, Any]) -> dict[str, Any]:
    """Query SQLite and render files from templates.

    Args:
        params: Dictionary containing:
            - sqlite_path: str — SQLite database path
            - query: str — SQL query
            - base_path: str — output base directory
            - path_template: str — format string for file path
            - content_template: str — format string for file body
            - front_matter: dict (optional) — front matter template

    Returns:
        CallableResult dict with one item per rendered file

    Raises:
        ValueError: If required params are missing
    """
    sqlite_path_str = params.get("sqlite_path")
    query = params.get("query")
    base_path_str = params.get("base_path")
    path_template = params.get("path_template")
    content_template = params.get("content_template")
    front_matter_spec = params.get("front_matter")

    missing = [
        k for k, v in [
            ("sqlite_path", sqlite_path_str),
            ("query", query),
            ("base_path", base_path_str),
            ("path_template", path_template),
            ("content_template", content_template),
        ] if not v
    ]
    if missing:
        raise ValueError(f"Missing required params: {missing}")

    sqlite_path = Path(sqlite_path_str).expanduser()
    base_path = Path(base_path_str).expanduser()

    # Query SQLite
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(query)
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

    if not rows:
        result = CallableResult(
            items=[],
            stats={"input": 0, "output": 0, "skipped": 0, "errors": 0},
        )
        return result.to_dict()

    # Capture projection timestamp
    projected_at = datetime.now(timezone.utc).isoformat()

    # Render one file.write item per row
    items = []
    for row in rows:
        row_with_meta = {**row, "_projected_at": projected_at}

        # Build file path
        file_path = str(base_path / path_template.format(**row_with_meta))

        # Render content body
        content_body = content_template.format(**row_with_meta)

        # Build front matter if configured
        if front_matter_spec:
            resolved = {}
            for key, value in front_matter_spec.items():
                if isinstance(value, str):
                    resolved[key] = value.format(**row_with_meta)
                else:
                    resolved[key] = value

            front_matter_yaml = yaml.safe_dump(
                resolved, sort_keys=False, allow_unicode=True
            )
            content = f"---\n{front_matter_yaml}---\n\n{content_body}"
        else:
            content = content_body

        items.append({"path": file_path, "content": content})

    result = CallableResult(
        items=items,
        stats={
            "input": len(rows),
            "output": len(items),
            "skipped": 0,
            "errors": 0,
        },
    )
    return result.to_dict()
