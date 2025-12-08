"""Projection processors for lorchestra.

This module provides processors for the therapist surface projection pipeline:

1. CreateProjectionProcessor (job_type: create_projection)
   - Creates/updates BigQuery views from SQL defined in lorchestra/sql/projections.py
   - Views extract and flatten data from canonical_objects table
   - Run once initially, then whenever projection SQL changes

2. SyncSqliteProcessor (job_type: sync_sqlite)
   - Syncs BQ projection view to local SQLite table
   - Uses full replace strategy (DELETE + INSERT)
   - Run daily to refresh local data

3. FileProjectionProcessor (job_type: file_projection)
   - Queries SQLite and renders results to markdown files
   - Uses path_template for file locations: "{client_folder}/sessions/session-{session_num}/transcript.md"
   - Uses content_template for file content: "{content}"

Pipeline flow:
    BigQuery canonical_objects
        → BQ Views (proj_*)
        → SQLite tables
        → Markdown files

See docs/projection-pipeline.md for full documentation.
"""

import os
import sqlite3
from pathlib import Path
from typing import Any

from lorchestra.processors import registry
from lorchestra.processors.base import EventClient, JobContext, StorageClient
from lorchestra.sql.projections import get_projection_sql


class CreateProjectionProcessor:
    """Create/update BQ projection views from hardcoded SQL."""

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute the create_projection job.

        Args:
            job_spec: Job specification with projection.name
            context: Execution context
            storage_client: Storage operations interface
            event_client: Event emission interface
        """
        proj_name = job_spec["projection"]["name"]
        source_system = job_spec.get("source_system", "lorchestra")

        # Get project and dataset from context config
        project = context.config.project
        dataset = context.config.dataset_canonical

        # Log start event
        event_client.log_event(
            event_type="projection.started",
            source_system=source_system,
            correlation_id=context.run_id,
            status="success",
            payload={
                "projection_name": proj_name,
                "project": project,
                "dataset": dataset,
            },
        )

        try:
            # Get SQL and execute
            sql = get_projection_sql(proj_name, project, dataset)

            if context.dry_run:
                # Log what would happen
                event_client.log_event(
                    event_type="projection.dry_run",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={
                        "projection_name": proj_name,
                        "sql_preview": sql[:500],
                    },
                )
            else:
                # Execute the CREATE VIEW SQL
                result = storage_client.execute_sql(sql)

                # Log completion event
                event_client.log_event(
                    event_type="projection.completed",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={
                        "projection_name": proj_name,
                        "rows_affected": result.get("rows_affected", 0),
                    },
                )

        except Exception as e:
            # Log failure event
            event_client.log_event(
                event_type="projection.failed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "projection_name": proj_name,
                },
            )
            raise


class SyncSqliteProcessor:
    """Sync BQ projection to local SQLite (full replace)."""

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute the sync_sqlite job.

        Args:
            job_spec: Job specification with source.projection, sink.sqlite_path, sink.table
            context: Execution context
            storage_client: Storage operations interface
            event_client: Event emission interface
        """
        proj_name = job_spec["source"]["projection"]
        sqlite_path = Path(job_spec["sink"]["sqlite_path"]).expanduser()
        table = job_spec["sink"]["table"]
        source_system = job_spec.get("source_system", "lorchestra")

        # Get project and dataset from context config
        project = context.config.project
        dataset = context.config.dataset_canonical

        # Log start event
        event_client.log_event(
            event_type="sync.started",
            source_system=source_system,
            correlation_id=context.run_id,
            status="success",
            payload={
                "projection_name": proj_name,
                "sqlite_path": str(sqlite_path),
                "table": table,
            },
        )

        try:
            # Build the fully qualified view name
            view_name = f"`{project}.{dataset}.{proj_name}`"

            if context.dry_run:
                # Log what would happen
                event_client.log_event(
                    event_type="sync.dry_run",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={
                        "projection_name": proj_name,
                        "sqlite_path": str(sqlite_path),
                        "table": table,
                    },
                )
                return

            # 1. Query BQ projection
            rows = storage_client.query_to_dataframe(f"SELECT * FROM {view_name}")

            if not rows:
                event_client.log_event(
                    event_type="sync.completed",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={"rows": 0, "table": table},
                )
                return

            # 2. Ensure SQLite directory exists
            sqlite_path.parent.mkdir(parents=True, exist_ok=True)

            # 3. Write to SQLite (DELETE + INSERT)
            conn = sqlite3.connect(sqlite_path)
            try:
                # Get column names from first row
                columns = list(rows[0].keys())

                # Create table if not exists (infer types from first row)
                col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
                conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')

                # Delete existing data
                conn.execute(f'DELETE FROM "{table}"')

                # Bulk insert rows
                placeholders = ", ".join("?" * len(columns))
                col_names = ", ".join(f'"{c}"' for c in columns)
                insert_sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'

                for row in rows:
                    values = [str(row.get(col)) if row.get(col) is not None else None for col in columns]
                    conn.execute(insert_sql, values)

                conn.commit()
            finally:
                conn.close()

            # Log completion event
            event_client.log_event(
                event_type="sync.completed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="success",
                payload={
                    "rows": len(rows),
                    "table": table,
                    "sqlite_path": str(sqlite_path),
                },
            )

        except Exception as e:
            # Log failure event
            event_client.log_event(
                event_type="sync.failed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "projection_name": proj_name,
                    "table": table,
                },
            )
            raise


class FileProjectionProcessor:
    """Render SQLite data to markdown files."""

    def run(
        self,
        job_spec: dict[str, Any],
        context: JobContext,
        storage_client: StorageClient,
        event_client: EventClient,
    ) -> None:
        """Execute the file_projection job.

        Args:
            job_spec: Job specification with source.sqlite_path, source.query,
                      sink.base_path, sink.path_template, sink.content_template
            context: Execution context
            storage_client: Storage operations interface (not used directly)
            event_client: Event emission interface
        """
        sqlite_path = Path(job_spec["source"]["sqlite_path"]).expanduser()
        query = job_spec["source"]["query"]
        base_path = Path(job_spec["sink"]["base_path"]).expanduser()
        path_template = job_spec["sink"]["path_template"]
        content_template = job_spec["sink"]["content_template"]
        source_system = job_spec.get("source_system", "lorchestra")

        # Log start event
        event_client.log_event(
            event_type="file_projection.started",
            source_system=source_system,
            correlation_id=context.run_id,
            status="success",
            payload={
                "sqlite_path": str(sqlite_path),
                "base_path": str(base_path),
                "query": query[:100],
            },
        )

        try:
            if context.dry_run:
                # Log what would happen
                event_client.log_event(
                    event_type="file_projection.dry_run",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={
                        "sqlite_path": str(sqlite_path),
                        "base_path": str(base_path),
                    },
                )
                return

            # Query SQLite
            conn = sqlite3.connect(sqlite_path)
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute(query)
                rows = [dict(row) for row in cursor.fetchall()]
            finally:
                conn.close()

            if not rows:
                event_client.log_event(
                    event_type="file_projection.completed",
                    source_system=source_system,
                    correlation_id=context.run_id,
                    status="success",
                    payload={"files": 0},
                )
                return

            # Render files
            files_written = 0
            for row in rows:
                # Build file path from template
                file_path = base_path / path_template.format(**row)
                file_path.parent.mkdir(parents=True, exist_ok=True)

                # Render content from template
                content = content_template.format(**row)
                file_path.write_text(content)
                files_written += 1

            # Log completion event
            event_client.log_event(
                event_type="file_projection.completed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="success",
                payload={
                    "files": files_written,
                    "base_path": str(base_path),
                },
            )

        except Exception as e:
            # Log failure event
            event_client.log_event(
                event_type="file_projection.failed",
                source_system=source_system,
                correlation_id=context.run_id,
                status="failed",
                error_message=str(e),
                payload={
                    "sqlite_path": str(sqlite_path),
                    "base_path": str(base_path),
                },
            )
            raise


# Register the processors
registry.register("create_projection", CreateProjectionProcessor())
registry.register("sync_sqlite", SyncSqliteProcessor())
registry.register("file_projection", FileProjectionProcessor())
