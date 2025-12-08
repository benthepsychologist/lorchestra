"""Tests for projection processors."""

import sqlite3
import tempfile
from pathlib import Path

import pytest
import yaml
from unittest.mock import MagicMock

from lorchestra.processors.base import JobContext
from lorchestra.processors.projection import (
    CreateProjectionProcessor,
    FileProjectionProcessor,
    SyncSqliteProcessor,
)


class TestCreateProjectionProcessor:
    """Tests for CreateProjectionProcessor."""

    @pytest.fixture
    def processor(self):
        """Create a CreateProjectionProcessor instance."""
        return CreateProjectionProcessor()

    @pytest.fixture
    def mock_context(self, test_config):
        """Create a mock JobContext."""
        return JobContext(
            bq_client=MagicMock(),
            run_id="test-run-123",
            config=test_config,
            dry_run=False,
            test_table=False,
        )

    @pytest.fixture
    def mock_storage_client(self):
        """Create a mock StorageClient."""
        client = MagicMock()
        client.execute_sql.return_value = {"rows_affected": 0, "total_rows": 0}
        return client

    @pytest.fixture
    def mock_event_client(self):
        """Create a mock EventClient."""
        return MagicMock()

    def test_create_projection_executes_sql(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """CreateProjectionProcessor executes the projection SQL."""
        job_spec = {
            "job_id": "test_create_projection",
            "job_type": "create_projection",
            "projection": {"name": "proj_clients"},
        }

        processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

        # Verify SQL was executed
        mock_storage_client.execute_sql.assert_called_once()
        sql_arg = mock_storage_client.execute_sql.call_args[0][0]
        assert "CREATE OR REPLACE VIEW" in sql_arg
        assert "test-project" in sql_arg
        assert "test_canonical" in sql_arg
        assert "proj_clients" in sql_arg

    def test_create_projection_logs_events(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """CreateProjectionProcessor logs started and completed events."""
        job_spec = {
            "job_id": "test_create_projection",
            "job_type": "create_projection",
            "projection": {"name": "proj_clients"},
        }

        processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

        # Verify events were logged
        assert mock_event_client.log_event.call_count == 2

        # Check started event
        started_call = mock_event_client.log_event.call_args_list[0]
        assert started_call.kwargs["event_type"] == "projection.started"
        assert started_call.kwargs["status"] == "success"
        assert started_call.kwargs["payload"]["projection_name"] == "proj_clients"

        # Check completed event
        completed_call = mock_event_client.log_event.call_args_list[1]
        assert completed_call.kwargs["event_type"] == "projection.completed"
        assert completed_call.kwargs["status"] == "success"

    def test_create_projection_dry_run(
        self, processor, mock_storage_client, mock_event_client, test_config
    ):
        """CreateProjectionProcessor skips SQL execution in dry run mode."""
        context = JobContext(
            bq_client=MagicMock(),
            run_id="test-run-123",
            config=test_config,
            dry_run=True,
            test_table=False,
        )
        job_spec = {
            "job_id": "test_create_projection",
            "job_type": "create_projection",
            "projection": {"name": "proj_clients"},
        }

        processor.run(job_spec, context, mock_storage_client, mock_event_client)

        # Verify SQL was NOT executed
        mock_storage_client.execute_sql.assert_not_called()

        # Verify dry_run event was logged
        dry_run_call = mock_event_client.log_event.call_args_list[1]
        assert dry_run_call.kwargs["event_type"] == "projection.dry_run"

    def test_create_projection_logs_failure(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """CreateProjectionProcessor logs failure event on error."""
        mock_storage_client.execute_sql.side_effect = RuntimeError("BQ error")
        job_spec = {
            "job_id": "test_create_projection",
            "job_type": "create_projection",
            "projection": {"name": "proj_clients"},
        }

        with pytest.raises(RuntimeError, match="BQ error"):
            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

        # Verify failure event was logged
        failed_call = mock_event_client.log_event.call_args_list[-1]
        assert failed_call.kwargs["event_type"] == "projection.failed"
        assert failed_call.kwargs["status"] == "failed"
        assert "BQ error" in failed_call.kwargs["error_message"]

    def test_create_projection_unknown_projection_raises(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """CreateProjectionProcessor raises for unknown projection names."""
        job_spec = {
            "job_id": "test_create_projection",
            "job_type": "create_projection",
            "projection": {"name": "unknown_projection"},
        }

        with pytest.raises(KeyError, match="Unknown projection"):
            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)


class TestSyncSqliteProcessor:
    """Tests for SyncSqliteProcessor."""

    @pytest.fixture
    def processor(self):
        """Create a SyncSqliteProcessor instance."""
        return SyncSqliteProcessor()

    @pytest.fixture
    def mock_context(self, test_config):
        """Create a mock JobContext."""
        return JobContext(
            bq_client=MagicMock(),
            run_id="test-run-123",
            config=test_config,
            dry_run=False,
            test_table=False,
        )

    @pytest.fixture
    def mock_storage_client(self):
        """Create a mock StorageClient."""
        client = MagicMock()
        client.query_to_dataframe.return_value = [
            {"client_id": "c1", "session_id": "s1", "started_at": "2024-01-01"},
            {"client_id": "c2", "session_id": "s2", "started_at": "2024-01-02"},
        ]
        return client

    @pytest.fixture
    def mock_event_client(self):
        """Create a mock EventClient."""
        return MagicMock()

    def test_sync_sqlite_queries_bq_and_writes_sqlite(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """SyncSqliteProcessor queries BQ and writes to SQLite."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            job_spec = {
                "job_id": "test_sync",
                "job_type": "sync_sqlite",
                "source": {"projection": "proj_clients"},
                "sink": {"sqlite_path": str(sqlite_path), "table": "sessions"},
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            # Verify BQ was queried
            mock_storage_client.query_to_dataframe.assert_called_once()
            sql_arg = mock_storage_client.query_to_dataframe.call_args[0][0]
            assert "proj_clients" in sql_arg

            # Verify SQLite was written
            conn = sqlite3.connect(sqlite_path)
            cursor = conn.execute("SELECT * FROM sessions ORDER BY client_id")
            rows = cursor.fetchall()
            conn.close()

            assert len(rows) == 2
            assert rows[0][0] == "c1"  # client_id
            assert rows[1][0] == "c2"

    def test_sync_sqlite_logs_events(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """SyncSqliteProcessor logs started and completed events."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            job_spec = {
                "job_id": "test_sync",
                "job_type": "sync_sqlite",
                "source": {"projection": "proj_clients"},
                "sink": {"sqlite_path": str(sqlite_path), "table": "sessions"},
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            # Verify events were logged
            assert mock_event_client.log_event.call_count == 2

            # Check started event
            started_call = mock_event_client.log_event.call_args_list[0]
            assert started_call.kwargs["event_type"] == "sync.started"

            # Check completed event
            completed_call = mock_event_client.log_event.call_args_list[1]
            assert completed_call.kwargs["event_type"] == "sync.completed"
            assert completed_call.kwargs["payload"]["rows"] == 2

    def test_sync_sqlite_dry_run(
        self, processor, mock_storage_client, mock_event_client, test_config
    ):
        """SyncSqliteProcessor skips sync in dry run mode."""
        context = JobContext(
            bq_client=MagicMock(),
            run_id="test-run-123",
            config=test_config,
            dry_run=True,
            test_table=False,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            job_spec = {
                "job_id": "test_sync",
                "job_type": "sync_sqlite",
                "source": {"projection": "proj_clients"},
                "sink": {"sqlite_path": str(sqlite_path), "table": "sessions"},
            }

            processor.run(job_spec, context, mock_storage_client, mock_event_client)

            # Verify BQ was NOT queried
            mock_storage_client.query_to_dataframe.assert_not_called()

            # Verify dry_run event was logged
            dry_run_call = mock_event_client.log_event.call_args_list[1]
            assert dry_run_call.kwargs["event_type"] == "sync.dry_run"

    def test_sync_sqlite_handles_empty_results(
        self, processor, mock_context, mock_event_client
    ):
        """SyncSqliteProcessor handles empty BQ results gracefully."""
        storage_client = MagicMock()
        storage_client.query_to_dataframe.return_value = []

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            job_spec = {
                "job_id": "test_sync",
                "job_type": "sync_sqlite",
                "source": {"projection": "proj_clients"},
                "sink": {"sqlite_path": str(sqlite_path), "table": "sessions"},
            }

            processor.run(job_spec, mock_context, storage_client, mock_event_client)

            # Verify completed event with 0 rows
            completed_call = mock_event_client.log_event.call_args_list[-1]
            assert completed_call.kwargs["event_type"] == "sync.completed"
            assert completed_call.kwargs["payload"]["rows"] == 0

    def test_sync_sqlite_replaces_existing_data(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """SyncSqliteProcessor replaces existing data in SQLite."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"

            # Pre-populate SQLite with old data
            conn = sqlite3.connect(sqlite_path)
            conn.execute('CREATE TABLE sessions (client_id TEXT, session_id TEXT, started_at TEXT)')
            conn.execute("INSERT INTO sessions VALUES ('old_client', 'old_session', '2023-01-01')")
            conn.commit()
            conn.close()

            job_spec = {
                "job_id": "test_sync",
                "job_type": "sync_sqlite",
                "source": {"projection": "proj_clients"},
                "sink": {"sqlite_path": str(sqlite_path), "table": "sessions"},
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            # Verify old data was replaced
            conn = sqlite3.connect(sqlite_path)
            cursor = conn.execute("SELECT * FROM sessions ORDER BY client_id")
            rows = cursor.fetchall()
            conn.close()

            assert len(rows) == 2
            assert rows[0][0] == "c1"  # New data, not old_client


class TestFileProjectionProcessor:
    """Tests for FileProjectionProcessor."""

    @pytest.fixture
    def processor(self):
        """Create a FileProjectionProcessor instance."""
        return FileProjectionProcessor()

    @pytest.fixture
    def mock_context(self, test_config):
        """Create a mock JobContext."""
        return JobContext(
            bq_client=MagicMock(),
            run_id="test-run-123",
            config=test_config,
            dry_run=False,
            test_table=False,
        )

    @pytest.fixture
    def mock_storage_client(self):
        """Create a mock StorageClient."""
        return MagicMock()

    @pytest.fixture
    def mock_event_client(self):
        """Create a mock EventClient."""
        return MagicMock()

    def test_file_projection_renders_files(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """FileProjectionProcessor queries SQLite and renders markdown files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Set up SQLite with test data
            sqlite_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(sqlite_path)
            conn.execute('''
                CREATE TABLE sessions (
                    client_id TEXT,
                    session_id TEXT,
                    started_at TEXT,
                    summary TEXT
                )
            ''')
            conn.execute("INSERT INTO sessions VALUES ('c1', 's1', '2024-01-01', 'Summary 1')")
            conn.execute("INSERT INTO sessions VALUES ('c2', 's2', '2024-01-02', 'Summary 2')")
            conn.commit()
            conn.close()

            output_dir = Path(tmpdir) / "output"
            job_spec = {
                "job_id": "test_file_projection",
                "job_type": "file_projection",
                "source": {
                    "sqlite_path": str(sqlite_path),
                    "query": "SELECT * FROM sessions ORDER BY client_id",
                },
                "sink": {
                    "base_path": str(output_dir),
                    "path_template": "{client_id}/{session_id}.md",
                    "content_template": "# Session {session_id}\n\nDate: {started_at}\n\n{summary}",
                },
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            # Verify files were created
            file1 = output_dir / "c1" / "s1.md"
            file2 = output_dir / "c2" / "s2.md"

            assert file1.exists()
            assert file2.exists()

            content1 = file1.read_text()
            assert "# Session s1" in content1
            assert "Date: 2024-01-01" in content1
            assert "Summary 1" in content1

    def test_file_projection_logs_events(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """FileProjectionProcessor logs started and completed events."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(sqlite_path)
            conn.execute('CREATE TABLE sessions (client_id TEXT, summary TEXT)')
            conn.execute("INSERT INTO sessions VALUES ('c1', 'Summary')")
            conn.commit()
            conn.close()

            output_dir = Path(tmpdir) / "output"
            job_spec = {
                "job_id": "test_file_projection",
                "job_type": "file_projection",
                "source": {
                    "sqlite_path": str(sqlite_path),
                    "query": "SELECT * FROM sessions",
                },
                "sink": {
                    "base_path": str(output_dir),
                    "path_template": "{client_id}.md",
                    "content_template": "{summary}",
                },
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            # Verify events were logged
            assert mock_event_client.log_event.call_count == 2

            # Check started event
            started_call = mock_event_client.log_event.call_args_list[0]
            assert started_call.kwargs["event_type"] == "file_projection.started"

            # Check completed event
            completed_call = mock_event_client.log_event.call_args_list[1]
            assert completed_call.kwargs["event_type"] == "file_projection.completed"
            assert completed_call.kwargs["payload"]["files"] == 1

    def test_file_projection_dry_run(
        self, processor, mock_storage_client, mock_event_client, test_config
    ):
        """FileProjectionProcessor skips file creation in dry run mode."""
        context = JobContext(
            bq_client=MagicMock(),
            run_id="test-run-123",
            config=test_config,
            dry_run=True,
            test_table=False,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(sqlite_path)
            conn.execute('CREATE TABLE sessions (client_id TEXT)')
            conn.execute("INSERT INTO sessions VALUES ('c1')")
            conn.commit()
            conn.close()

            output_dir = Path(tmpdir) / "output"
            job_spec = {
                "job_id": "test_file_projection",
                "job_type": "file_projection",
                "source": {
                    "sqlite_path": str(sqlite_path),
                    "query": "SELECT * FROM sessions",
                },
                "sink": {
                    "base_path": str(output_dir),
                    "path_template": "{client_id}.md",
                    "content_template": "test",
                },
            }

            processor.run(job_spec, context, mock_storage_client, mock_event_client)

            # Verify no files were created
            assert not output_dir.exists()

            # Verify dry_run event was logged
            dry_run_call = mock_event_client.log_event.call_args_list[1]
            assert dry_run_call.kwargs["event_type"] == "file_projection.dry_run"

    def test_file_projection_handles_empty_results(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """FileProjectionProcessor handles empty SQLite results gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(sqlite_path)
            conn.execute('CREATE TABLE sessions (client_id TEXT)')
            conn.commit()
            conn.close()

            output_dir = Path(tmpdir) / "output"
            job_spec = {
                "job_id": "test_file_projection",
                "job_type": "file_projection",
                "source": {
                    "sqlite_path": str(sqlite_path),
                    "query": "SELECT * FROM sessions",
                },
                "sink": {
                    "base_path": str(output_dir),
                    "path_template": "{client_id}.md",
                    "content_template": "test",
                },
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            # Verify completed event with 0 files
            completed_call = mock_event_client.log_event.call_args_list[-1]
            assert completed_call.kwargs["event_type"] == "file_projection.completed"
            assert completed_call.kwargs["payload"]["files"] == 0

    def test_file_projection_creates_nested_directories(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """FileProjectionProcessor creates nested directory structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(sqlite_path)
            conn.execute('CREATE TABLE sessions (client_id TEXT, year TEXT, month TEXT)')
            conn.execute("INSERT INTO sessions VALUES ('c1', '2024', '01')")
            conn.commit()
            conn.close()

            output_dir = Path(tmpdir) / "output"
            job_spec = {
                "job_id": "test_file_projection",
                "job_type": "file_projection",
                "source": {
                    "sqlite_path": str(sqlite_path),
                    "query": "SELECT * FROM sessions",
                },
                "sink": {
                    "base_path": str(output_dir),
                    "path_template": "clients/{client_id}/{year}/{month}/session.md",
                    "content_template": "content",
                },
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            # Verify nested directory and file were created
            expected_file = output_dir / "clients" / "c1" / "2024" / "01" / "session.md"
            assert expected_file.exists()
            assert expected_file.read_text() == "content"

    def test_file_projection_renders_front_matter(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """FileProjectionProcessor renders YAML front matter when configured."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(sqlite_path)
            conn.execute('''
                CREATE TABLE transcripts (
                    client_id TEXT,
                    idem_key TEXT,
                    transcript_id TEXT,
                    content TEXT
                )
            ''')
            conn.execute(
                "INSERT INTO transcripts VALUES ('c1', 'key123', 't1', 'Hello world')"
            )
            conn.commit()
            conn.close()

            output_dir = Path(tmpdir) / "output"
            job_spec = {
                "job_id": "test_file_projection",
                "job_type": "file_projection",
                "source": {
                    "sqlite_path": str(sqlite_path),
                    "query": "SELECT * FROM transcripts",
                },
                "sink": {
                    "base_path": str(output_dir),
                    "path_template": "{client_id}/{transcript_id}.md",
                    "content_template": "{content}",
                    "front_matter": {
                        "source_type": "transcript",
                        "idem_key": "{idem_key}",
                        "transcript_id": "{transcript_id}",
                        "client_id": "{client_id}",
                    },
                },
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            # Verify file was created with front matter
            output_file = output_dir / "c1" / "t1.md"
            assert output_file.exists()

            content = output_file.read_text()
            lines = content.split("\n")

            # Verify front matter structure
            assert lines[0] == "---"
            # Find closing delimiter
            closing_idx = None
            for i, line in enumerate(lines[1:], 1):
                if line == "---":
                    closing_idx = i
                    break
            assert closing_idx is not None, "No closing --- found"

            # Parse front matter
            front_matter_yaml = "\n".join(lines[1:closing_idx])
            front_matter = yaml.safe_load(front_matter_yaml)

            assert front_matter["source_type"] == "transcript"
            assert front_matter["idem_key"] == "key123"
            assert front_matter["transcript_id"] == "t1"
            assert front_matter["client_id"] == "c1"

            # Verify blank line after front matter
            assert lines[closing_idx + 1] == ""

            # Verify content body
            assert "Hello world" in content

    def test_file_projection_no_front_matter_backward_compatible(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """FileProjectionProcessor produces identical output without front_matter config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(sqlite_path)
            conn.execute('CREATE TABLE docs (client_id TEXT, content TEXT)')
            conn.execute("INSERT INTO docs VALUES ('c1', 'Document content')")
            conn.commit()
            conn.close()

            output_dir = Path(tmpdir) / "output"
            job_spec = {
                "job_id": "test_file_projection",
                "job_type": "file_projection",
                "source": {
                    "sqlite_path": str(sqlite_path),
                    "query": "SELECT * FROM docs",
                },
                "sink": {
                    "base_path": str(output_dir),
                    "path_template": "{client_id}.md",
                    "content_template": "{content}",
                    # No front_matter config
                },
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            output_file = output_dir / "c1.md"
            content = output_file.read_text()

            # Content should be exactly the template output, no front matter
            assert content == "Document content"
            assert "---" not in content

    def test_file_projection_front_matter_handles_special_characters(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """FileProjectionProcessor handles special characters in front matter values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(sqlite_path)
            conn.execute('''
                CREATE TABLE docs (
                    client_id TEXT,
                    idem_key TEXT,
                    title TEXT,
                    content TEXT
                )
            ''')
            # Test various special characters
            conn.execute(
                "INSERT INTO docs VALUES (?, ?, ?, ?)",
                (
                    "c1",
                    "abc123#session_transcript",  # Contains #
                    'John "Johnny" Doe',  # Contains quotes
                    "Content here",
                ),
            )
            conn.execute(
                "INSERT INTO docs VALUES (?, ?, ?, ?)",
                (
                    "c2",
                    "foo:bar:baz",  # Contains colons
                    "José García",  # Unicode characters
                    "More content",
                ),
            )
            conn.commit()
            conn.close()

            output_dir = Path(tmpdir) / "output"
            job_spec = {
                "job_id": "test_file_projection",
                "job_type": "file_projection",
                "source": {
                    "sqlite_path": str(sqlite_path),
                    "query": "SELECT * FROM docs ORDER BY client_id",
                },
                "sink": {
                    "base_path": str(output_dir),
                    "path_template": "{client_id}.md",
                    "content_template": "{content}",
                    "front_matter": {
                        "idem_key": "{idem_key}",
                        "title": "{title}",
                    },
                },
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            # Verify first file (special chars: # and quotes)
            file1 = output_dir / "c1.md"
            content1 = file1.read_text()
            lines1 = content1.split("\n")
            assert lines1[0] == "---"
            closing_idx = lines1.index("---", 1)
            front_matter1 = yaml.safe_load("\n".join(lines1[1:closing_idx]))
            assert front_matter1["idem_key"] == "abc123#session_transcript"
            assert front_matter1["title"] == 'John "Johnny" Doe'

            # Verify second file (colons and unicode)
            file2 = output_dir / "c2.md"
            content2 = file2.read_text()
            lines2 = content2.split("\n")
            closing_idx2 = lines2.index("---", 1)
            front_matter2 = yaml.safe_load("\n".join(lines2[1:closing_idx2]))
            assert front_matter2["idem_key"] == "foo:bar:baz"
            assert front_matter2["title"] == "José García"

    def test_file_projection_front_matter_missing_key_raises_error(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """FileProjectionProcessor raises RuntimeError with helpful message on missing key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(sqlite_path)
            conn.execute('CREATE TABLE docs (client_id TEXT, content TEXT)')
            conn.execute("INSERT INTO docs VALUES ('c1', 'Content')")
            conn.commit()
            conn.close()

            output_dir = Path(tmpdir) / "output"
            job_spec = {
                "job_id": "test_missing_key_job",
                "job_type": "file_projection",
                "source": {
                    "sqlite_path": str(sqlite_path),
                    "query": "SELECT * FROM docs",
                },
                "sink": {
                    "base_path": str(output_dir),
                    "path_template": "{client_id}.md",
                    "content_template": "{content}",
                    "front_matter": {
                        "idem_key": "{nonexistent_key}",  # This key doesn't exist
                    },
                },
            }

            with pytest.raises(RuntimeError) as exc_info:
                processor.run(
                    job_spec, mock_context, mock_storage_client, mock_event_client
                )

            error_msg = str(exc_info.value)
            assert "nonexistent_key" in error_msg
            assert "test_missing_key_job" in error_msg
            assert "client_id" in error_msg  # Should list available keys
            assert "content" in error_msg

    def test_file_projection_front_matter_literal_values_preserved(
        self, processor, mock_context, mock_storage_client, mock_event_client
    ):
        """FileProjectionProcessor preserves literal (non-template) values in front matter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(sqlite_path)
            conn.execute('CREATE TABLE docs (client_id TEXT, content TEXT)')
            conn.execute("INSERT INTO docs VALUES ('c1', 'Content')")
            conn.commit()
            conn.close()

            output_dir = Path(tmpdir) / "output"
            job_spec = {
                "job_id": "test_file_projection",
                "job_type": "file_projection",
                "source": {
                    "sqlite_path": str(sqlite_path),
                    "query": "SELECT * FROM docs",
                },
                "sink": {
                    "base_path": str(output_dir),
                    "path_template": "{client_id}.md",
                    "content_template": "{content}",
                    "front_matter": {
                        "source_type": "contact",  # Literal string, no placeholder
                        "client_id": "{client_id}",  # Placeholder
                        "version": 1,  # Non-string literal (int)
                        "active": True,  # Non-string literal (bool)
                    },
                },
            }

            processor.run(job_spec, mock_context, mock_storage_client, mock_event_client)

            output_file = output_dir / "c1.md"
            content = output_file.read_text()
            lines = content.split("\n")
            closing_idx = lines.index("---", 1)
            front_matter = yaml.safe_load("\n".join(lines[1:closing_idx]))

            assert front_matter["source_type"] == "contact"
            assert front_matter["client_id"] == "c1"
            assert front_matter["version"] == 1
            assert front_matter["active"] is True
