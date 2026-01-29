from __future__ import annotations

import warnings


def test_gate03c_projectionist_uses_storacle_rpc_dry_run():
    """Gate 03c→04e: projectionist processor routes through lorchestra.storacle boundary.

    Originally (03c) this tested the direct storacle.jsonrpc.handle_request import.
    After 04e, the deprecated ProjectionistProcessor routes through
    lorchestra.storacle.submit_plan instead. This test verifies:
    - The processor still runs (backwards compat)
    - It emits a deprecation warning
    - It logs started/completed events
    - Dry run produces noop responses
    """

    from lorchestra.config import LorchestraConfig
    from lorchestra.processors.base import JobContext

    # Import module (not just class) so we can register a custom build_plan.
    from lorchestra.processors import projectionist as projectionist_module

    class _NoopStorageClient:
        def query_to_dataframe(self, sql: str):  # pragma: no cover
            raise AssertionError(f"StorageClient should not be used in this test. sql={sql!r}")

    class _CapturingEventClient:
        def __init__(self) -> None:
            self.events: list[dict] = []

        def log_event(
            self,
            event_type: str,
            source_system: str,
            correlation_id: str,
            status: str,
            connection_name: str | None = None,
            target_object_type: str | None = None,
            payload: dict | None = None,
            error_message: str | None = None,
        ) -> None:
            self.events.append(
                {
                    "event_type": event_type,
                    "source_system": source_system,
                    "correlation_id": correlation_id,
                    "status": status,
                    "connection_name": connection_name,
                    "target_object_type": target_object_type,
                    "payload": payload,
                    "error_message": error_message,
                }
            )

    def _build_plan(_ctx, _config, *, bq):
        # bq is present by signature but intentionally unused in this test.
        _ = bq
        return {
            "plan_version": "storacle.plan/1.0.0",
            "plan_id": "plan_test_projectionist_dry_run_0001",
            "jsonrpc": "2.0",
            "meta": {"account": "gdrive"},
            "ops": [
                {
                    "jsonrpc": "2.0",
                    "id": "op_1",
                    "method": "sheets.write_table",
                    "params": {
                        "spreadsheet_id": "spreadsheet_test",
                        "sheet_name": "sheet_test",
                        "strategy": "replace",
                        "values": [["col"], ["value"]],
                    },
                }
            ],
        }

    # Register a test-only projection plan builder.
    projectionist_module.PROJECTION_BUILD_PLAN_REGISTRY["test_sheets"] = _build_plan

    cfg = LorchestraConfig(
        project="test-project",
        dataset_raw="test_raw",
        dataset_canonical="test_canonical",
        dataset_derived="test_derived",
        sqlite_path="/tmp/test.db",
        local_views_root="/tmp/views",
    )

    ctx = JobContext(
        bq_client=object(),
        run_id="run_test_0001",
        config=cfg,
        dry_run=True,
    )

    job_spec = {
        "job_id": "job_test_0001",
        "job_type": "projectionist",
        "projection_name": "test_sheets",
        "projection_config": {"account": "gdrive"},
        "source_system": "lorchestra",
    }

    processor = projectionist_module.ProjectionistProcessor()
    events = _CapturingEventClient()

    # Processor should emit deprecation warning
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        processor.run(
            job_spec,
            ctx,
            _NoopStorageClient(),
            events,
        )
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) >= 1
        assert "deprecated" in str(deprecation_warnings[0].message).lower()

    # Verify events were logged
    event_types = [e["event_type"] for e in events.events]
    assert "projection.started" in event_types
    assert "projection.completed" in event_types

    # Verify completed event has responses (dry run noop)
    completed = next(e for e in events.events if e["event_type"] == "projection.completed")
    responses = (completed.get("payload") or {}).get("responses")
    assert isinstance(responses, list)
    assert len(responses) == 1
