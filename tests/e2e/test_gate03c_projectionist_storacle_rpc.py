from __future__ import annotations


def test_gate03c_projectionist_uses_storacle_rpc_dry_run():
    """Gate 03c: lorchestra projectionist processor calls storacle via JSON-RPC plan.

    This uses the *current* e005b implementation:
    - lorchestra builds a `storacle.plan/1.0.0` dict (with JSON-RPC envelope fields)
    - lorchestra calls `storacle.rpc.execute_plan(plan, dry_run=True)`

    We choose a dry-run `sheets.write_table` op so the test is hermetic and does
    not require credentials.
    """

    from lorchestra.config import LorchestraConfig
    from lorchestra.processors.base import JobContext

    # Import module (not just class) so we can register a custom build_plan.
    from lorchestra.processors import projectionist as projectionist_module

    import storacle.jsonrpc as storacle_jsonrpc

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

    captured: dict = {}
    original_handle_request = storacle_jsonrpc.handle_request

    def _capturing_handle_request(request: dict, *, dry_run: bool = False) -> dict:
        captured["request"] = request
        captured["dry_run"] = dry_run
        return original_handle_request(request, dry_run=dry_run)

    # Prove the boundary is exercised via JSON-RPC envelope.
    storacle_jsonrpc.handle_request = _capturing_handle_request  # type: ignore[assignment]

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

    processor.run(
        job_spec,
        ctx,
        _NoopStorageClient(),
        events,
    )

    req = captured.get("request")
    assert isinstance(req, dict)
    assert req.get("jsonrpc") == "2.0"
    assert req.get("method") == "storacle.execute_plan"
    assert isinstance(req.get("params"), dict)
    assert isinstance(req["params"].get("payload"), dict)
    assert captured.get("dry_run") is True

    event_types = [e["event_type"] for e in events.events]
    assert "projection.started" in event_types
    assert "projection.completed" in event_types

    completed = next(e for e in events.events if e["event_type"] == "projection.completed")
    responses = (completed.get("payload") or {}).get("responses")

    assert isinstance(responses, list)
    assert len(responses) == 1
    assert responses[0]["jsonrpc"] == "2.0"
    assert responses[0]["id"] == "op_1"
    assert "result" in responses[0]

    result = responses[0]["result"]
    assert result.get("dry_run") is True
    assert result.get("rows_written") == 1
