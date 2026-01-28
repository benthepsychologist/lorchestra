import json

import pytest


def test_job_language_has_no_jsonrpc_envelope_keys_in_schemas():
    """Epic footgun: job language must be envelope-free."""
    from lorchestra.schemas.job_def import JobDef
    from lorchestra.schemas.job_instance import JobInstance
    from lorchestra.schemas.step_manifest import StepManifest

    envelope_keys = {"jsonrpc", "method", "params", "id", "result", "error"}

    for cls in (JobDef, JobInstance, StepManifest):
        leaked = set(cls.__dataclass_fields__.keys()) & envelope_keys
        assert not leaked, f"{cls.__name__} leaked envelope keys: {leaked}"


def test_callable_result_xor_items_items_ref_and_serializable():
    """Epic contract: CallableResult must be planable and JSON serializable."""
    from lorchestra.callable.result import CallableResult

    r_items = CallableResult(items=[{"id": "1", "x": 1}], stats={})
    assert r_items.items is not None
    assert r_items.items_ref is None
    json.dumps(r_items.to_dict())

    r_ref = CallableResult(items_ref="artifact://example", stats={})
    assert r_ref.items is None
    assert r_ref.items_ref is not None
    json.dumps(r_ref.to_dict())

    with pytest.raises(ValueError):
        CallableResult(items=[{"id": "1"}], items_ref="artifact://both", stats={})

    with pytest.raises(ValueError):
        CallableResult(stats={})


def test_plan_builder_computes_deterministic_idempotency_key():
    """Epic contract: wal.append ops require deterministic idempotency_key."""
    from lorchestra.plan_builder import _compute_idempotency_key

    event1 = {
        "event_type": "project.created",
        "aggregate_type": "project",
        "aggregate_id": "proj_123",
        "idem_key": "pm.project.create:proj_123:corr_001",
        "payload": {"name": "A"},
    }
    event2 = {
        **event1,
        "payload": {"name": "B"},
    }

    k1 = _compute_idempotency_key(event1, "wal.append")
    k2 = _compute_idempotency_key(event2, "wal.append")

    assert k1.startswith("sha256:")
    assert k1 == k2


def test_callable_handler_submits_plan_via_storacle_boundary(monkeypatch):
    """Footgun: lorchestra should submit plans via storacle boundary abstraction."""
    from lorchestra.callable import register_callable
    from lorchestra.handlers.callable_handler import CallableHandler
    from lorchestra.schemas import Op, StepManifest

    def mock_callable(_params):
        return {
            "schema_version": "1.0",
            "items": [{"event_type": "x", "aggregate_type": "y", "aggregate_id": "z", "payload": {}}],
            "stats": {"input": 1, "output": 1, "skipped": 0, "errors": 0},
        }

    register_callable(Op.CALL_WORKMAN, mock_callable)

    submitted = []

    def capture_submit(plan, meta):
        submitted.append((plan, meta))
        return {"status": "captured", "ops_executed": len(plan.ops), "correlation_id": meta.correlation_id}

    monkeypatch.setattr("lorchestra.handlers.callable_handler.submit_plan", capture_submit)

    handler = CallableHandler()
    manifest = StepManifest(
        run_id="run_e2e",
        step_id="step_1",
        backend="callable",
        op=Op.CALL_WORKMAN,
        resolved_params={"op": "pm.project.create", "payload": {"name": "Test"}, "ctx": {}},
    )

    result = handler.execute(manifest)

    assert result is not None
    assert len(submitted) == 1
    plan, meta = submitted[0]
    assert len(plan.ops) == 1
    assert meta.correlation_id is not None
