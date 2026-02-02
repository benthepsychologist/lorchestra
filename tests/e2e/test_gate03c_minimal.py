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


def test_executor_native_call_returns_callable_result(monkeypatch):
    """Footgun: executor native call op must dispatch to callable and surface result."""
    from lorchestra.callable import register_callable
    from lorchestra.schemas import Op, StepManifest
    from lorchestra.executor import Executor
    from lorchestra.run_store import InMemoryRunStore

    def mock_callable(_params):
        return {
            "schema_version": "1.0",
            "items": [{"event_type": "x", "aggregate_type": "y", "aggregate_id": "z", "payload": {}}],
            "stats": {"input": 1, "output": 1, "skipped": 0, "errors": 0},
        }

    register_callable("workman", mock_callable)

    store = InMemoryRunStore()
    executor = Executor(store=store)

    manifest = StepManifest(
        run_id="run_e2e",
        step_id="step_1",
        backend="callable",
        op=Op.CALL,
        resolved_params={"callable": "workman", "op": "pm.project.create", "payload": {"name": "Test"}, "ctx": {}},
    )

    result = executor._handle_call(manifest)

    assert result is not None
    assert len(result["items"]) == 1
    assert result["stats"]["input"] == 1
