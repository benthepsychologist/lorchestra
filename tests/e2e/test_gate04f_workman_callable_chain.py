"""Gate 04f: Validate workman callable chain through execute(envelope).

Confirms:
- Generic call op dispatches to workman callable by name
- workman.execute() returns CallableResult with domain event items
- plan_builder converts items to StoraclePlan with wal.append ops
- Each wal.append op carries a deterministic idempotency_key
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _reset_callable_registry():
    """Reset the callable dispatch registry so prior test mocks don't leak."""
    import lorchestra.callable.dispatch as _dispatch
    _dispatch._CALLABLES = None
    yield
    _dispatch._CALLABLES = None


def test_workman_dispatch_returns_callable_result():
    """call -> workman.execute() returns well-formed CallableResult."""
    from lorchestra.callable.dispatch import dispatch_callable

    params = {
        "op": "pm.project.create",
        "payload": {"name": "Gate 04f Test Project", "project_id": "proj_TEST_04F_001"},
        "ctx": {"producer": "lorchestra", "correlation_id": "gate_04f"},
    }
    result = dispatch_callable("workman", params)

    assert result.schema_version == "1.0"
    assert len(result.items) == 1
    assert result.stats["input"] == 1
    assert result.stats["output"] == 1

    item = result.items[0]
    assert item["event_type"] == "project.created"
    assert item["aggregate_type"] == "project"
    assert item["aggregate_id"] == "proj_TEST_04F_001"
    assert item["is_create"] is True
    assert "idempotency_key" in item
    assert item["idempotency_key"] == "lorchestra:pm.project.create:project:proj_TEST_04F_001:gate_04f"


def test_workman_plan_builder_wal_append_with_idempotency():
    """plan_builder converts workman items to wal.append ops with idempotency_key."""
    from lorchestra.callable.dispatch import dispatch_callable
    from lorchestra.plan_builder import build_plan

    params = {
        "op": "pm.project.create",
        "payload": {"name": "Idem Test", "project_id": "proj_IDEM_04F"},
        "ctx": {"producer": "lorchestra", "correlation_id": "idem_04f"},
    }
    result = dispatch_callable("workman", params)
    plan = build_plan(result, correlation_id="idem_04f")

    assert plan.to_dict()["plan_version"] == "storacle.plan/1.0.0"
    assert plan.correlation_id == "idem_04f"
    assert len(plan.ops) == 1

    op = plan.ops[0]
    assert op.method == "wal.append"
    assert op.idempotency_key is not None
    assert op.idempotency_key.startswith("sha256:")
    assert len(op.idempotency_key) > 10

    # Item-level idempotency_key from workman is preserved in params
    assert op.params["idempotency_key"] == "lorchestra:pm.project.create:project:proj_IDEM_04F:idem_04f"


def test_workman_idempotency_key_deterministic():
    """Same input to workman produces identical idempotency keys."""
    from lorchestra.callable.dispatch import dispatch_callable
    from lorchestra.plan_builder import build_plan

    params = {
        "op": "pm.project.create",
        "payload": {"name": "Determinism Check", "project_id": "proj_DET_04F"},
        "ctx": {"producer": "lorchestra", "correlation_id": "det_04f"},
    }

    result1 = dispatch_callable("workman", params)
    plan1 = build_plan(result1, correlation_id="det_04f")

    result2 = dispatch_callable("workman", params)
    plan2 = build_plan(result2, correlation_id="det_04f")

    # Item-level keys match
    assert result1.items[0]["idempotency_key"] == result2.items[0]["idempotency_key"]

    # Plan-op-level keys match
    assert plan1.ops[0].idempotency_key == plan2.ops[0].idempotency_key


def test_workman_job_def_compiles():
    """workman_create_project.yaml compiles via compile(envelope)."""
    from pathlib import Path
    from lorchestra.executor import compile as compile_envelope

    definitions_dir = Path(__file__).resolve().parents[2] / "lorchestra" / "jobs" / "definitions"
    envelope = {
        "job_id": "workman_create_project",
        "ctx": {},
        "payload": {"name": "Compile Test Project"},
        "definitions_dir": definitions_dir,
    }
    instance = compile_envelope(envelope)

    assert instance.job_id == "workman_create_project"
    assert len(instance.steps) == 3  # call, plan.build, storacle.submit

    step = instance.steps[0]
    assert step.op.value == "call"
    assert step.params["callable"] == "workman"
    assert step.params["op"] == "pm.project.create"
    assert step.params["payload"]["name"] == "Compile Test Project"
    assert step.params["ctx"]["producer"] == "lorchestra"
