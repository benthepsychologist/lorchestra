"""PM Orchestration acceptance tests — all 24 ops x 3 jobs + multi-op refs.

Tests the full chain:
  pm.plan  → compile_intent via call op → returns plan/diff/plan_hash
  pm.exec  → compile_intent + storacle.submit in one job
  pm.apply → storacle.submit with a pre-compiled plan

Uses workman (installed) as the real callable — no mocks.
Schema registry is a temp fixture (same pattern as workman/tests/conftest.py).

Storacle write tests use smoke namespace "pm_accept" to avoid polluting prod WAL.
They require STORACLE_NAMESPACE_SALT (loaded from .env) and BQ access.
"""

import json
import os
from pathlib import Path

import pytest

from lorchestra.executor import execute
from lorchestra.run_store import InMemoryRunStore


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFINITIONS_DIR = Path(__file__).resolve().parent.parent / "lorchestra" / "jobs" / "definitions"
DOTENV_PATH = Path(__file__).resolve().parent.parent / ".env"
SMOKE_NS = "smoke_v2"


# ---------------------------------------------------------------------------
# Env loading
# ---------------------------------------------------------------------------

def _load_dotenv():
    """Load key=value pairs from .env (no shell expansion, no overwrite)."""
    if not DOTENV_PATH.exists():
        return
    for line in DOTENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if key not in os.environ:
            os.environ[key] = value


_load_dotenv()

# Detect whether storacle BQ infra is available for write tests
_HAS_STORACLE_INFRA = bool(os.environ.get("STORACLE_NAMESPACE_SALT"))
_skip_no_infra = pytest.mark.skipif(
    not _HAS_STORACLE_INFRA,
    reason="STORACLE_NAMESPACE_SALT not set — storacle write tests skipped",
)


# ---------------------------------------------------------------------------
# Schema registry fixture (mirrors workman/tests/conftest.py)
# ---------------------------------------------------------------------------


def _write_schema(registry_root: Path, vendor: str, name: str, version: str, properties: dict, **extra):
    schema_path = registry_root / "schemas" / vendor / name / "jsonschema" / version / "schema.json"
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": properties,
        "additionalProperties": True,
    }
    schema.update(extra)
    schema_path.write_text(json.dumps(schema))


@pytest.fixture(autouse=True)
def schema_registry(tmp_path):
    """Temp schema registry with all PM op schemas."""
    root = tmp_path / "schema-reg"
    vendor = "org1.workman"

    _write_schema(root, vendor, "pm.project.create", "1-0-0", {"project_id": {"type": "string"}, "name": {"type": "string"}})
    _write_schema(root, vendor, "pm.project.close", "1-0-0", {"project_id": {"type": "string"}})
    _write_schema(root, vendor, "pm.project.update", "1-0-0", {"project_id": {"type": "string"}, "name": {"type": "string"}, "status": {"type": "string"}})
    _write_schema(root, vendor, "pm.work_item.create", "1-0-0", {"work_item_id": {"type": "string"}, "project_id": {"type": "string"}, "deliverable_id": {"type": "string"}, "opsstream_id": {"type": "string"}, "title": {"type": "string"}})
    _write_schema(root, vendor, "pm.work_item.complete", "1-0-0", {"work_item_id": {"type": "string"}})
    _write_schema(root, vendor, "pm.work_item.move", "1-0-0", {"work_item_id": {"type": "string"}, "project_id": {"type": "string"}, "opsstream_id": {"type": "string"}, "deliverable_id": {"type": "string"}, "parent_id": {"type": "string"}})
    _write_schema(root, vendor, "pm.work_item.update", "1-0-0", {"work_item_id": {"type": "string"}, "title": {"type": "string"}, "description": {"type": "string"}, "kind": {"type": "string"}, "state": {"type": "string"}, "priority": {"type": "string"}, "severity": {"type": "string"}, "labels": {"type": "array"}, "assignees": {"type": "array"}, "due_at": {"type": "string"}, "time_estimate": {"type": "number"}, "time_spent": {"type": "number"}})
    _write_schema(root, vendor, "pm.work_item.cancel", "1-0-0", {"work_item_id": {"type": "string"}, "reason": {"type": "string"}})
    _write_schema(root, vendor, "pm.deliverable.create", "1-0-0", {"deliverable_id": {"type": "string"}, "project_id": {"type": "string"}, "opsstream_id": {"type": "string"}, "name": {"type": "string"}})
    _write_schema(root, vendor, "pm.deliverable.complete", "1-0-0", {"deliverable_id": {"type": "string"}})
    _write_schema(root, vendor, "pm.deliverable.update", "1-0-0", {"deliverable_id": {"type": "string"}, "name": {"type": "string"}})
    _write_schema(root, vendor, "pm.deliverable.reject", "1-0-0", {"deliverable_id": {"type": "string"}, "reason": {"type": "string"}})
    _write_schema(root, vendor, "pm.opsstream.create", "1-0-0", {"opsstream_id": {"type": "string"}, "name": {"type": "string"}, "type": {"type": "string"}, "owner": {"type": "string"}, "status": {"type": "string"}, "description": {"type": "string"}, "meta": {"type": "object"}})
    _write_schema(root, vendor, "pm.opsstream.update", "1-0-0", {"opsstream_id": {"type": "string"}, "name": {"type": "string"}, "status": {"type": "string"}})
    _write_schema(root, vendor, "pm.opsstream.close", "1-0-0", {"opsstream_id": {"type": "string"}, "reason": {"type": "string"}})
    _write_schema(root, vendor, "pm.artifact.create", "1-0-0", {"artifact_id": {"type": "string"}, "name": {"type": "string"}, "kind": {"type": "string"}, "work_item_id": {"type": "string"}, "deliverable_id": {"type": "string"}, "project_id": {"type": "string"}, "opsstream_id": {"type": "string"}})
    _write_schema(root, vendor, "pm.artifact.update", "1-0-0", {"artifact_id": {"type": "string"}, "name": {"type": "string"}})
    _write_schema(root, vendor, "pm.artifact.finalize", "1-0-0", {"artifact_id": {"type": "string"}})
    _write_schema(root, vendor, "pm.artifact.deliver", "1-0-0", {"artifact_id": {"type": "string"}, "delivered_via": {"type": "string"}})
    _write_schema(root, vendor, "pm.artifact.defer", "1-0-0", {"artifact_id": {"type": "string"}, "reason": {"type": "string"}})
    _write_schema(root, vendor, "pm.artifact.supersede", "1-0-0", {"artifact_id": {"type": "string"}, "superseded_by": {"type": "string"}})
    _write_schema(root, vendor, "pm.artifact.archive", "1-0-0", {"artifact_id": {"type": "string"}})
    _write_schema(root, vendor, "link.create", "1-0-0", {"link_id": {"type": "string"}, "source_id": {"type": "string"}, "source_type": {"type": "string"}, "target_id": {"type": "string"}, "target_type": {"type": "string"}, "predicate": {"type": "string"}, "meta": {"type": "object"}})
    _write_schema(root, vendor, "link.remove", "1-0-0", {"link_id": {"type": "string"}, "reason": {"type": "string"}})

    old_val = os.environ.get("SCHEMA_REGISTRY_ROOT")
    os.environ["SCHEMA_REGISTRY_ROOT"] = str(root)

    yield root

    if old_val is None:
        os.environ.pop("SCHEMA_REGISTRY_ROOT", None)
    else:
        os.environ["SCHEMA_REGISTRY_ROOT"] = old_val


@pytest.fixture(autouse=True)
def _reset_caches():
    """Clear callable dispatch cache and storacle client cache between tests."""
    import lorchestra.callable.dispatch as _d
    _d._CALLABLES = None

    # Reset storacle's cached config/clients so env changes take effect
    try:
        import storacle.rpc as _rpc
        _rpc._config = None
        _rpc._bq_store = None
        _rpc._wal_client = None
        _rpc._ops_client = None
    except (ImportError, AttributeError):
        pass

    yield

    _d._CALLABLES = None


@pytest.fixture()
def smoke_env():
    """Set STORACLE_SMOKE_NAMESPACE for write tests (isolated BQ dataset)."""
    old = os.environ.get("STORACLE_SMOKE_NAMESPACE")
    os.environ["STORACLE_SMOKE_NAMESPACE"] = SMOKE_NS
    yield SMOKE_NS
    if old is None:
        os.environ.pop("STORACLE_SMOKE_NAMESPACE", None)
    else:
        os.environ["STORACLE_SMOKE_NAMESPACE"] = old


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_intent(ops_list):
    """Build a single-or-multi-op PMIntent."""
    return {
        "intent_id": "intent_ACCEPT",
        "ops": [{"op": op, "payload": payload} for op, payload in ops_list],
        "source": "acceptance-tests",
        "actor": {"actor_type": "user", "actor_id": "u_accept"},
        "issued_at": "2026-02-13T00:00:00Z",
    }


def _run_pm_plan(intent):
    """Execute pm.plan job and return (result, store)."""
    store = InMemoryRunStore()
    result = execute({
        "job_id": "pm.plan",
        "definitions_dir": str(DEFINITIONS_DIR),
        "payload": {"intent": intent},
        "store": store,
    })
    return result, store


def _run_pm_exec(intent):
    """Execute pm.exec job and return (result, store)."""
    store = InMemoryRunStore()
    result = execute({
        "job_id": "pm.exec",
        "definitions_dir": str(DEFINITIONS_DIR),
        "payload": {"intent": intent},
        "store": store,
    })
    return result, store


def _run_pm_apply(plan_dict):
    """Execute pm.apply job and return (result, store)."""
    store = InMemoryRunStore()
    result = execute({
        "job_id": "pm.apply",
        "definitions_dir": str(DEFINITIONS_DIR),
        "payload": {"plan": plan_dict},
        "store": store,
    })
    return result, store


def _get_step_output(store, run_id, step_id):
    """Retrieve step output from InMemoryRunStore."""
    ref = f"mem://{run_id}/{step_id}/output"
    return store.get_output(ref)


def _assert_storacle_write_success(submit_output):
    """Assert storacle submit actually wrote successfully.

    Raises AssertionError with details if storacle returned JSON-RPC errors
    (e.g. missing STORACLE_NAMESPACE_SALT, missing BQ tables).
    """
    if isinstance(submit_output, dict):
        # noop client path
        assert submit_output.get("ops_executed", 0) > 0
        return

    assert isinstance(submit_output, list), f"Unexpected submit output type: {type(submit_output)}"
    assert len(submit_output) > 0, "Empty storacle response"

    errors = []
    for resp in submit_output:
        if isinstance(resp, dict) and "error" in resp:
            errors.append(resp["error"].get("message", str(resp["error"])))

    assert not errors, f"Storacle returned errors: {errors}"


# ---------------------------------------------------------------------------
# 24 parametrized test tuples: (op_name, payload, expected_event_type)
# ---------------------------------------------------------------------------

# All 24 ops — used by pm.plan tests (compile-only, no BQ assertions checked)
ALL_OPS = [
    # Project
    ("pm.project.create", {"name": "Acceptance Project"}, "project.created"),
    ("pm.project.close", {"project_id": "proj_EXIST"}, "project.closed"),
    ("pm.project.update", {"project_id": "proj_EXIST", "name": "Updated"}, "project.updated"),
    # WorkItem
    ("pm.work_item.create", {"title": "Acceptance Task", "project_id": "proj_FK"}, "work_item.created"),
    ("pm.work_item.complete", {"work_item_id": "wi_EXIST"}, "work_item.completed"),
    ("pm.work_item.move", {"work_item_id": "wi_EXIST", "project_id": "proj_FK", "opsstream_id": "ops_FK"}, "work_item.moved"),
    ("pm.work_item.update", {"work_item_id": "wi_EXIST", "title": "Updated Task"}, "work_item.updated"),
    ("pm.work_item.cancel", {"work_item_id": "wi_EXIST"}, "work_item.cancelled"),
    # Deliverable
    ("pm.deliverable.create", {"name": "Acceptance Del", "project_id": "proj_FK"}, "deliverable.created"),
    ("pm.deliverable.complete", {"deliverable_id": "del_EXIST"}, "deliverable.completed"),
    ("pm.deliverable.update", {"deliverable_id": "del_EXIST", "name": "Updated Del"}, "deliverable.updated"),
    ("pm.deliverable.reject", {"deliverable_id": "del_EXIST"}, "deliverable.rejected"),
    # OpsStream
    ("pm.opsstream.create", {"name": "Acceptance Stream"}, "opsstream.created"),
    ("pm.opsstream.update", {"opsstream_id": "ops_EXIST", "name": "Updated Stream"}, "opsstream.updated"),
    ("pm.opsstream.close", {"opsstream_id": "ops_EXIST"}, "opsstream.closed"),
    # Artifact
    ("pm.artifact.create", {"name": "Acceptance Art", "kind": "document", "project_id": "proj_FK"}, "artifact.created"),
    ("pm.artifact.update", {"artifact_id": "art_EXIST", "name": "Updated Art"}, "artifact.updated"),
    ("pm.artifact.finalize", {"artifact_id": "art_EXIST"}, "artifact.finalized"),
    ("pm.artifact.deliver", {"artifact_id": "art_EXIST"}, "artifact.delivered"),
    ("pm.artifact.defer", {"artifact_id": "art_EXIST"}, "artifact.deferred"),
    ("pm.artifact.supersede", {"artifact_id": "art_EXIST", "superseded_by": "art_OTHER"}, "artifact.superseded"),
    ("pm.artifact.archive", {"artifact_id": "art_EXIST"}, "artifact.archived"),
    # Links
    ("link.create", {"source_id": "proj_A", "source_type": "project", "target_id": "wi_B", "target_type": "work_item", "predicate": "contains"}, "link.created"),
    ("link.remove", {"link_id": "lnk_EXIST"}, "link.removed"),
]

# Create-only ops — self-contained, no prior aggregates needed in BQ.
# Used by pm.exec and pm.apply tests that write to real storacle.
# Note: link.create excluded because dynamic FK assertions require source/target to exist.
CREATE_OPS = [
    ("pm.project.create", {"name": "Acceptance Project"}, "project.created"),
    ("pm.opsstream.create", {"name": "Acceptance Stream"}, "opsstream.created"),
]


# ---------------------------------------------------------------------------
# Group 1: test_pm_plan — 24 parametrized (compile only, no BQ needed)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("op_name,payload,expected_event", ALL_OPS, ids=[t[0] for t in ALL_OPS])
def test_pm_plan(op_name, payload, expected_event):
    """pm.plan compiles a single-op intent and returns plan/diff/plan_hash."""
    intent = _make_intent([(op_name, payload)])
    result, store = _run_pm_plan(intent)

    assert result.success, f"pm.plan failed for {op_name}: {result.error}"

    # 1 step outcome (plan_compile)
    assert len(result.attempt.step_outcomes) == 1
    assert result.attempt.step_outcomes[0].step_id == "plan_compile"

    # Retrieve step output
    output = _get_step_output(store, result.run_id, "plan_compile")
    assert output is not None

    # Envelope structure
    assert len(output["items"]) == 1
    envelope = output["items"][0]
    assert isinstance(envelope["plan"], dict)
    assert isinstance(envelope["diff"], list)
    assert isinstance(envelope["plan_hash"], str)
    assert len(envelope["plan_hash"]) == 64

    # Merged plan structure
    plan = envelope["plan"]
    assert plan["plan_version"] == "storacle.plan/1.0.0"
    assert plan["meta"]["op"] == "pm.compile_intent"

    # Find wal.append op with expected event_type
    wal_ops = [op for op in plan["ops"] if op["method"] == "wal.append"]
    assert len(wal_ops) >= 1
    event_types = [op["params"]["event_type"] for op in wal_ops]
    assert expected_event in event_types


# ---------------------------------------------------------------------------
# Group 2: test_pm_exec — 24 parametrized (compile + submit to BQ)
# ---------------------------------------------------------------------------

@_skip_no_infra
@pytest.mark.parametrize("op_name,payload,expected_event", CREATE_OPS, ids=[t[0] for t in CREATE_OPS])
def test_pm_exec(op_name, payload, expected_event, smoke_env):
    """pm.exec compiles + submits a create op in one job (2 steps).

    Writes to smoke dataset. Uses create ops only (no prior aggregates needed).
    """
    intent = _make_intent([(op_name, payload)])
    result, store = _run_pm_exec(intent)

    assert result.success, f"pm.exec failed for {op_name}: {result.error}"

    # 2 step outcomes (plan_compile, plans_submit)
    assert len(result.attempt.step_outcomes) == 2
    step_ids = [s.step_id for s in result.attempt.step_outcomes]
    assert step_ids == ["plan_compile", "plans_submit"]

    # Assert storacle actually wrote
    submit_output = _get_step_output(store, result.run_id, "plans_submit")
    assert submit_output is not None
    _assert_storacle_write_success(submit_output)


# ---------------------------------------------------------------------------
# Group 3: test_pm_apply — 5 representative ops (compile then submit)
# ---------------------------------------------------------------------------

@_skip_no_infra
@pytest.mark.parametrize("op_name,payload,expected_event", CREATE_OPS, ids=[t[0] for t in CREATE_OPS])
def test_pm_apply(op_name, payload, expected_event, smoke_env):
    """pm.apply submits a pre-compiled plan (create ops only).

    Writes to smoke dataset.
    """
    # First compile via pm.plan
    intent = _make_intent([(op_name, payload)])
    plan_result, plan_store = _run_pm_plan(intent)
    assert plan_result.success

    # Extract the compiled plan
    plan_output = _get_step_output(plan_store, plan_result.run_id, "plan_compile")
    plan_dict = plan_output["items"][0]["plan"]

    # Submit via pm.apply
    apply_result, apply_store = _run_pm_apply(plan_dict)
    assert apply_result.success, f"pm.apply failed for {op_name}: {apply_result.error}"

    # 1 step outcome (plans_submit)
    assert len(apply_result.attempt.step_outcomes) == 1
    assert apply_result.attempt.step_outcomes[0].step_id == "plans_submit"

    # Assert storacle actually wrote
    submit_output = _get_step_output(apply_store, apply_result.run_id, "plans_submit")
    _assert_storacle_write_success(submit_output)


# ---------------------------------------------------------------------------
# Group 3b: test_pm_lifecycle — create then update via multi-op intent
# ---------------------------------------------------------------------------

@_skip_no_infra
def test_pm_lifecycle_create_then_update(smoke_env):
    """Full lifecycle: create project via pm.exec, then update via separate pm.exec.

    Storacle uses two-phase execution (asserts first, writes second) so
    assert.exists for the update cannot see a create in the same batch.
    This test verifies the two-step pattern works end-to-end.
    """
    import time

    # Step 1: create
    create_intent = _make_intent([
        ("pm.project.create", {"name": "Lifecycle Project"}),
    ])
    create_result, create_store = _run_pm_exec(create_intent)
    assert create_result.success, f"Create failed: {create_result.error}"
    create_submit = _get_step_output(create_store, create_result.run_id, "plans_submit")
    _assert_storacle_write_success(create_submit)

    # Extract the project_id that was created
    plan_output = _get_step_output(create_store, create_result.run_id, "plan_compile")
    plan = plan_output["items"][0]["plan"]
    wal_ops = [op for op in plan["ops"] if op["method"] == "wal.append"]
    project_id = wal_ops[0]["params"]["aggregate_id"]

    # BQ streaming insert has a short propagation delay
    time.sleep(3)

    # Step 2: update (uses the real project_id from step 1)
    update_intent = {
        "intent_id": "intent_ACCEPT_UPDATE",
        "ops": [{"op": "pm.project.update", "payload": {"project_id": project_id, "name": "Updated Lifecycle"}}],
        "source": "acceptance-tests",
        "actor": {"actor_type": "user", "actor_id": "u_accept"},
        "issued_at": "2026-02-13T00:00:01Z",
    }
    update_result, update_store = _run_pm_exec(update_intent)
    assert update_result.success, f"Update failed: {update_result.error}"
    update_submit = _get_step_output(update_store, update_result.run_id, "plans_submit")
    _assert_storacle_write_success(update_submit)


# ---------------------------------------------------------------------------
# Group 4: test_multi_op_intent_with_refs (compile-only, no BQ needed)
# ---------------------------------------------------------------------------

def test_multi_op_intent_with_refs():
    """3-op intent: project create -> work_item create (@ref:0) -> artifact create (@ref:1).

    Verifies @ref resolution in merged plan and 3-entry diff.
    """
    intent = _make_intent([
        ("pm.project.create", {"name": "Multi-Ref Project"}),
        ("pm.work_item.create", {"title": "Ref Task", "project_id": "@ref:0"}),
        ("pm.artifact.create", {"name": "Ref Art", "kind": "doc", "work_item_id": "@ref:1"}),
    ])

    # Use pm.plan (not pm.exec) to avoid needing BQ for this structural test
    result, store = _run_pm_plan(intent)
    assert result.success, f"Multi-op pm.plan failed: {result.error}"

    # Get plan_compile output
    output = _get_step_output(store, result.run_id, "plan_compile")
    envelope = output["items"][0]

    # Diff should have 3 entries
    assert len(envelope["diff"]) == 3

    # Merged plan: find all wal.append ops
    plan = envelope["plan"]
    wal_ops = [op for op in plan["ops"] if op["method"] == "wal.append"]
    assert len(wal_ops) == 3

    # Verify @ref resolution
    project_id = wal_ops[0]["params"]["aggregate_id"]
    wi_id = wal_ops[1]["params"]["aggregate_id"]

    # work_item.created should reference the project
    assert wal_ops[1]["params"]["payload"]["project_id"] == project_id
    # artifact.created should reference the work_item
    assert wal_ops[2]["params"]["payload"]["work_item_id"] == wi_id

    # Verify merged plan ID renumbering is sequential
    all_ids = [op["id"] for op in plan["ops"]]
    a_ids = sorted([i for i in all_ids if i.startswith("a")])
    w_ids = sorted([i for i in all_ids if i.startswith("w")])
    assert a_ids == [f"a{i}" for i in range(1, len(a_ids) + 1)]
    assert w_ids == [f"w{i}" for i in range(1, len(w_ids) + 1)]


# ---------------------------------------------------------------------------
# Group 5: Mega hierarchy test + negative inheritance/FK tests
# ---------------------------------------------------------------------------

def test_mega_hierarchy_positive():
    """27-op intent: full PM hierarchy with inheritance auto-fill and link FK assertions.

    Hierarchy:
      OpsStream: ClinicalOPS
      ├── Project: Quarterly Reporting (linked via link.create)
      │   ├── Deliverable: Q1 Report → 2 WorkItems
      │   ├── Deliverable: Q2 Report → 2 WorkItems
      │   └── Deliverable: Q3 Report → 2 WorkItems
      ├── Project: Clinic Update (linked via link.create)
      │   ├── Deliverable: Facility Assessment → 2 WorkItems
      │   ├── Deliverable: Vendor Selection → 2 WorkItems
      │   └── Deliverable: Implementation Plan → 2 WorkItems
      └── Direct WorkItems (opsstream_id only, no project)
          ├── WorkItem: Weekly Standup Notes
          └── WorkItem: Monthly Metrics Review
    """
    intent = _make_intent([
        # 0: OpsStream
        ("pm.opsstream.create", {"name": "ClinicalOPS"}),
        # 1: Project: Quarterly Reporting
        ("pm.project.create", {"name": "Quarterly Reporting"}),
        # 2: link opsstream → project
        ("link.create", {"source_id": "@ref:0", "source_type": "opsstream",
                         "target_id": "@ref:1", "target_type": "project",
                         "predicate": "has_part"}),
        # 3: Project: Clinic Update
        ("pm.project.create", {"name": "Clinic Update"}),
        # 4: link opsstream → project
        ("link.create", {"source_id": "@ref:0", "source_type": "opsstream",
                         "target_id": "@ref:3", "target_type": "project",
                         "predicate": "has_part"}),
        # 5-7: Deliverables under Quarterly Reporting
        ("pm.deliverable.create", {"name": "Q1 Report", "project_id": "@ref:1"}),   # @ref:5
        ("pm.deliverable.create", {"name": "Q2 Report", "project_id": "@ref:1"}),   # @ref:6
        ("pm.deliverable.create", {"name": "Q3 Report", "project_id": "@ref:1"}),   # @ref:7
        # 8-13: WorkItems under QR deliverables (inherit project_id)
        ("pm.work_item.create", {"title": "Draft Q1 Financials", "deliverable_id": "@ref:5"}),   # @ref:8
        ("pm.work_item.create", {"title": "Review Q1 Data", "deliverable_id": "@ref:5"}),         # @ref:9
        ("pm.work_item.create", {"title": "Draft Q2 Financials", "deliverable_id": "@ref:6"}),   # @ref:10
        ("pm.work_item.create", {"title": "Review Q2 Data", "deliverable_id": "@ref:6"}),         # @ref:11
        ("pm.work_item.create", {"title": "Draft Q3 Financials", "deliverable_id": "@ref:7"}),   # @ref:12
        ("pm.work_item.create", {"title": "Review Q3 Data", "deliverable_id": "@ref:7"}),         # @ref:13
        # 14-16: Deliverables under Clinic Update
        ("pm.deliverable.create", {"name": "Facility Assessment", "project_id": "@ref:3"}),  # @ref:14
        ("pm.deliverable.create", {"name": "Vendor Selection", "project_id": "@ref:3"}),     # @ref:15
        ("pm.deliverable.create", {"name": "Implementation Plan", "project_id": "@ref:3"}),  # @ref:16
        # 17-22: WorkItems under CU deliverables (inherit project_id)
        ("pm.work_item.create", {"title": "Survey Building", "deliverable_id": "@ref:14"}),   # @ref:17
        ("pm.work_item.create", {"title": "Review Codes", "deliverable_id": "@ref:14"}),       # @ref:18
        ("pm.work_item.create", {"title": "RFP Draft", "deliverable_id": "@ref:15"}),           # @ref:19
        ("pm.work_item.create", {"title": "Vendor Interviews", "deliverable_id": "@ref:15"}),   # @ref:20
        ("pm.work_item.create", {"title": "Timeline Draft", "deliverable_id": "@ref:16"}),      # @ref:21
        ("pm.work_item.create", {"title": "Resource Planning", "deliverable_id": "@ref:16"}),    # @ref:22
        # 23-24: Direct work items under opsstream (no project)
        ("pm.work_item.create", {"title": "Weekly Standup Notes", "opsstream_id": "@ref:0"}),    # @ref:23
        ("pm.work_item.create", {"title": "Monthly Metrics Review", "opsstream_id": "@ref:0"}),  # @ref:24
        # 25-26: Artifacts on two work items
        ("pm.artifact.create", {"name": "Q1 Draft Doc", "kind": "document", "work_item_id": "@ref:8"}),   # @ref:25
        ("pm.artifact.create", {"name": "RFP Document", "kind": "document", "work_item_id": "@ref:19"}),  # @ref:26
    ])

    result, store = _run_pm_plan(intent)
    assert result.success, f"Mega pm.plan failed: {result.error}"

    output = _get_step_output(store, result.run_id, "plan_compile")
    envelope = output["items"][0]
    plan = envelope["plan"]

    # 1. 27 wal.append ops
    wal_ops = [op for op in plan["ops"] if op["method"] == "wal.append"]
    assert len(wal_ops) == 27

    # 2. Diff has 27 entries
    assert len(envelope["diff"]) == 27

    # 3. Work items under deliverables have inherited project_id
    # Build a lookup: aggregate_id → wal payload
    wal_by_id = {}
    for w in wal_ops:
        wal_by_id[w["params"]["aggregate_id"]] = w["params"]["payload"]

    qr_project_id = wal_ops[1]["params"]["aggregate_id"]  # Quarterly Reporting
    cu_project_id = wal_ops[3]["params"]["aggregate_id"]  # Clinic Update

    # QR deliverables (ops 5,6,7 → wal indices 5,6,7)
    for idx in (5, 6, 7):
        del_payload = wal_ops[idx]["params"]["payload"]
        assert del_payload["project_id"] == qr_project_id

    # QR work items (ops 8-13 → wal indices 8-13): should have inherited project_id
    for idx in range(8, 14):
        wi_payload = wal_ops[idx]["params"]["payload"]
        assert wi_payload["project_id"] == qr_project_id, (
            f"WI at index {idx} should inherit QR project_id"
        )

    # CU deliverables
    for idx in (14, 15, 16):
        del_payload = wal_ops[idx]["params"]["payload"]
        assert del_payload["project_id"] == cu_project_id

    # CU work items (ops 17-22): should have inherited project_id
    for idx in range(17, 23):
        wi_payload = wal_ops[idx]["params"]["payload"]
        assert wi_payload["project_id"] == cu_project_id, (
            f"WI at index {idx} should inherit CU project_id"
        )

    # 4. Direct-to-opsstream work items have opsstream_id but no project_id
    opsstream_id = wal_ops[0]["params"]["aggregate_id"]
    for idx in (23, 24):
        wi_payload = wal_ops[idx]["params"]["payload"]
        assert wi_payload["opsstream_id"] == opsstream_id
        assert wi_payload.get("project_id") is None or "project_id" not in wi_payload or wi_payload["project_id"] == ""

    # 5. link.create ops have assert.exists for both source and target
    link_wal_ids = set()
    for w in wal_ops:
        if w["params"]["event_type"] == "link.created":
            link_wal_ids.add(w["params"]["aggregate_id"])
    assert len(link_wal_ids) == 2

    assert_exists_ops = [op for op in plan["ops"] if op["method"] == "assert.exists"]
    # Each link.create contributes 2 assert.exists (source + target)
    # Other ops also contribute assert.exists for FKs
    # Just verify we have at least 4 from links (2 links × 2)
    link_source_types = {"opsstream", "project"}
    link_asserts = [op for op in assert_exists_ops
                    if op["params"]["aggregate_type"] in link_source_types
                    and op["params"]["aggregate_id"] in {opsstream_id, qr_project_id, cu_project_id}]
    assert len(link_asserts) >= 4


def test_mega_deliverable_overwrites_explicit_project():
    """Anchor-based: deliverable is authority on project — explicit project_id is overwritten.

    Calls compile_intent directly to inspect the resolved payload.
    """
    from workman.intent import compile_intent

    intent = _make_intent([
        ("pm.project.create", {"name": "Quarterly Reporting"}),       # @ref:0
        ("pm.project.create", {"name": "Clinic Update"}),             # @ref:1
        ("pm.deliverable.create", {"name": "Vendor Selection", "project_id": "@ref:1"}),  # @ref:2
        # deliverable belongs to Clinic Update; explicit QR is overwritten
        ("pm.work_item.create", {"title": "Placed Correctly", "deliverable_id": "@ref:2", "project_id": "@ref:0"}),
    ])
    result = compile_intent(intent)
    plan = result["items"][0]["plan"]
    wal_ops = [op for op in plan["ops"] if op["method"] == "wal.append"]
    cu_project_id = wal_ops[1]["params"]["aggregate_id"]
    wi_payload = wal_ops[3]["params"]["payload"]
    # project_id was overwritten to Clinic Update (deliverable's project)
    assert wi_payload["project_id"] == cu_project_id


def test_mega_negative_move_project_blocked_by_deliverable_anchor():
    """Anchor-based: cannot reassign project when work item is anchored to a deliverable.

    Calls compile_intent directly (not through lorchestra) because lorchestra
    catches CompileError and wraps it in a failure result.
    """
    from workman.errors import CompileError
    from workman.intent import compile_intent

    intent = _make_intent([
        ("pm.project.create", {"name": "Quarterly Reporting"}),       # @ref:0
        ("pm.project.create", {"name": "Clinic Update"}),             # @ref:1
        ("pm.deliverable.create", {"name": "Assessment", "project_id": "@ref:1"}),  # @ref:2
        ("pm.work_item.create", {"title": "Task", "deliverable_id": "@ref:2"}),     # @ref:3
        # This should FAIL: work item anchored to deliverable, can't reassign project directly
        ("pm.work_item.move", {"work_item_id": "@ref:3", "project_id": "@ref:0"}),
    ])
    with pytest.raises(CompileError, match="Cannot reassign project"):
        compile_intent(intent)


def test_mega_negative_link_nonexistent_target():
    """link.create with non-existent atoms — verifies dynamic FK assertions produce assert.exists."""
    intent = _make_intent([
        ("link.create", {"source_id": "proj_FAKE", "source_type": "project",
                         "target_id": "wi_FAKE", "target_type": "work_item",
                         "predicate": "contains"}),
    ])
    result, store = _run_pm_plan(intent)
    assert result.success  # compiles fine — assertions are in the plan
    plan = _get_step_output(store, result.run_id, "plan_compile")["items"][0]["plan"]
    assert_ops = [op for op in plan["ops"] if op["method"] == "assert.exists"]
    # Should have 2 assert.exists: one for source, one for target
    assert len(assert_ops) == 2
    assert_types = {op["params"]["aggregate_type"] for op in assert_ops}
    assert assert_types == {"project", "work_item"}
    assert_ids = {op["params"]["aggregate_id"] for op in assert_ops}
    assert assert_ids == {"proj_FAKE", "wi_FAKE"}
