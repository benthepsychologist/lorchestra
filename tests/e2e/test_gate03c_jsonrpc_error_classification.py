from __future__ import annotations


def test_gate03c_jsonrpc_errors_include_classification():
    """Gate 03c: JSON-RPC error responses include error.data.classification.

    This is a pure in-proc call into storacle's plan executor using an invalid method
    so it does not require BigQuery/WAL credentials.
    """

    from storacle.rpc import execute_plan

    plan = {
        "plan_version": "storacle.plan/1.0.0",
        "plan_id": "plan_test",
        "jsonrpc": "2.0",
        "ops": [
            {
                "jsonrpc": "2.0",
                "id": "op_1",
                "method": "does.not_exist",
                "params": {},
            }
        ],
    }

    responses = execute_plan(plan, dry_run=True)
    assert isinstance(responses, list)
    assert len(responses) == 1

    resp = responses[0]
    assert resp["jsonrpc"] == "2.0"
    assert resp["id"] == "op_1"
    assert "error" in resp

    data = resp["error"].get("data")
    assert isinstance(data, dict)
    assert data.get("classification") in {"transient", "permanent"}
