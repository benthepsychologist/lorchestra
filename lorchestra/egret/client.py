"""
Egret client - IO boundary for submitting EgretPlan.

This module provides the single boundary where lorchestra talks to egret.
Currently implements in-proc v0; RPC transport to follow.

v0: Direct in-proc call to egret.execute_plan(plan)
Later: Wrap in JSON-RPC envelope and send over transport

Error classification:
- TransientError/PermanentError propagated unchanged from egret
- Builtin TimeoutError -> TransientError (safe to retry)
- Unknown exceptions -> PermanentError (fail fast, no string matching)
"""

import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

from lorchestra.errors import TransientError, PermanentError


@dataclass
class RpcMeta:
    """
    Metadata for RPC calls to egret.

    Provides tracing and audit information for the request.
    """
    schema_version: str = "1.0"
    job_id: str = ""
    run_id: str = ""
    step_id: str = ""
    correlation_id: str = ""
    caller: str = "lorchestra"
    ts: str = ""

    def __post_init__(self):
        """Set timestamp if not provided."""
        if not self.ts:
            self.ts = datetime.now(timezone.utc).isoformat()


# v0: in-proc. Later: swap to remote transport
IN_PROC = True


def submit_plan(plan: dict, meta: RpcMeta) -> dict:
    """
    Submit EgretPlan dict to egret.

    v0: Direct in-proc call to egret.execute_plan(plan)
    Later: Wrap in JSON-RPC envelope and send over transport.

    Args:
        plan: EgretPlan dict (egret.plan/1.0.0 schema)
        meta: RPC metadata for tracing

    Returns:
        Result dictionary from egret (contains responses list)

    Raises:
        TransientError: Transient failure (safe to retry)
        PermanentError: Permanent failure (do not retry)
    """
    if IN_PROC:
        return _submit_inproc(plan, meta)
    else:
        return _submit_rpc(plan, meta)


def _submit_inproc(plan: dict, meta: RpcMeta) -> dict:
    """
    In-proc path: call egret directly with plan dict.

    Error classification:
    - TransientError: Already classified, propagate
    - PermanentError: Already classified, propagate
    - TimeoutError: Builtin timeout is transient
    - Exception: Unknown = permanent (fail fast)
    """
    try:
        # Try to import egret - it may not be installed
        from egret import execute_plan  # type: ignore
    except ImportError:
        # Egret not installed - use noop implementation
        return _noop_execute_plan(plan, meta)

    try:
        result = execute_plan(plan)
        return {"responses": result}
    except TransientError:
        raise  # Already classified, propagate
    except PermanentError:
        raise  # Already classified, propagate
    except TimeoutError as e:
        raise TransientError(str(e)) from e  # Builtin timeout is transient
    except Exception as e:
        raise PermanentError(str(e)) from e  # Unknown = permanent (fail fast)


def _noop_execute_plan(plan: dict, meta: RpcMeta) -> dict:
    """
    No-op implementation when egret is not installed.

    Returns a mock success result for testing.
    """
    return {
        "status": "noop",
        "ops_count": len(plan.get("ops", [])),
        "correlation_id": meta.correlation_id,
    }


def _submit_rpc(plan: dict, meta: RpcMeta) -> dict:
    """
    RPC path: wrap in JSON-RPC envelope (for later).

    Not yet implemented - will be added when remote transport is needed.
    """
    request = {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "egret.execute_plan",
        "params": {
            "_meta": asdict(meta),
            "payload": plan,
        },
    }

    # TODO: send over transport
    raise NotImplementedError(
        f"RPC transport not yet implemented. Request: {request}"
    )
