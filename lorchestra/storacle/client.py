"""
Storacle client - IO boundary for submitting StoraclePlan.

This module provides the single boundary where lorchestra talks to storacle.
Currently implements in-proc v0; RPC transport to follow.

v0: Direct in-proc call to storacle.execute_plan(plan)
Later: Wrap in JSON-RPC envelope and send over transport

Error classification:
- TransientError/PermanentError propagated unchanged from storacle
- Builtin TimeoutError -> TransientError (safe to retry)
- Unknown exceptions -> PermanentError (fail fast, no string matching)
"""

import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

from lorchestra.errors import TransientError, PermanentError
from lorchestra.plan_builder import StoraclePlan


@dataclass
class RpcMeta:
    """
    Metadata for RPC calls to storacle.

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


def submit_plan(plan: StoraclePlan, meta: RpcMeta) -> dict:
    """
    Submit StoraclePlan to storacle.

    v0: Direct in-proc call to storacle.execute_plan(plan)
    Later: Wrap in JSON-RPC envelope and send over transport.

    Args:
        plan: StoraclePlan to submit
        meta: RPC metadata for tracing

    Returns:
        Result dictionary from storacle

    Raises:
        TransientError: Transient failure (safe to retry)
        PermanentError: Permanent failure (do not retry)
    """
    if IN_PROC:
        return _submit_inproc(plan, meta)
    else:
        return _submit_rpc(plan, meta)


def _submit_inproc(plan: StoraclePlan, meta: RpcMeta) -> dict:
    """
    In-proc path: call storacle directly with plan object.

    Error classification:
    - TransientError: Already classified, propagate
    - PermanentError: Already classified, propagate
    - TimeoutError: Builtin timeout is transient
    - Exception: Unknown = permanent (fail fast)
    """
    try:
        # Try to import storacle - it may not be installed
        from storacle import execute_plan  # type: ignore
    except ImportError:
        # Storacle not installed - use noop implementation
        return _noop_execute_plan(plan, meta)

    try:
        result = execute_plan(plan.to_dict(), correlation_id=meta.correlation_id)
        return result
    except TransientError:
        raise  # Already classified, propagate
    except PermanentError:
        raise  # Already classified, propagate
    except TimeoutError as e:
        raise TransientError(str(e)) from e  # Builtin timeout is transient
    except Exception as e:
        raise PermanentError(str(e)) from e  # Unknown = permanent (fail fast)


def _noop_execute_plan(plan: StoraclePlan, meta: RpcMeta) -> dict:
    """
    No-op implementation when storacle is not installed.

    Returns a mock success result for testing.
    """
    return {
        "status": "noop",
        "ops_executed": len(plan.ops),
        "correlation_id": meta.correlation_id,
    }


def _submit_rpc(plan: StoraclePlan, meta: RpcMeta) -> dict:
    """
    RPC path: wrap in JSON-RPC envelope (for later).

    Not yet implemented - will be added when remote transport is needed.
    """
    request = {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "storacle.execute_plan",
        "params": {
            "_meta": asdict(meta),
            "payload": plan.to_dict(),
        },
    }

    # TODO: send over transport
    raise NotImplementedError(
        f"RPC transport not yet implemented. Request: {request}"
    )
