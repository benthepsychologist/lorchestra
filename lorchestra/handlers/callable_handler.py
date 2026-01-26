"""
Callable Handler for dispatching call.* ops to in-proc callables.

This handler:
1. Receives StepManifest with call.* op
2. Dispatches to appropriate callable via dispatch_callable
3. Converts CallableResult to StoraclePlan
4. Submits plan to storacle via client boundary
5. Returns result
"""

from typing import Any

from lorchestra.handlers.base import Handler
from lorchestra.schemas import StepManifest
from lorchestra.callable import dispatch_callable, CallableResult
from lorchestra.plan_builder import build_plan
from lorchestra.storacle import submit_plan, RpcMeta


class CallableHandler(Handler):
    """
    Handler for callable operations (call.*).

    Dispatches to in-proc callables, converts results to StoraclePlan,
    and submits to storacle.
    """

    def execute(self, manifest: StepManifest) -> dict[str, Any]:
        """
        Execute a callable operation.

        1. Dispatch to callable
        2. Build StoraclePlan from CallableResult
        3. Submit plan to storacle
        4. Return result

        Args:
            manifest: StepManifest with call.* op

        Returns:
            Execution result with storacle response

        Raises:
            ValueError: If op is not a call.* operation
            TransientError: Propagated from callable or storacle (safe to retry)
            PermanentError: Propagated from callable or storacle (do not retry)
        """
        op = manifest.op

        if not op.value.startswith("call."):
            raise ValueError(f"CallableHandler only handles call.* ops, got: {op.value}")

        # 1. Dispatch to callable
        result: CallableResult = dispatch_callable(op, manifest.resolved_params)

        # 2. Build StoraclePlan
        correlation_id = f"{manifest.run_id}:{manifest.step_id}"
        plan = build_plan(result, correlation_id)

        # 3. Submit to storacle
        meta = RpcMeta(
            run_id=manifest.run_id,
            step_id=manifest.step_id,
            correlation_id=correlation_id,
        )
        storacle_result = submit_plan(plan, meta)

        # 4. Return combined result
        return {
            "callable_result": {
                "schema_version": result.schema_version,
                "items_count": len(result.items) if result.items else 0,
                "items_ref": result.items_ref,
                "stats": result.stats,
            },
            "storacle_result": storacle_result,
            "plan_ops_count": len(plan.ops),
        }
