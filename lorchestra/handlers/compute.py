"""
Compute Handler for external IO operations.

Handles compute.llm operations via ComputeClient.

Keeping the orchestration layer free of compute service details.
"""

import hashlib
import json
from typing import Any, Protocol, runtime_checkable

from lorchestra.handlers.base import Handler
from lorchestra.schemas import StepManifest, Op


@runtime_checkable
class ComputeClient(Protocol):
    """
    Protocol for compute operations.

    This interface abstracts LLM operations so that:
    1. lorchestra orchestration layer has no compute service imports
    2. Compute backend can be swapped (OpenAI, Anthropic, local, mock)
    3. Testing is simplified via mock implementations
    """

    def llm_invoke(
        self,
        prompt: str,
        model: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        system_prompt: str | None = None,
    ) -> dict[str, Any]:
        """
        Invoke an LLM and return the response.

        Args:
            prompt: The user prompt to send
            model: Model identifier (optional, uses default)
            temperature: Sampling temperature (optional)
            max_tokens: Maximum tokens in response (optional)
            system_prompt: System prompt (optional)

        Returns:
            Dict with {"response": str, "model": str, "usage": {...}}
        """
        ...


class NoOpComputeClient:
    """
    No-op implementation of ComputeClient for testing.

    Returns mock results without performing actual operations.
    """

    def llm_invoke(
        self,
        prompt: str,
        model: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        system_prompt: str | None = None,
    ) -> dict[str, Any]:
        """Return mock LLM response."""
        return {
            "response": "[noop response]",
            "model": model or "noop-model",
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        }


class ComputeHandler(Handler):
    """
    Handler for compute operations (compute.*).

    Delegates to a ComputeClient implementation for actual computation.
    """

    def __init__(self, client: ComputeClient):
        """
        Initialize the compute handler.

        Args:
            client: ComputeClient implementation for compute operations
        """
        self._client = client

    def execute(self, manifest: StepManifest) -> dict[str, Any]:
        """
        Execute a compute operation.

        Args:
            manifest: The StepManifest containing operation details

        Returns:
            The operation result as a dictionary

        Raises:
            ValueError: If the operation is not compute.llm
        """
        op = manifest.op
        params = manifest.resolved_params

        if op == Op.COMPUTE_LLM:
            return self._compute_llm(params, manifest)

        raise ValueError(f"Unsupported compute op: {op.value}")

    def _compute_llm(
        self, params: dict[str, Any], manifest: StepManifest
    ) -> dict[str, Any]:
        """Execute compute.llm operation."""
        prompt: str = params["prompt"]
        config = {
            "model": params.get("model"),
            "temperature": params.get("temperature"),
            "max_tokens": params.get("max_tokens"),
            "system_prompt": params.get("system_prompt"),
        }

        result = self._client.llm_invoke(
            prompt=prompt,
            model=config["model"],
            temperature=config["temperature"],
            max_tokens=config["max_tokens"],
            system_prompt=config["system_prompt"],
        )

        prompt_hash = manifest.prompt_hash or _sha256_prefixed(prompt)
        result["prompt_hash"] = prompt_hash

        config_hash = _sha256_prefixed(_canonical_json(config))
        result["config_hash"] = config_hash

        response_text = result.get("response")
        if isinstance(response_text, str):
            output_hash = _sha256_prefixed(response_text)
            result["output_hash"] = output_hash
            # v0: no artifact store; use a stable synthetic ref for audit parity.
            result["output_ref"] = f"artifact://inline/{output_hash}"

        return result


def _canonical_json(data: dict[str, Any]) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_prefixed(text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"
