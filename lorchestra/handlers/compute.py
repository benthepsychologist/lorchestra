"""
Compute Handler for external IO operations.

Handles all compute operations:
- compute.llm: LLM invocation
- compute.transform: Data transformation
- compute.extract: Structured data extraction
- compute.render: Template rendering

These operations are delegated to a ComputeClient implementation,
keeping the orchestration layer free of compute service details.
"""

from typing import Any, Protocol, runtime_checkable

from lorchestra.handlers.base import Handler
from lorchestra.schemas import StepManifest, Op


@runtime_checkable
class ComputeClient(Protocol):
    """
    Protocol for compute operations.

    This interface abstracts all compute operations so that:
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

    def transform(
        self,
        input_data: Any,
        transform_ref: str,
    ) -> Any:
        """
        Apply a registered transform to input data.

        Args:
            input_data: Data to transform
            transform_ref: Reference to transform definition (e.g., "email/gmail_to_jmap@1.0")

        Returns:
            Transformed data
        """
        ...

    def extract(
        self,
        source: str,
        extractor_ref: str,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Extract structured data from a source.

        Args:
            source: Source content (text, URL, file path)
            extractor_ref: Reference to extractor definition
            options: Extraction options

        Returns:
            Dict with extracted data
        """
        ...

    def render(
        self,
        template_ref: str,
        context: dict[str, Any],
    ) -> str:
        """
        Render a template with context.

        Args:
            template_ref: Reference to template definition
            context: Template context variables

        Returns:
            Rendered template string
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

    def transform(
        self,
        input_data: Any,
        transform_ref: str,
    ) -> Any:
        """Return input unchanged (identity transform)."""
        return input_data

    def extract(
        self,
        source: str,
        extractor_ref: str,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return mock extraction result."""
        return {"source": source, "extractor": extractor_ref, "extracted": {}}

    def render(
        self,
        template_ref: str,
        context: dict[str, Any],
    ) -> str:
        """Return mock rendered string."""
        return f"[noop render: {template_ref}]"


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

        Dispatches to the appropriate ComputeClient method based on the Op.

        Args:
            manifest: The StepManifest containing operation details

        Returns:
            The operation result as a dictionary

        Raises:
            ValueError: If the operation is not a compute operation
        """
        op = manifest.op
        params = manifest.resolved_params

        if op == Op.COMPUTE_LLM:
            return self._compute_llm(params, manifest)
        elif op == Op.COMPUTE_TRANSFORM:
            return self._compute_transform(params)
        elif op == Op.COMPUTE_EXTRACT:
            return self._compute_extract(params)
        elif op == Op.COMPUTE_RENDER:
            return self._compute_render(params)

        raise ValueError(f"Unsupported compute op: {op.value}")

    def _compute_llm(
        self, params: dict[str, Any], manifest: StepManifest
    ) -> dict[str, Any]:
        """Execute compute.llm operation."""
        result = self._client.llm_invoke(
            prompt=params["prompt"],
            model=params.get("model"),
            temperature=params.get("temperature"),
            max_tokens=params.get("max_tokens"),
            system_prompt=params.get("system_prompt"),
        )
        # Include prompt_hash from manifest if available
        if manifest.prompt_hash:
            result["prompt_hash"] = manifest.prompt_hash
        return result

    def _compute_transform(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute compute.transform operation."""
        output = self._client.transform(
            input_data=params["input"],
            transform_ref=params["transform_ref"],
        )
        return {"output": output}

    def _compute_extract(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute compute.extract operation."""
        return self._client.extract(
            source=params["source"],
            extractor_ref=params["extractor_ref"],
            options=params.get("options"),
        )

    def _compute_render(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute compute.render operation."""
        rendered = self._client.render(
            template_ref=params["template_ref"],
            context=params.get("context", {}),
        )
        return {"rendered": rendered}
