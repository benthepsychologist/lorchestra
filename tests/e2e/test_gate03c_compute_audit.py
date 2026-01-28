from __future__ import annotations


def test_gate03c_compute_llm_audit_minimums_present():
    """Gate 03c: compute.llm audit minimums are present.

    Spec requires: prompt_hash, config_hash, output_hash + output_ref.
    This test does not call an external LLM.
    """

    from lorchestra.handlers.compute import ComputeHandler, NoOpComputeClient
    from lorchestra.schemas import Op, StepManifest

    handler = ComputeHandler(NoOpComputeClient())

    manifest = StepManifest(
        run_id="run_e2e",
        step_id="step_llm",
        backend="inferometer",
        op=Op.COMPUTE_LLM,
        resolved_params={
            "prompt": "Hello world",
            "model": "noop-model",
            "temperature": 0.0,
            "max_tokens": 16,
            "system_prompt": "You are a helpful assistant.",
        },
        prompt_hash=None,
        idempotency_key="idem",
    )

    result = handler.execute(manifest)

    assert "prompt_hash" in result
    assert "config_hash" in result
    assert "output_hash" in result
    assert "output_ref" in result

    assert result["prompt_hash"].startswith("sha256:")
    assert result["config_hash"].startswith("sha256:")
    assert result["output_hash"].startswith("sha256:")
    assert result["output_ref"].startswith("artifact://")
