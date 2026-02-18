"""
inferometer_adapter callable — Execute LLM inference via inferometer.

This callable bridges lorchestra's dict-based callable protocol with
inferometer's PromptPlan execution model.
"""

from typing import Any

from lorchestra.errors import PermanentError, TransientError


MAX_TOKENS_CEILING = 100000
_PARAM_KEYS = {
    "model",
    "prompt_template",
    "transcript",
    "config",
    "step_id",
    "temperature",
    "max_tokens",
    "inputs",
    "steps",
    "plan",
}


def execute(params: dict[str, Any]) -> dict[str, Any]:
    """Execute LLM analysis via inferometer."""
    try:
        import inferometer
        from inferometer import (
            PermanentError as InferometerPermanentError,
            PromptPlan,
            PromptStep,
            TransientError as InferometerTransientError,
        )
    except ImportError as e:
        raise PermanentError(f"inferometer not installed: {e}") from e

    model = params.get("model")
    prompt_template = params.get("prompt_template")
    transcript = params.get("transcript")
    step_id = params.get("step_id", "analyze")
    inputs_param = params.get("inputs")
    steps_param = params.get("steps")
    plan_param = params.get("plan")

    if transcript is not None and not isinstance(transcript, str):
        raise PermanentError("transcript must be a string")
    if inputs_param is not None and not isinstance(inputs_param, dict):
        raise PermanentError("inputs must be a dict when provided")

    raw_config = params.get("config", {})
    if raw_config is None:
        raw_config = {}
    if not isinstance(raw_config, dict):
        raise PermanentError("config must be a dict when provided")

    # Base config overrides passed in directly to this callable
    config_overrides = dict(raw_config)

    # Forward any additional params into inferometer config (as overrides)
    for key, value in params.items():
        if key not in _PARAM_KEYS:
            config_overrides[key] = value

    try:
        if plan_param is not None:
            if not isinstance(plan_param, dict):
                raise PermanentError("plan must be a dict when provided")

            plan_model = plan_param.get("model") or model
            if not plan_model:
                raise PermanentError("Required params missing: model")

            plan_config = plan_param.get("config", {})
            if plan_config is None:
                plan_config = {}
            if not isinstance(plan_config, dict):
                raise PermanentError("plan.config must be a dict when provided")

            config = dict(plan_config)
            config.update(config_overrides)

            plan_steps = plan_param.get("steps")
            if not isinstance(plan_steps, list) or not plan_steps:
                raise PermanentError("plan.steps must be a non-empty list")

            steps = []
            for s in plan_steps:
                if not isinstance(s, dict):
                    raise PermanentError("plan.steps items must be dicts")
                steps.append(PromptStep(**s))

        elif steps_param is not None:
            if not isinstance(steps_param, list) or not steps_param:
                raise PermanentError("steps must be a non-empty list when provided")
            if not model:
                raise PermanentError("Required params missing: model")

            plan_model = model
            config = dict(config_overrides)

            steps = []
            for s in steps_param:
                if not isinstance(s, dict):
                    raise PermanentError("steps items must be dicts")
                steps.append(PromptStep(**s))

        else:
            if not model or not prompt_template:
                raise PermanentError("Required params missing: model, prompt_template")
            if transcript is None and inputs_param is None:
                raise PermanentError("Required params missing: transcript or inputs")

            plan_model = model
            config = dict(config_overrides)

            inputs: dict[str, str] = {}
            if inputs_param is not None:
                inputs.update(inputs_param)
            if transcript is not None:
                inputs.setdefault("transcript", transcript)

            steps = [
                PromptStep(
                    step_id=step_id,
                    prompt_template=prompt_template,
                    inputs=inputs,
                )
            ]

        # temperature: params overrides config; config overrides default
        if "temperature" in params:
            config["temperature"] = params.get("temperature")
        else:
            config.setdefault("temperature", 0.3)

        # Normalize and validate max_tokens (params overrides config; config overrides default)
        max_tokens_raw = params.get("max_tokens", config.get("max_tokens", 10000))
        try:
            max_tokens = int(max_tokens_raw)
        except (TypeError, ValueError) as e:
            raise PermanentError("max_tokens must be an integer") from e
        if max_tokens > MAX_TOKENS_CEILING:
            raise PermanentError(f"max_tokens {max_tokens} exceeds ceiling of {MAX_TOKENS_CEILING}")
        config["max_tokens"] = max_tokens

        plan = PromptPlan(model=plan_model, config=config, steps=steps)

    except PermanentError:
        raise
    except Exception as e:
        raise PermanentError(f"Invalid inference plan: {e}") from e

    try:
        result = inferometer.execute(plan)
    except InferometerTransientError as e:
        raise TransientError(f"Inference transient failure: {e}") from e
    except InferometerPermanentError as e:
        raise PermanentError(f"Inference failed: {e}") from e
    except Exception as e:
        error_str = str(e).lower()
        if any(phrase in error_str for phrase in ("rate_limit", "rate limit", "quota", "unavailable", "capacity", "overloaded")):
            raise TransientError(f"Inference transient failure: {e}") from e
        raise PermanentError(f"Inference failed: {e}") from e

    item: dict[str, Any] = {
        "prompt_hash": result.prompt_hash,
        "output_hash": result.output_hash,
        "config_hash": result.config_hash,
        "model": plan.model,
        "config": config,
        "steps": [
            {
                "step_id": step.step_id,
                "prompt_hash": step.prompt_hash,
                "output_hash": step.output_hash,
            }
            for step in result.steps
        ],
    }
    if result.output is not None:
        item["output"] = result.output
    if getattr(result, "output_ref", None) is not None:
        item["output_ref"] = result.output_ref

    return {"items": [item], "stats": {"steps_count": len(result.steps)}}
