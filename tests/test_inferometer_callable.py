"""Tests for inferometer callable adapter integration."""

from __future__ import annotations

import sys
import types

import pytest

from lorchestra.callable.dispatch import dispatch_callable, register_callable
from lorchestra.callable.inferometer_adapter import execute as inferometer_execute
from lorchestra.errors import PermanentError, TransientError


def _install_fake_inferometer(
    monkeypatch: pytest.MonkeyPatch,
    *,
    execute_error: Exception | None = None,
) -> dict[str, object]:
    captured: dict[str, object] = {}

    class FakeTransientError(Exception):
        pass

    class FakePermanentError(Exception):
        pass

    class PromptStep:
        def __init__(self, step_id: str, prompt_template: str, inputs: dict[str, str]):
            self.step_id = step_id
            self.prompt_template = prompt_template
            self.inputs = inputs

    class PromptPlan:
        def __init__(self, model: str, config: dict, steps: list[PromptStep]):
            self.model = model
            self.config = config
            self.steps = steps

    class StepResult:
        def __init__(self, step_id: str, prompt_hash: str, output_hash: str):
            self.step_id = step_id
            self.prompt_hash = prompt_hash
            self.output_hash = output_hash

    class InferenceResult:
        def __init__(self):
            self.output = "analysis output"
            self.output_ref = None
            self.steps = [StepResult("analyze", "sha256:prompt", "sha256:out")]
            self.config_hash = "sha256:config"
            self.prompt_hash = "sha256:prompt"
            self.output_hash = "sha256:out"

    def execute(plan: PromptPlan):
        captured["plan"] = plan
        if execute_error is not None:
            raise execute_error
        return InferenceResult()

    fake_module = types.ModuleType("inferometer")
    fake_module.PromptPlan = PromptPlan
    fake_module.PromptStep = PromptStep
    fake_module.TransientError = FakeTransientError
    fake_module.PermanentError = FakePermanentError
    fake_module.execute = execute

    monkeypatch.setitem(sys.modules, "inferometer", fake_module)
    return captured


def test_inferometer_callable_with_mock(monkeypatch: pytest.MonkeyPatch):
    """inferometer callable should execute through dispatch and return normalized result."""
    captured = _install_fake_inferometer(monkeypatch)
    register_callable("inferometer", inferometer_execute)

    params = {
        "model": "claude-3-5-sonnet",
        "temperature": 0.3,
        "max_tokens": 500,
        "top_p": 0.9,
        "config": {"top_p": 0.1},
        "prompt_template": "Analyze: {transcript}",
        "transcript": "This is a test transcript.",
    }

    result = dispatch_callable("inferometer", params)

    assert result.items is not None
    assert result.items[0]["output"] == "analysis output"
    assert result.items[0]["prompt_hash"] == "sha256:prompt"
    assert result.items[0]["output_hash"] == "sha256:out"
    assert result.items[0]["config_hash"] == "sha256:config"
    assert result.stats["steps_count"] == 1

    plan = captured["plan"]
    assert plan.model == "claude-3-5-sonnet"
    assert plan.config["temperature"] == 0.3
    assert plan.config["max_tokens"] == 500
    assert plan.config["top_p"] == 0.9  # params should override config


def test_inferometer_missing_params(monkeypatch: pytest.MonkeyPatch):
    """Missing required params should raise PermanentError."""
    _install_fake_inferometer(monkeypatch)
    register_callable("inferometer", inferometer_execute)

    with pytest.raises(PermanentError, match="Required params missing"):
        dispatch_callable("inferometer", {"model": "claude-3-5-sonnet"})


def test_inferometer_max_tokens_ceiling(monkeypatch: pytest.MonkeyPatch):
    """max_tokens above 100k should raise PermanentError."""
    _install_fake_inferometer(monkeypatch)
    register_callable("inferometer", inferometer_execute)

    params = {
        "model": "claude-3-5-sonnet",
        "max_tokens": 100001,
        "prompt_template": "Analyze: {transcript}",
        "transcript": "x",
    }

    with pytest.raises(PermanentError, match="exceeds ceiling"):
        dispatch_callable("inferometer", params)


def test_inferometer_model_unavailable_is_transient(monkeypatch: pytest.MonkeyPatch):
    """Rate limit / quota failures should raise TransientError."""
    _install_fake_inferometer(monkeypatch, execute_error=RuntimeError("rate limit exceeded"))
    register_callable("inferometer", inferometer_execute)

    params = {
        "model": "claude-3-5-sonnet",
        "prompt_template": "Analyze: {transcript}",
        "transcript": "x",
    }

    with pytest.raises(TransientError, match="transient"):
        dispatch_callable("inferometer", params)


def test_inferometer_unknown_model_is_permanent(monkeypatch: pytest.MonkeyPatch):
    """Unknown model errors should raise PermanentError, not TransientError."""
    _install_fake_inferometer(monkeypatch, execute_error=RuntimeError("Unknown model: fake-model"))
    register_callable("inferometer", inferometer_execute)

    params = {
        "model": "fake-model",
        "prompt_template": "Analyze: {transcript}",
        "transcript": "x",
    }

    with pytest.raises(PermanentError, match="Inference failed"):
        dispatch_callable("inferometer", params)


def test_inferometer_inferometer_transient_error_mapped(monkeypatch: pytest.MonkeyPatch):
    """Inferometer's own TransientError should be re-raised as lorchestra TransientError."""
    _install_fake_inferometer(monkeypatch)
    fake_mod = sys.modules["inferometer"]

    def execute(_plan):
        raise fake_mod.TransientError("service overloaded")

    fake_mod.execute = execute
    register_callable("inferometer", inferometer_execute)

    params = {
        "model": "claude-3-5-sonnet",
        "prompt_template": "Analyze: {transcript}",
        "transcript": "x",
    }

    with pytest.raises(TransientError, match="transient"):
        dispatch_callable("inferometer", params)
