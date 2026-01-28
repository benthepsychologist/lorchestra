from __future__ import annotations

import pytest


def _configure_canonizer_registry(monkeypatch, tmp_path) -> None:
    """Force canonizer to use the canonizer repo registry, not lorchestra/.canonizer.

    Canonizer prefers a project-local .canonizer/ when present. In this workspace,
    lorchestra may have an incomplete .canonizer which makes transforms fail to load.
    Running from a temp cwd avoids auto-detection, and CANONIZER_REGISTRY_ROOT pins
    to the known-good registry in /workspace/canonizer.
    """

    monkeypatch.chdir(tmp_path)
    # Disable implicit global config resolution (e.g. ~/.config/canonizer)
    # so this test is reproducible in any environment.
    monkeypatch.setenv("CANONIZER_HOME", str(tmp_path / "canonizer_home"))
    monkeypatch.setenv("CANONIZER_REGISTRY_ROOT", "/workspace/canonizer/.canonizer/registry")


def _require_importable(module_name: str) -> None:
    try:
        __import__(module_name)
    except Exception as e:  # pragma: no cover
        raise AssertionError(
            f"Required module '{module_name}' is not importable: {e}. "
            "For Gate 03c, install sibling repos into the active venv."
        ) from e


def _raw_google_forms_response_example_intake() -> dict:
    """Minimal raw Google Forms response for canonizer forms transform.

    This matches canonizer's transforms/forms/google_forms_to_canonical/1.0.0/spec.jsonata.
    We use questionId values that align with finalform's example_intake binding.
    """

    # PHQ-9 answers (10 items) - binding uses entry.123456001..010
    phq9_answers = ["not at all"] * 9 + ["not difficult at all"]
    # GAD-7 answers (8 items) - binding uses entry.789012001..008
    gad7_answers = ["not at all"] * 7 + ["not difficult at all"]

    answers: dict[str, dict] = {}

    for i, answer in enumerate(phq9_answers, 1):
        field_key = f"entry.123456{i:03d}"
        answers[field_key] = {
            "questionId": field_key,
            "textAnswers": {"answers": [{"value": answer}]},
        }

    for i, answer in enumerate(gad7_answers, 1):
        field_key = f"entry.789012{i:03d}"
        answers[field_key] = {
            "questionId": field_key,
            "textAnswers": {"answers": [{"value": answer}]},
        }

    return {
        "responseId": "resp_test_execute",
        "formId": "test_form",
        "createTime": "2025-01-15T10:30:00Z",
        "lastSubmittedTime": "2025-01-15T10:30:00Z",
        "respondentEmail": "test@example.com",
        "answers": answers,
    }


def _canonized_forms_to_finalform_form_response(canonized: dict) -> dict:
    """Adapter: canonizer forms canonical -> finalform callable input."""
    respondent = canonized.get("respondent") or {}
    email = respondent.get("email") or "unknown@example.com"

    items = []
    for idx, ans in enumerate(canonized.get("answers", []), 1):
        items.append(
            {
                "field_key": ans["question_id"],
                "position": idx,
                "answer": ans.get("answer_value"),
            }
        )

    return {
        "form_id": f"googleforms::{canonized['form_id']}",
        "form_submission_id": canonized["response_id"],
        "subject_id": f"contact::{email}",
        "timestamp": canonized.get("submitted_at") or canonized.get("last_updated_at"),
        "items": items,
    }


def test_gate03c_full_pipeline_forms_smoke(tmp_path, monkeypatch):
    """Gate 03c: injest → canonizer → finalform → projectionist (hermetic smoke).

    This uses:
    - injest fixture stream (no external credentials)
    - canonizer forms transform
    - finalform example_intake binding (deterministic ids)
    - projectionist with a test-registered projection

    It does NOT hit storacle / BigQuery.
    """

    for module in ("injest", "canonizer", "finalform", "projectionist"):
        _require_importable(module)

    import injest
    import canonizer
    import finalform
    import projectionist

    raw = _raw_google_forms_response_example_intake()

    inj = injest.execute({"source": "fixture", "config": {"items": [raw]}})
    assert inj["schema_version"] == "1.0"
    assert isinstance(inj["items"], list) and len(inj["items"]) == 1

    _configure_canonizer_registry(monkeypatch, tmp_path)
    can = canonizer.execute(
        {
            "source_type": "form",
            "items": inj["items"],
            "config": {
                "transform_id": "forms/google_forms_to_canonical@1.0.0",
                "validate_input": False,
                "validate_output": False,
            },
        }
    )
    assert can["schema_version"] == "1.0"
    assert isinstance(can["items"], list) and len(can["items"]) == 1

    ff_input = _canonized_forms_to_finalform_form_response(can["items"][0])

    ff = finalform.execute(
        {
            "instrument": "example_intake",
            "items": ff_input,
            "config": {
                "measure_registry_path": "/workspace/finalform/measure-registry",
                "binding_registry_path": "/workspace/finalform/form-binding-registry",
                "binding_id": "example_intake",
                "binding_version": "1.0.0",
                "deterministic_ids": True,
            },
        }
    )
    assert ff["schema_version"] == "1.0"
    assert isinstance(ff["items"], list)
    assert len(ff["items"]) == 2  # PHQ-9 + GAD-7 measurement events

    @projectionist.register_projection("test.projection.identity")
    def _identity_projection(items, _config):
        return list(items)

    proj = projectionist.execute(
        {
            "projection_id": "test.projection.identity",
            "items": ff["items"],
            "config": {},
        }
    )
    assert proj["schema_version"] == "1.0"
    assert isinstance(proj["items"], list)
    assert len(proj["items"]) == len(ff["items"])


@pytest.mark.skipif(
    not bool(__import__("os").environ.get("E005B_E2E_RUN_PERF")),
    reason="Set E005B_E2E_RUN_PERF=1 to run the 1000-item functional perf check.",
)
def test_gate03c_pipeline_1000_functional_no_io(tmp_path, monkeypatch):
    """Gate 03c: Validate pipeline can process 1000 items (functional check).

    This is intentionally not time-based. It ensures the chain works at scale
    without external IO.
    """

    import injest
    import canonizer
    import finalform

    raw = _raw_google_forms_response_example_intake()
    raws = [dict(raw, responseId=f"resp_{i:04d}") for i in range(1000)]

    inj = injest.execute({"source": "fixture", "config": {"items": raws}})

    _configure_canonizer_registry(monkeypatch, tmp_path)
    can = canonizer.execute(
        {
            "source_type": "form",
            "items": inj["items"],
            "config": {
                "transform_id": "forms/google_forms_to_canonical@1.0.0",
                "validate_input": False,
                "validate_output": False,
            },
        }
    )

    ff_inputs = [_canonized_forms_to_finalform_form_response(x) for x in can["items"]]

    ff = finalform.execute(
        {
            "instrument": "example_intake",
            "items": ff_inputs,
            "config": {
                "measure_registry_path": "/workspace/finalform/measure-registry",
                "binding_registry_path": "/workspace/finalform/form-binding-registry",
                "binding_id": "example_intake",
                "binding_version": "1.0.0",
                "deterministic_ids": True,
            },
        }
    )

    assert ff["stats"]["input"] == 1000
    assert ff["stats"]["output"] == 2000
    assert ff["stats"]["errors"] == 0
