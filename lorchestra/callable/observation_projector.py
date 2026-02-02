"""
observation_projector callable — Score measurement_events into observations.

Replaces ObservationProjection processor logic:
1. Reads measurement_events that don't have observations yet (incremental)
2. Looks up canonical payload for each via BQ
3. Runs final-form scoring pipeline
4. Creates observation rows (1 per scored measure, with components JSON)
5. Returns CallableResult with items for bq.upsert

Each item is a batch: {"dataset": ..., "table": ..., "rows": [...], "key_columns": [...]}
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lorchestra.callable.result import CallableResult

logger = logging.getLogger(__name__)

# Default registry paths for final-form
DEFAULT_MEASURE_REGISTRY = Path("/workspace/finalform/measure-registry")
DEFAULT_BINDING_REGISTRY = Path("/workspace/finalform/form-binding-registry")


def execute(params: dict[str, Any]) -> dict[str, Any]:
    """Score measurement_events into observation rows.

    Params:
        binding_id: The form binding ID (e.g., "intake_01")
        binding_version: Optional binding version (default: latest)
        measurement_table: Source table name (default: "measurement_events")
        observation_table: Target table name (default: "observations")

    Returns:
        CallableResult with a single item containing:
        {"dataset": ..., "table": ..., "rows": [...], "key_columns": ["idem_key"]}
    """
    from lorchestra.config import load_config
    from finalform.pipeline import Pipeline, PipelineConfig, ProcessingResult  # noqa: F401

    binding_id = params.get("binding_id")
    binding_version = params.get("binding_version")
    measurement_table = params.get("measurement_table", "measurement_events")
    observation_table = params.get("observation_table", "observations")

    if not binding_id:
        raise ValueError("Missing required param: 'binding_id'")

    config = load_config()

    # Resolve registry paths
    formation_root = config.formation_registry_root
    if formation_root:
        root_path = Path(formation_root).expanduser()
        measure_registry_path = root_path / "measure-registry"
        binding_registry_path = root_path / "form-binding-registry"
    else:
        measure_registry_path = DEFAULT_MEASURE_REGISTRY
        binding_registry_path = DEFAULT_BINDING_REGISTRY

    # Create final-form Pipeline
    pipeline_config = PipelineConfig(
        measure_registry_path=measure_registry_path,
        binding_registry_path=binding_registry_path,
        binding_id=binding_id,
        binding_version=binding_version,
    )
    pipeline = Pipeline(pipeline_config)

    # Query measurement_events that need scoring
    me_records = _query_unscored_measurement_events(
        project=config.project,
        dataset_derived=config.dataset_derived,
        measurement_table=measurement_table,
        observation_table=observation_table,
        binding_id=binding_id,
    )

    logger.info(f"Found {len(me_records)} measurement_events to score")

    if not me_records:
        result = CallableResult(
            items=[],
            stats={"input": 0, "output": 0, "skipped": 0, "errors": 0},
        )
        return result.to_dict()

    # Score each measurement_event
    observation_rows = []
    errors = 0

    for me_record in me_records:
        try:
            # Look up canonical payload
            canonical_object_id = me_record["canonical_object_id"]
            canonical_record = _get_canonical_record(
                project=config.project,
                dataset_canonical=config.dataset_canonical,
                idem_key=canonical_object_id,
            )

            if not canonical_record:
                raise ValueError(f"Canonical object not found: {canonical_object_id}")

            payload = canonical_record.get("payload", {})
            if isinstance(payload, str):
                payload = json.loads(payload)

            # Build form_response for final-form
            form_response = _build_form_response(canonical_record, payload)

            # Run scoring
            result: ProcessingResult = pipeline.process(form_response)

            # Create observations from scored results
            correlation_id = me_record.get("correlation_id")

            for event in result.events:
                obs_row = _measure_to_observation(
                    event=event,
                    measurement_event_id=me_record["measurement_event_id"],
                    correlation_id=correlation_id,
                )
                observation_rows.append(obs_row)

        except Exception as e:
            errors += 1
            if errors <= 3:
                me_id = me_record.get("measurement_event_id", "unknown")
                logger.warning(f"Failed to score {me_id}: {e}")

    logger.info(f"Scored {len(observation_rows)} observations ({errors} errors)")

    # Package as a single bq.upsert item
    items = []
    if observation_rows:
        items.append({
            "dataset": config.dataset_derived,
            "table": observation_table,
            "rows": observation_rows,
            "key_columns": ["idem_key"],
        })

    callable_result = CallableResult(
        items=items,
        stats={
            "input": len(me_records),
            "output": len(observation_rows),
            "skipped": 0,
            "errors": errors,
        },
    )
    return callable_result.to_dict()


def _query_unscored_measurement_events(
    project: str,
    dataset_derived: str,
    measurement_table: str,
    observation_table: str,
    binding_id: str,
) -> list[dict[str, Any]]:
    """Query measurement_events that don't have observations yet.

    Returns measurement_events where:
    - No observations exist for that measurement_event_id
    - binding_id matches
    """
    from storacle.clients.bigquery import BigQueryClient

    bq_client = BigQueryClient(project=project)

    sql = f"""
        SELECT m.idem_key, m.measurement_event_id, m.subject_id, m.event_type,
               m.event_subtype, m.occurred_at, m.source_system, m.source_entity,
               m.source_id, m.canonical_object_id, m.form_id, m.binding_id,
               m.binding_version, m.correlation_id
        FROM `{project}.{dataset_derived}.{measurement_table}` m
        LEFT JOIN (
            SELECT DISTINCT measurement_event_id
            FROM `{project}.{dataset_derived}.{observation_table}`
        ) o ON m.measurement_event_id = o.measurement_event_id
        WHERE o.measurement_event_id IS NULL
          AND m.binding_id = @binding_id
    """

    query_params = [{"name": "binding_id", "type": "STRING", "value": binding_id}]
    result = bq_client.query(sql, params=query_params)
    rows = []
    for row in result.rows:
        row_dict = dict(row) if not isinstance(row, dict) else row
        rows.append(row_dict)
    return rows


def _get_canonical_record(
    project: str,
    dataset_canonical: str,
    idem_key: str,
) -> dict[str, Any] | None:
    """Look up a canonical object by idem_key."""
    from storacle.clients.bigquery import BigQueryClient

    bq_client = BigQueryClient(project=project)

    sql = f"""
        SELECT idem_key, source_system, connection_name, object_type,
               canonical_schema, transform_ref, payload, correlation_id
        FROM `{project}.{dataset_canonical}.canonical_objects`
        WHERE idem_key = @idem_key
        LIMIT 1
    """

    query_params = [{"name": "idem_key", "type": "STRING", "value": idem_key}]
    result = bq_client.query(sql, params=query_params)
    rows = list(result.rows)
    if not rows:
        return None
    row = rows[0]
    row_dict = dict(row) if not isinstance(row, dict) else row
    # Parse payload JSON string
    if "payload" in row_dict and isinstance(row_dict["payload"], str):
        row_dict["payload"] = json.loads(row_dict["payload"])
    return row_dict


def _build_form_response(
    record: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Map canonical form_response to final-form input format."""
    answers_data = payload.get("answers", [])
    items = []
    for i, ans in enumerate(answers_data):
        items.append({
            "field_key": ans.get("question_id"),
            "answer": ans.get("answer_value"),
            "position": i,
        })

    connection_name = record.get("connection_name", "")
    form_id = payload.get("form_id") or f"googleforms::{connection_name}"

    respondent = payload.get("respondent", {})
    subject_id = respondent.get("id") or respondent.get("email")

    return {
        "form_id": form_id,
        "form_submission_id": payload.get("response_id") or payload.get("submission_id"),
        "subject_id": subject_id,
        "timestamp": payload.get("submitted_at"),
        "items": items,
    }


def _measure_to_observation(
    event: Any,  # MeasurementEvent from final-form
    measurement_event_id: str,
    correlation_id: str | None,
) -> dict[str, Any]:
    """Create an observation row from a scored MeasurementEvent."""
    # Build idempotent key
    idem_key = f"{measurement_event_id}:{event.measure_id}"

    # Build components array
    components = []
    for obs in event.observations:
        components.append({
            "code": obs.code,
            "kind": obs.kind,
            "value": obs.value,
            "value_type": obs.value_type,
            "label": obs.label,
            "raw_answer": obs.raw_answer,
            "position": obs.position,
            "missing": obs.missing,
        })

    # Extract total score and severity
    total_score = _extract_total_score(event)
    severity_label = _extract_severity(event)
    severity_code = _severity_label_to_code(severity_label) if severity_label else None

    now = datetime.now(timezone.utc).isoformat()

    return {
        "idem_key": idem_key,
        "observation_id": event.measurement_event_id,
        "measurement_event_id": measurement_event_id,
        "subject_id": event.subject_id,
        "measure_code": event.measure_id,
        "measure_version": event.measure_version,
        "value_numeric": total_score,
        "value_text": None,
        "unit": "score",
        "severity_code": severity_code,
        "severity_label": severity_label,
        "components": json.dumps(components),
        "metadata": json.dumps({}),
        "correlation_id": correlation_id,
        "processed_at": now,
        "created_at": now,
    }


def _extract_total_score(event: Any) -> float | None:
    """Extract the total score from a MeasurementEvent."""
    for obs in event.observations:
        if obs.kind == "scale" and obs.code.endswith("_total"):
            if obs.value is not None:
                try:
                    return float(obs.value)
                except (TypeError, ValueError):
                    pass
    return None


def _extract_severity(event: Any) -> str | None:
    """Extract severity label from a MeasurementEvent."""
    for obs in event.observations:
        if obs.kind == "scale" and obs.code.endswith("_total"):
            if obs.label:
                return obs.label
    return None


def _severity_label_to_code(label: str) -> str | None:
    """Map severity label to standardized code."""
    if not label:
        return None
    label_lower = label.lower()
    if "none" in label_lower or "minimal" in label_lower:
        return "none"
    elif "mild" in label_lower:
        return "mild"
    elif "moderate" in label_lower:
        if "severe" in label_lower:
            return "moderate_severe"
        return "moderate"
    elif "severe" in label_lower:
        return "severe"
    return None
