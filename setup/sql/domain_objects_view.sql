-- Create domain.objects view over wal.domain_events for transcript analysis objects
--
-- NOTE: Canonical creation path is the view job:
--   - lorchestra/sql/projections.py (projection_name: domain_objects)
--   - lorchestra/jobs/definitions/projection/view/view_domain_objects.yaml
--   - lorchestra/jobs/definitions/pipeline/pipeline.views.yaml
--
-- object_variant values:
-- - evidence
-- - therapeutic_work
-- - emotional_relational
-- - client_model_delta
-- - goal_progress
-- - narrative
-- - client_model

CREATE OR REPLACE VIEW `local-orchestration.wal.domain_objects` AS
SELECT
  event_id,
  aggregate_id,
  aggregate_type,
  event_type,
  subject_id,
  occurred_at AS synthesized_at,
  ingested_at AS created_at,
  JSON_VALUE(payload, '$.idem_key') AS idem_key,
  JSON_VALUE(payload, '$.entity_id') AS entity_id,
  JSON_VALUE(payload, '$.object_type') AS object_type,
  JSON_VALUE(payload, '$.object_variant') AS object_variant,
  JSON_VALUE(payload, '$.content') AS content,
  JSON_VALUE(payload, '$.model_used') AS model_used,
  JSON_QUERY(payload, '$.inference_audit') AS inference_audit,
  payload,
  producer_service,
  producer_component,
  correlation_id
FROM `local-orchestration.wal.domain_events`
WHERE aggregate_type = 'domain_object'
  AND event_type NOT LIKE '%.deleted'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY aggregate_id
  ORDER BY occurred_at DESC, event_id DESC
) = 1;
