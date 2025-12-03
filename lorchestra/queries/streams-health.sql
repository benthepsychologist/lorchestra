-- Quick health check: is anything dead?
-- Shows hours since last successful stage completion per stream

SELECT
  source_system,
  connection_name,
  JSON_VALUE(payload, '$.object_type') AS object_type,
  MAX(created_at) AS last_success_at,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR) AS hours_since_last
FROM `${PROJECT}.${DATASET}.event_log`
WHERE event_type IN (
  'ingestion.completed',
  'validation.completed',
  'canonization.completed'
)
  AND status = 'success'
GROUP BY source_system, connection_name, object_type
ORDER BY hours_since_last DESC;
