-- Recent job events from the last 7 days
-- Shows job events grouped by type, source, and status

SELECT
  event_type,
  source_system,
  status,
  COUNT(*) AS count,
  MIN(created_at) AS first_seen,
  MAX(created_at) AS last_seen
FROM `${PROJECT}.${DATASET}.event_log`
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY event_type, source_system, status
ORDER BY last_seen DESC;
