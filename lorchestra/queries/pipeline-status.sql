-- Pipeline status: last activity per stream (30-day window)
-- Shows when each stage last ran and cumulative record counts

SELECT
  source_system,
  connection_name,
  JSON_VALUE(payload, '$.object_type') AS object_type,
  MAX(IF(event_type = 'ingestion.completed', created_at, NULL)) AS last_ingestion,
  MAX(IF(event_type = 'validation.completed', created_at, NULL)) AS last_validation,
  MAX(IF(event_type = 'canonization.completed', created_at, NULL)) AS last_canonization,
  MAX(IF(status = 'failed', created_at, NULL)) AS last_failure,
  SUM(IF(event_type = 'ingestion.completed',
      CAST(JSON_VALUE(payload, '$.records_extracted') AS INT64), 0)) AS total_extracted,
  SUM(IF(event_type = 'ingestion.completed',
      CAST(JSON_VALUE(payload, '$.records_inserted') AS INT64), 0)) AS total_inserted
FROM `${PROJECT}.${DATASET}.event_log`
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY source_system, connection_name, object_type
ORDER BY source_system, connection_name, object_type;
