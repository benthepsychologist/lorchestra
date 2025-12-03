-- Raw objects that haven't been updated recently
-- Shows objects not seen in the last 30 days, possibly stale

SELECT
  source_system,
  object_type,
  COUNT(*) AS count,
  MAX(last_seen) AS most_recent
FROM `${PROJECT}.${DATASET}.raw_objects`
WHERE last_seen < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY source_system, object_type
ORDER BY most_recent ASC;
