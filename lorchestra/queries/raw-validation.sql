-- Raw objects validation status breakdown
-- Shows count of raw objects by source, type, and validation status

SELECT
  source_system,
  object_type,
  validation_status,
  COUNT(*) AS count
FROM `${PROJECT}.${DATASET}.raw_objects`
GROUP BY source_system, object_type, validation_status
ORDER BY source_system, object_type, validation_status;
