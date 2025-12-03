-- Canonical objects summary by schema and source
-- Shows count of canonical objects grouped by schema and source system

SELECT
  canonical_schema,
  source_system,
  COUNT(*) AS count
FROM `${PROJECT}.${DATASET}.canonical_objects`
GROUP BY canonical_schema, source_system
ORDER BY count DESC;
