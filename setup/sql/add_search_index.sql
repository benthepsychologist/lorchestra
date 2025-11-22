-- Add search index to raw_objects for full-text search on JSON payloads
--
-- This dramatically speeds up searches within JSON data.
-- Use this when you need to search email subjects, bodies, or other text fields.
--
-- Prerequisites:
-- - raw_objects table exists
-- - Some data has been ingested (index works better with data)
--
-- Usage:
--   Replace YOUR_PROJECT and YOUR_DATASET
--   bq query --use_legacy_sql=false < add_search_index.sql
--
-- Cost: No additional cost, but uses storage for the index
-- Performance: 10-100x faster for text searches

CREATE SEARCH INDEX IF NOT EXISTS idx_payload_search
ON `YOUR_PROJECT.YOUR_DATASET.raw_objects`(ALL COLUMNS)
OPTIONS(
  analyzer='LOG_ANALYZER'  -- Good for log-like data, email, JSON
  -- Alternative: analyzer='STANDARD' for general text
);

-- After creating, test with:
-- SELECT idem_key, JSON_VALUE(payload, '$.subject') as subject
-- FROM `YOUR_PROJECT.YOUR_DATASET.raw_objects`
-- WHERE SEARCH(payload, 'urgent meeting')
