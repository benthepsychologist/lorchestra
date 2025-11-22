-- Create materialized view for email objects with extracted JSON fields
--
-- This pre-computes commonly queried fields from email JSON payloads.
-- Queries against this view are 10-100x faster than querying raw_objects.
--
-- Prerequisites:
-- - raw_objects table exists
-- - Email data has been ingested (object_type='email')
--
-- Usage:
--   Replace YOUR_PROJECT and YOUR_DATASET
--   bq query --use_legacy_sql=false < create_materialized_view_emails.sql
--
-- Cost: Additional storage for materialized view (typically 20-50% of source)
-- Refresh: Automatic, typically within seconds to minutes

CREATE MATERIALIZED VIEW IF NOT EXISTS `YOUR_PROJECT.YOUR_DATASET.emails_mv`
AS
SELECT
  -- Primary keys
  idem_key,
  source_system,

  -- Extracted email fields from JSON payload
  JSON_VALUE(payload, '$.id') as message_id,
  JSON_VALUE(payload, '$.threadId') as thread_id,
  JSON_VALUE(payload, '$.subject') as subject,

  -- Sender/recipient
  JSON_VALUE(payload, '$.from') as from_email,
  JSON_QUERY_ARRAY(payload, '$.to') as to_emails,
  JSON_QUERY_ARRAY(payload, '$.cc') as cc_emails,

  -- Dates
  PARSE_TIMESTAMP('%a, %d %b %Y %H:%M:%S %z',
    JSON_VALUE(payload, '$.date')
  ) as email_date,

  -- Labels and flags
  JSON_QUERY_ARRAY(payload, '$.labelIds') as labels,
  JSON_VALUE(payload, '$.snippet') as snippet,

  -- Timestamps
  first_seen,
  last_seen,

  -- Keep full payload for reference
  payload
FROM `YOUR_PROJECT.YOUR_DATASET.raw_objects`
WHERE object_type = 'email';

-- Example queries after creation:
--
-- -- Find emails by subject
-- SELECT message_id, subject, from_email, email_date
-- FROM `YOUR_PROJECT.YOUR_DATASET.emails_mv`
-- WHERE subject LIKE '%meeting%'
-- ORDER BY email_date DESC;
--
-- -- Count emails by sender
-- SELECT from_email, COUNT(*) as email_count
-- FROM `YOUR_PROJECT.YOUR_DATASET.emails_mv`
-- GROUP BY from_email
-- ORDER BY email_count DESC;
--
-- -- Find unread emails
-- SELECT message_id, subject, email_date
-- FROM `YOUR_PROJECT.YOUR_DATASET.emails_mv`
-- WHERE 'UNREAD' IN UNNEST(labels);
