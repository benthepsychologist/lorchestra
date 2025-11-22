-- Create event_log table (audit trail)
--
-- This table stores event envelopes (metadata only, NO payload).
-- Each emit() call creates one row here for audit tracking.
--
-- Partitioning: By created_at date for efficient time-range queries
-- Clustering: By source_system, object_type, event_type for fast filtering
--
-- Usage:
--   Replace YOUR_PROJECT and YOUR_DATASET with actual values
--   bq query --use_legacy_sql=false < create_event_log.sql

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT.YOUR_DATASET.event_log` (
  -- Unique identifier for this event emission
  event_id STRING NOT NULL,

  -- Event type (e.g., "gmail.email.received")
  event_type STRING NOT NULL,

  -- Source system that produced the data (e.g., "tap-gmail--acct1-personal")
  source_system STRING NOT NULL,

  -- Object type (e.g., "email", "contact", "questionnaire_response")
  object_type STRING NOT NULL,

  -- Idempotency key - references raw_objects table
  idem_key STRING NOT NULL,

  -- Optional correlation ID for tracing (e.g., run_id)
  correlation_id STRING,

  -- Optional subject identifier (PHI - handle carefully)
  subject_id STRING,

  -- Event creation timestamp
  created_at TIMESTAMP NOT NULL,

  -- Event status ("ok" | "error")
  status STRING NOT NULL,

  -- Error message if status="error"
  error_message STRING
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, object_type, event_type
OPTIONS(
  description="Event audit trail - one row per emit() call, no payload"
);
