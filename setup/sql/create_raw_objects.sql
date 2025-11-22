-- Create raw_objects table (deduped object store)
--
-- This table stores object payloads, deduplicated by idem_key.
-- Multiple emit() calls with the same idem_key → one row (last_seen updated).
--
-- Clustering: By source_system, object_type for efficient filtering
-- Primary Key: idem_key (NOT ENFORCED - advisory only)
--
-- Usage:
--   Replace YOUR_PROJECT and YOUR_DATASET with actual values
--   bq query --use_legacy_sql=false < create_raw_objects.sql

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT.YOUR_DATASET.raw_objects` (
  -- Idempotency key - stable identity based on content
  -- Format: "{object_type}:{source_system}:{natural_id_or_hash}"
  idem_key STRING NOT NULL,

  -- Source system that produced the data
  source_system STRING NOT NULL,

  -- Object type (e.g., "email", "contact")
  object_type STRING NOT NULL,

  -- External ID from source system (e.g., Gmail message_id)
  external_id STRING,

  -- Full object payload as JSON
  payload JSON NOT NULL,

  -- First time this object was seen
  first_seen TIMESTAMP NOT NULL,

  -- Last time this object was seen (updated on re-ingestion)
  last_seen TIMESTAMP NOT NULL
)
CLUSTER BY source_system, object_type
OPTIONS(
  description="Deduped object store - one row per idem_key, with payload"
);

-- Add primary key constraint (advisory only, not enforced at insert time)
ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.raw_objects`
ADD PRIMARY KEY (idem_key) NOT ENFORCED;
