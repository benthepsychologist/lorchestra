-- Alter event_log table to support refactored event architecture
--
-- Changes:
-- 1. Make idem_key nullable (telemetry events don't have idem_keys)
-- 2. Add payload column (JSON, nullable) for small telemetry data
-- 3. Add trace_id field (STRING, nullable) for future cross-system tracing
--
-- Usage:
--   Replace YOUR_PROJECT and YOUR_DATASET with actual values
--   bq query --use_legacy_sql=false < alter_event_log.sql

-- Make idem_key nullable
ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.event_log`
  ALTER COLUMN idem_key DROP NOT NULL;

-- Add payload column for small telemetry data (job params, counts, duration)
ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.event_log`
  ADD COLUMN IF NOT EXISTS payload JSON OPTIONS(description="Small telemetry payload for event metadata (NOT full objects)");

-- Add trace_id for future cross-system tracing
ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.event_log`
  ADD COLUMN IF NOT EXISTS trace_id STRING OPTIONS(description="Optional trace ID for cross-system request tracing");
