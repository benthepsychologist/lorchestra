-- DDL for event_log table (production and test)
-- Migration: Standardize Source System Columns

-- ============================================================================
-- PRODUCTION TABLE
-- ============================================================================

-- Step 1: Archive existing data (optional)
-- CREATE TABLE `lorchestra_events.event_log_legacy` AS
-- SELECT * FROM `lorchestra_events.event_log`;

-- Step 2: Drop existing table
DROP TABLE IF EXISTS `lorchestra_events.event_log`;

-- Step 3: Create new table with updated schema
CREATE TABLE `lorchestra_events.event_log` (
  event_id STRING NOT NULL,
  event_type STRING NOT NULL,         -- verb: job.started, ingest.completed, upsert.completed
  source_system STRING NOT NULL,      -- provider (or 'lorchestra' for system events)
  connection_name STRING,             -- account (NULL for system events)
  target_object_type STRING,          -- noun: email, session, invoice (NULL for job events)
  event_schema_ref STRING,            -- iglu:com.mensio.event/ingest_completed/jsonschema/1-0-0
  correlation_id STRING,
  trace_id STRING,
  created_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  error_message STRING,
  payload JSON
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, event_type;

-- ============================================================================
-- TEST TABLE
-- ============================================================================

-- Step 1: Drop existing test table
DROP TABLE IF EXISTS `lorchestra_events.test_event_log`;

-- Step 2: Create new test table with same schema
CREATE TABLE `lorchestra_events.test_event_log` (
  event_id STRING NOT NULL,
  event_type STRING NOT NULL,
  source_system STRING NOT NULL,
  connection_name STRING,
  target_object_type STRING,
  event_schema_ref STRING,
  correlation_id STRING,
  trace_id STRING,
  created_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  error_message STRING,
  payload JSON
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, event_type;
