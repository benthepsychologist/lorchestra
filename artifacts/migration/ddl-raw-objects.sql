-- DDL for raw_objects table (production and test)
-- Migration: Standardize Source System Columns

-- ============================================================================
-- PRODUCTION TABLE
-- ============================================================================

-- Step 1: Archive existing data (optional)
-- CREATE TABLE `lorchestra_events.raw_objects_legacy` AS
-- SELECT * FROM `lorchestra_events.raw_objects`;

-- Step 2: Drop existing table
DROP TABLE IF EXISTS `lorchestra_events.raw_objects`;

-- Step 3: Create new table with updated schema
CREATE TABLE `lorchestra_events.raw_objects` (
  idem_key STRING NOT NULL,           -- opaque: {source_system}:{connection_name}:{object_type}:{external_id}
  source_system STRING NOT NULL,      -- provider: gmail, exchange, dataverse, google_forms
  connection_name STRING NOT NULL,    -- account: gmail-acct1, exchange-ben-mensio
  object_type STRING NOT NULL,        -- domain object: email, contact, session, form_response
  schema_ref STRING,                  -- iglu:com.mensio.raw/raw_gmail_email/jsonschema/1-0-0 (nullable)
  external_id STRING,                 -- upstream ID (Gmail message_id, Dataverse GUID, etc.)
  payload JSON NOT NULL,
  first_seen TIMESTAMP NOT NULL,
  last_seen TIMESTAMP NOT NULL
)
CLUSTER BY source_system, connection_name, object_type;

-- ============================================================================
-- TEST TABLE
-- ============================================================================

-- Step 1: Drop existing test table
DROP TABLE IF EXISTS `lorchestra_events.test_raw_objects`;

-- Step 2: Create new test table with same schema
CREATE TABLE `lorchestra_events.test_raw_objects` (
  idem_key STRING NOT NULL,
  source_system STRING NOT NULL,
  connection_name STRING NOT NULL,
  object_type STRING NOT NULL,
  schema_ref STRING,
  external_id STRING,
  payload JSON NOT NULL,
  first_seen TIMESTAMP NOT NULL,
  last_seen TIMESTAMP NOT NULL
)
CLUSTER BY source_system, connection_name, object_type;
