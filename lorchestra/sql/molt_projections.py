"""Curation SQL for Molt context projections.

These queries curate subsets of canonical data for the Molt chatbot.
Each projection runs as a CREATE OR REPLACE TABLE in the target project
(molt-chatbot.molt), executed by CrossProjectSyncProcessor.

Placeholders:
    {project}  - Source GCP project ID (e.g., local-orchestration)
    {dataset}  - Source BQ dataset (e.g., canonical)

PHI clinical detection config is loaded from phi_clinical.yaml (same directory).
See that file for documentation on maintaining clinical sender domains and
practice recipient email addresses.

Usage:
    from lorchestra.sql.molt_projections import get_molt_projection_sql

    sql = get_molt_projection_sql("context_emails", project="local-orchestration", dataset="canonical")
"""

from __future__ import annotations

from pathlib import Path

import yaml

_PHI_CONFIG_PATH = Path(__file__).parent / "phi_clinical.yaml"


def _load_phi_config() -> dict:
    """Load PHI clinical detection config from YAML.

    Returns dict with keys: clinical_sender_domains, practice_recipient_emails.
    Raises FileNotFoundError if config is missing (required for PHI safety).
    """
    with open(_PHI_CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _to_sql_in(values: list[str]) -> str:
    """Convert a list of strings to a SQL IN clause body.

    ['a.com', 'b.com'] -> "'a.com', 'b.com'"
    """
    return ", ".join(f"'{v}'" for v in values)

# =============================================================================
# context_emails — All emails from the last 14 days (sent + received)
#
# PHI scrubbing: Emails flagged as clinical have sender identity redacted
# and snippet stripped. Non-clinical emails pass through fully.
#
# Clinical detection signals (any match triggers scrubbing):
#   1. Contact match  — sender email matches a client in contact table
#   2. Clinical domain — sender domain is a known clinical platform
#   3. Practice recipient — email was sent TO a specific practice mailbox
#
# Signals 2 and 3 are configured in phi_clinical.yaml (same directory).
# =============================================================================
CONTEXT_EMAILS = """
WITH email_base AS (
  SELECT
    e.payload,
    JSON_VALUE(e.payload, '$.id') AS email_id,
    JSON_VALUE(e.payload, '$.threadId') AS thread_id,
    JSON_VALUE(e.payload, '$.from[0].email') AS raw_sender_email,
    JSON_VALUE(e.payload, '$.from[0].name') AS raw_sender_name,
    JSON_VALUE(e.payload, '$.subject') AS subject,
    JSON_VALUE(e.payload, '$.preview') AS raw_snippet,
    TIMESTAMP(JSON_VALUE(e.payload, '$.receivedAt')) AS received_at,
    ARRAY(
      SELECT label FROM UNNEST(
        JSON_EXTRACT_ARRAY(e.payload, '$.labels')
      ) AS label
    ) AS labels,
    IFNULL(JSON_VALUE(e.payload, '$.keywords.$flagged'), 'false') = 'true' AS is_flagged,
    IFNULL(JSON_VALUE(e.payload, '$.keywords.$seen'), 'false') != 'true' AS is_unread,
    (
      IFNULL(JSON_VALUE(e.payload, '$.keywords.$seen'), 'false') != 'true'
      AND JSON_VALUE(e.payload, '$.from[0].email') NOT LIKE '%noreply%'
      AND JSON_VALUE(e.payload, '$.from[0].email') NOT LIKE '%no-reply%'
    ) AS needs_response,
    cli.contact_id AS clinical_contact_id,
    e.source_system,
    e.connection_name,
    e.idem_key
  FROM `{project}.{dataset}.canonical_objects` e
  LEFT JOIN (
    SELECT
      JSON_VALUE(payload, '$.id') AS contact_id,
      LOWER(JSON_VALUE(payload, '$.telecom.email')) AS contact_email,
      LOWER(JSON_VALUE(payload, '$.telecom.emailSecondary')) AS contact_email_secondary
    FROM `{project}.{dataset}.canonical_objects`
    WHERE canonical_schema = 'iglu:org.canonical/contact/jsonschema/2-0-0'
  ) cli
    ON LOWER(JSON_VALUE(e.payload, '$.from[0].email')) = cli.contact_email
    OR LOWER(JSON_VALUE(e.payload, '$.from[0].email')) = cli.contact_email_secondary
  WHERE e.canonical_schema = 'iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0'
    AND TIMESTAMP(JSON_VALUE(e.payload, '$.receivedAt'))
        > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
)
SELECT
  email_id,
  thread_id,
  CASE WHEN is_clinical
    THEN CONCAT(
      LOWER(SUBSTR(raw_sender_email, 1, 1)),
      '*****@',
      REGEXP_EXTRACT(raw_sender_email, r'@(.+)$')
    )
    ELSE raw_sender_email
  END AS sender_email,
  CASE WHEN is_clinical
    THEN CONCAT('Client ', UPPER(SUBSTR(raw_sender_name, 1, 1)), '.')
    ELSE raw_sender_name
  END AS sender_name,
  subject,
  CASE WHEN is_clinical THEN NULL ELSE raw_snippet END AS snippet,
  received_at,
  labels,
  is_flagged,
  is_unread,
  needs_response,
  clinical_contact_id,
  is_clinical,
  source_system,
  connection_name,
  idem_key,
  CURRENT_TIMESTAMP() AS projected_at
FROM (
  SELECT
    *,
    (
      -- Signal 1: sender matches a known client contact
      clinical_contact_id IS NOT NULL
      -- Signal 2: sender domain is a known clinical referral platform
      OR REGEXP_EXTRACT(LOWER(raw_sender_email), r'@(.+)$')
         IN ({clinical_sender_domains})
      -- Signal 3: email was sent TO a specific practice mailbox
      OR EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(payload, '$.to')) AS addr
        WHERE LOWER(JSON_VALUE(addr, '$.email'))
          IN ({practice_recipient_emails})
      )
    ) AS is_clinical
  FROM email_base
)
ORDER BY received_at DESC
"""

# =============================================================================
# context_calendar — Sessions from last 14 days + next 14 days
# =============================================================================
CONTEXT_CALENDAR = """
SELECT
  JSON_VALUE(s.payload, '$.id') AS session_id,
  JSON_VALUE(s.payload, '$.title') AS title,
  c_name.client_name,
  CAST(JSON_VALUE(s.payload, '$.sessionNumber') AS INT64) AS session_num,
  TIMESTAMP(JSON_VALUE(s.payload, '$.period.start')) AS scheduled_start,
  TIMESTAMP(JSON_VALUE(s.payload, '$.period.end')) AS scheduled_end,
  TIMESTAMP(JSON_VALUE(s.payload, '$.meta.updatedAt')) AS actual_start,
  CAST(JSON_VALUE(s.payload, '$.duration') AS INT64) AS duration_minutes,
  JSON_VALUE(s.payload, '$.status') AS status,
  JSON_VALUE(s.payload, '$.type') AS session_type,
  JSON_VALUE(s.payload, '$.subject.reference') AS contact_id,
  s.idem_key,
  CURRENT_TIMESTAMP() AS projected_at
FROM `{project}.{dataset}.canonical_objects` s
LEFT JOIN (
  SELECT
    REGEXP_EXTRACT(JSON_VALUE(payload, '$.id'), r'(.+)') AS contact_id,
    CONCAT(
      IFNULL(JSON_VALUE(payload, '$.name.given'), ''),
      ' ',
      IFNULL(SUBSTR(JSON_VALUE(payload, '$.name.family'), 1, 1), '')
    ) AS client_name
  FROM `{project}.{dataset}.canonical_objects`
  WHERE canonical_schema = 'iglu:org.canonical/contact/jsonschema/2-0-0'
) c_name ON REGEXP_EXTRACT(JSON_VALUE(s.payload, '$.subject.reference'), r'/(.+)$') = c_name.contact_id
WHERE s.canonical_schema = 'iglu:org.canonical/clinical_session/jsonschema/2-0-0'
  AND TIMESTAMP(JSON_VALUE(s.payload, '$.period.start'))
      BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
          AND TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
ORDER BY scheduled_start ASC
"""

# =============================================================================
# context_actions — Pending actions from molt.actions (same project as target)
# Note: This query reads from molt-chatbot.molt.actions directly, not from
# local-orchestration. The {project}/{dataset} placeholders are not used here.
# =============================================================================
CONTEXT_ACTIONS = """
SELECT
  action_id,
  user_id,
  channel,
  action_type,
  context,
  instruction,
  created_at,
  status,
  CAST(NULL AS STRING) AS idem_key,
  CURRENT_TIMESTAMP() AS projected_at
FROM `molt-chatbot.molt.actions`
WHERE status = 'pending'
   OR (status = 'completed' AND created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
ORDER BY
  CASE WHEN status = 'pending' THEN 0 ELSE 1 END,
  created_at DESC
"""

# Registry of all molt projection queries
MOLT_PROJECTIONS: dict[str, str] = {
    "context_emails": CONTEXT_EMAILS,
    "context_calendar": CONTEXT_CALENDAR,
    "context_actions": CONTEXT_ACTIONS,
}


def get_molt_projection_sql(name: str, project: str, dataset: str) -> str:
    """Get molt projection SQL with project, dataset, and PHI config substituted.

    For context_emails, also loads phi_clinical.yaml and injects clinical
    detection config (sender domains, practice recipient emails).

    Args:
        name: Projection name (e.g., 'context_emails')
        project: Source GCP project ID (e.g., 'local-orchestration')
        dataset: Source BQ dataset name (e.g., 'canonical')

    Returns:
        SQL string with placeholders replaced

    Raises:
        KeyError: If projection name not found
        FileNotFoundError: If phi_clinical.yaml is missing (context_emails only)
    """
    if name not in MOLT_PROJECTIONS:
        raise KeyError(
            f"Unknown molt projection: {name}. "
            f"Available: {list(MOLT_PROJECTIONS.keys())}"
        )

    sql_template = MOLT_PROJECTIONS[name]

    format_args: dict[str, str] = {"project": project, "dataset": dataset}

    if name == "context_emails":
        phi_config = _load_phi_config()
        format_args["clinical_sender_domains"] = _to_sql_in(
            phi_config["clinical_sender_domains"]
        )
        format_args["practice_recipient_emails"] = _to_sql_in(
            phi_config["practice_recipient_emails"]
        )

    return sql_template.format(**format_args)
