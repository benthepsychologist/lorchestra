"""Curation SQL for Molt context projections.

These queries curate subsets of canonical data for the Molt chatbot.
Each projection runs as a CREATE OR REPLACE TABLE in the target project
(molt-chatbot.molt), executed by CrossProjectSyncProcessor.

Placeholders:
    {project}  - Source GCP project ID (e.g., local-orchestration)
    {dataset}  - Source BQ dataset (e.g., canonical)

Usage:
    from lorchestra.sql.molt_projections import get_molt_projection_sql

    sql = get_molt_projection_sql("context_emails", project="local-orchestration", dataset="canonical")
"""

# =============================================================================
# context_emails — All emails from the last 14 days (sent + received)
# =============================================================================
CONTEXT_EMAILS = """
SELECT
  JSON_VALUE(payload, '$.id') AS email_id,
  JSON_VALUE(payload, '$.threadId') AS thread_id,
  JSON_VALUE(payload, '$.from[0].email') AS sender_email,
  JSON_VALUE(payload, '$.from[0].name') AS sender_name,
  JSON_VALUE(payload, '$.subject') AS subject,
  JSON_VALUE(payload, '$.preview') AS snippet,
  TIMESTAMP(JSON_VALUE(payload, '$.receivedAt')) AS received_at,
  ARRAY(
    SELECT label FROM UNNEST(
      JSON_EXTRACT_ARRAY(payload, '$.labels')
    ) AS label
  ) AS labels,
  IFNULL(JSON_VALUE(payload, '$.keywords.$flagged'), 'false') = 'true' AS is_flagged,
  IFNULL(JSON_VALUE(payload, '$.keywords.$seen'), 'false') != 'true' AS is_unread,
  (
    IFNULL(JSON_VALUE(payload, '$.keywords.$seen'), 'false') != 'true'
    AND JSON_VALUE(payload, '$.from[0].email') NOT LIKE '%noreply%'
    AND JSON_VALUE(payload, '$.from[0].email') NOT LIKE '%no-reply%'
  ) AS needs_response,
  source_system,
  connection_name,
  idem_key,
  CURRENT_TIMESTAMP() AS projected_at
FROM `{project}.{dataset}.canonical_objects`
WHERE canonical_schema = 'iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0'
  AND TIMESTAMP(JSON_VALUE(payload, '$.receivedAt')) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
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
    """Get molt projection SQL with project and dataset substituted.

    Args:
        name: Projection name (e.g., 'context_emails')
        project: Source GCP project ID (e.g., 'local-orchestration')
        dataset: Source BQ dataset name (e.g., 'canonical')

    Returns:
        SQL string with placeholders replaced

    Raises:
        KeyError: If projection name not found
    """
    if name not in MOLT_PROJECTIONS:
        raise KeyError(
            f"Unknown molt projection: {name}. "
            f"Available: {list(MOLT_PROJECTIONS.keys())}"
        )

    sql_template = MOLT_PROJECTIONS[name]
    return sql_template.format(project=project, dataset=dataset)
