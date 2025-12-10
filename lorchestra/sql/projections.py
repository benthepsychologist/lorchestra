"""BigQuery SQL projections for therapist surface views.

This module contains all projection SQL for the therapist surface pipeline.
Each projection is a CREATE OR REPLACE VIEW statement that extracts and
flattens data from the canonical_objects table.

Naming convention: proj_<entity>

Available projections:
    proj_clients           - Therapy clients with folder naming
    proj_sessions          - Clinical sessions with session numbers
    proj_transcripts       - Session transcripts
    proj_clinical_documents - Session notes, summaries, reports
    proj_form_responses    - Form responses linked to clients by email
    proj_contact_events    - Unified operational event ledger (forms + sessions)

Usage:
    from lorchestra.sql.projections import get_projection_sql

    sql = get_projection_sql("proj_clients", project="my-project", dataset="events")
    # Returns: CREATE OR REPLACE VIEW `my-project.events.proj_clients` AS ...

Key design decisions:
    - Clients filtered to client_type_label = 'Therapy' only
    - Session numbers come from source data (session_num), not calculated
    - Content extracted from nested structure: $.content.text
    - Client references extracted from subject.reference: Contact/<id>
    - Form responses linked by matching respondent.email to client email

See docs/projection-pipeline.md for full documentation.
"""

# =============================================================================
# proj_clients - Client/contact information with folder naming
# =============================================================================
PROJ_CLIENTS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.proj_clients` AS
SELECT
    JSON_VALUE(payload, '$.contact_id') AS client_id,
    JSON_VALUE(payload, '$.first_name') AS first_name,
    JSON_VALUE(payload, '$.last_name') AS last_name,
    JSON_VALUE(payload, '$.email') AS email,
    JSON_VALUE(payload, '$.mobile') AS mobile,
    JSON_VALUE(payload, '$.phone') AS phone,
    JSON_VALUE(payload, '$.client_type_label') AS client_type,
    -- Build client folder name: "FirstName L" or just "LastName" if no first name
    CASE
        WHEN JSON_VALUE(payload, '$.first_name') IS NOT NULL
             AND JSON_VALUE(payload, '$.last_name') IS NOT NULL
        THEN CONCAT(JSON_VALUE(payload, '$.first_name'), ' ',
                    SUBSTR(JSON_VALUE(payload, '$.last_name'), 1, 1))
        WHEN JSON_VALUE(payload, '$.last_name') IS NOT NULL
        THEN JSON_VALUE(payload, '$.last_name')
        ELSE JSON_VALUE(payload, '$.contact_id')
    END AS client_folder,
    JSON_VALUE(payload, '$.created_at') AS created_at,
    JSON_VALUE(payload, '$.updated_at') AS updated_at,
    idem_key
FROM `{project}.{dataset}.canonical_objects`
WHERE canonical_schema = 'iglu:org.canonical/contact/jsonschema/2-0-0'
  AND JSON_VALUE(payload, '$.client_type_label') = 'Therapy'
"""

# =============================================================================
# proj_sessions - Clinical sessions with session numbering per client
# =============================================================================
PROJ_SESSIONS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.proj_sessions` AS
SELECT
    JSON_VALUE(payload, '$.session_id') AS session_id,
    JSON_VALUE(payload, '$.contact_id') AS client_id,
    CAST(JSON_VALUE(payload, '$.session_num') AS INT64) AS session_num,
    JSON_VALUE(payload, '$.created_at') AS started_at,
    JSON_VALUE(payload, '$.updated_at') AS ended_at,
    JSON_VALUE(payload, '$.status') AS session_status,
    idem_key
FROM `{project}.{dataset}.canonical_objects`
WHERE canonical_schema = 'iglu:org.canonical/clinical_session/jsonschema/2-0-0'
  AND JSON_VALUE(payload, '$.contact_id') IS NOT NULL
"""

# =============================================================================
# proj_transcripts - Session transcripts
# =============================================================================
PROJ_TRANSCRIPTS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.proj_transcripts` AS
SELECT
    JSON_VALUE(payload, '$.id') AS transcript_id,
    JSON_VALUE(payload, '$.session_source_id') AS session_id,
    REGEXP_EXTRACT(JSON_VALUE(payload, '$.subject.reference'), r'/(.+)$') AS client_id,
    JSON_VALUE(payload, '$.content.text') AS content,
    JSON_VALUE(payload, '$.language') AS language,
    JSON_VALUE(payload, '$.date') AS transcript_date,
    JSON_VALUE(payload, '$.processing.status') AS processing_status,
    JSON_VALUE(payload, '$.meta.createdAt') AS created_at,
    JSON_VALUE(payload, '$.meta.updatedAt') AS updated_at,
    idem_key
FROM `{project}.{dataset}.canonical_objects`
WHERE canonical_schema = 'iglu:org.canonical/session_transcript/jsonschema/2-0-0'
"""

# =============================================================================
# proj_clinical_documents - Session notes and other clinical documents
# =============================================================================
PROJ_CLINICAL_DOCUMENTS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.proj_clinical_documents` AS
SELECT
    JSON_VALUE(payload, '$.id') AS document_id,
    JSON_VALUE(payload, '$.session_source_id') AS session_id,
    REGEXP_EXTRACT(JSON_VALUE(payload, '$.subject.reference'), r'/(.+)$') AS client_id,
    JSON_VALUE(payload, '$.docType') AS doc_type,
    JSON_VALUE(payload, '$.title') AS title,
    JSON_VALUE(payload, '$.content.text') AS content,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.audience') AS audience,
    JSON_VALUE(payload, '$.date') AS document_date,
    JSON_VALUE(payload, '$.meta.createdAt') AS created_at,
    JSON_VALUE(payload, '$.meta.updatedAt') AS updated_at,
    object_type,
    idem_key
FROM `{project}.{dataset}.canonical_objects`
WHERE canonical_schema = 'iglu:org.canonical/clinical_document/jsonschema/2-0-0'
"""

# =============================================================================
# proj_contact_events - Unified operational event ledger for all client-touch events
# =============================================================================
PROJ_CONTACT_EVENTS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.proj_contact_events` AS

-- Source 1: measurement_events (forms)
-- New schema: 1 row per form submission (not per measure)
-- event_subtype = binding_id (intake_01, followup, etc.)
-- occurred_at = when form was submitted
SELECT
    c.client_id AS contact_id,
    m.subject_id AS contact_email,
    m.event_type AS event_type,
    m.event_subtype AS event_name,
    m.source_system AS event_source,
    m.occurred_at AS event_timestamp,
    'measurement_events' AS source_system,
    m.measurement_event_id AS source_record_id,
    TO_JSON_STRING(STRUCT(
        m.binding_id,
        m.binding_version,
        m.form_id,
        m.source_id AS form_submission_id,
        m.canonical_object_id,
        m.idem_key
    )) AS payload
FROM `{project}.{dataset}.measurement_events` m
LEFT JOIN (
    SELECT
        JSON_VALUE(payload, '$.contact_id') AS client_id,
        LOWER(JSON_VALUE(payload, '$.email')) AS email
    FROM `{project}.{dataset}.canonical_objects`
    WHERE canonical_schema = 'iglu:org.canonical/contact/jsonschema/2-0-0'
      AND JSON_VALUE(payload, '$.client_type_label') = 'Therapy'
) c ON LOWER(m.subject_id) = c.email

UNION ALL

-- Source 2: proj_sessions (clinical sessions)
SELECT
    s.client_id AS contact_id,
    c.email AS contact_email,
    'session' AS event_type,
    s.session_status AS event_name,
    'dataverse' AS event_source,
    COALESCE(
        TIMESTAMP(s.started_at),
        TIMESTAMP(s.ended_at)
    ) AS event_timestamp,
    'proj_sessions' AS source_system,
    s.session_id AS source_record_id,
    TO_JSON_STRING(STRUCT(
        s.session_num,
        s.idem_key
    )) AS payload
FROM (
    SELECT
        JSON_VALUE(payload, '$.session_id') AS session_id,
        JSON_VALUE(payload, '$.contact_id') AS client_id,
        CAST(JSON_VALUE(payload, '$.session_num') AS INT64) AS session_num,
        JSON_VALUE(payload, '$.created_at') AS started_at,
        JSON_VALUE(payload, '$.updated_at') AS ended_at,
        JSON_VALUE(payload, '$.status') AS session_status,
        idem_key
    FROM `{project}.{dataset}.canonical_objects`
    WHERE canonical_schema = 'iglu:org.canonical/clinical_session/jsonschema/2-0-0'
      AND JSON_VALUE(payload, '$.contact_id') IS NOT NULL
) s
LEFT JOIN (
    SELECT
        JSON_VALUE(payload, '$.contact_id') AS client_id,
        JSON_VALUE(payload, '$.email') AS email
    FROM `{project}.{dataset}.canonical_objects`
    WHERE canonical_schema = 'iglu:org.canonical/contact/jsonschema/2-0-0'
      AND JSON_VALUE(payload, '$.client_type_label') = 'Therapy'
) c ON s.client_id = c.client_id
"""

# =============================================================================
# proj_form_responses - Form responses (measurement events) linked to clients by email
# =============================================================================
PROJ_FORM_RESPONSES = """
CREATE OR REPLACE VIEW `{project}.{dataset}.proj_form_responses` AS
SELECT
    fr.idem_key,
    JSON_VALUE(fr.payload, '$.response_id') AS response_id,
    JSON_VALUE(fr.payload, '$.respondent.email') AS respondent_email,
    c.client_id,
    JSON_VALUE(fr.payload, '$.status') AS status,
    JSON_VALUE(fr.payload, '$.submitted_at') AS submitted_at,
    JSON_VALUE(fr.payload, '$.last_updated_at') AS last_updated_at,
    JSON_VALUE(fr.payload, '$.source.platform') AS source_platform,
    TO_JSON_STRING(JSON_QUERY(fr.payload, '$.answers')) AS answers_json
FROM `{project}.{dataset}.canonical_objects` fr
LEFT JOIN (
    SELECT
        JSON_VALUE(payload, '$.contact_id') AS client_id,
        LOWER(JSON_VALUE(payload, '$.email')) AS email
    FROM `{project}.{dataset}.canonical_objects`
    WHERE canonical_schema = 'iglu:org.canonical/contact/jsonschema/2-0-0'
) c ON LOWER(JSON_VALUE(fr.payload, '$.respondent.email')) = c.email
WHERE fr.canonical_schema = 'iglu:org.canonical/form_response/jsonschema/1-0-0'
"""

# Registry of all projections
PROJECTIONS: dict[str, str] = {
    "proj_clients": PROJ_CLIENTS,
    "proj_sessions": PROJ_SESSIONS,
    "proj_transcripts": PROJ_TRANSCRIPTS,
    "proj_clinical_documents": PROJ_CLINICAL_DOCUMENTS,
    "proj_form_responses": PROJ_FORM_RESPONSES,
    "proj_contact_events": PROJ_CONTACT_EVENTS,
}


def get_projection_sql(name: str, project: str, dataset: str) -> str:
    """Get projection SQL with project and dataset substituted.

    Args:
        name: Projection name (e.g., 'proj_client_sessions')
        project: GCP project ID
        dataset: BigQuery dataset name

    Returns:
        SQL string with placeholders replaced

    Raises:
        KeyError: If projection name not found
    """
    if name not in PROJECTIONS:
        raise KeyError(f"Unknown projection: {name}. Available: {list(PROJECTIONS.keys())}")

    sql_template = PROJECTIONS[name]
    return sql_template.format(project=project, dataset=dataset)
