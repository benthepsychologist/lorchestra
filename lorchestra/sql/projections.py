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
    JSON_VALUE(payload, '$.client_type_code') AS client_type_code,
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
  -- Filter to Therapy clients: client_type_code contains '1'
  -- Handles single value '1' and comma-separated '1,3,6'
  AND '1' IN UNNEST(SPLIT(JSON_VALUE(payload, '$.client_type_code'), ','))
  AND JSON_VALUE(payload, '$.lifecycleStage') = 'active'
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

-- Therapy clients CTE (used by all sources for contact resolution)
-- Filter: client_type_code contains '1' (Therapy)
-- NOTE: Deduped by email to handle duplicate contact records in source data
WITH therapy_clients AS (
    SELECT client_id, email
    FROM (
        SELECT
            JSON_VALUE(payload, '$.contact_id') AS client_id,
            LOWER(JSON_VALUE(payload, '$.email')) AS email,
            ROW_NUMBER() OVER (PARTITION BY LOWER(JSON_VALUE(payload, '$.email')) ORDER BY JSON_VALUE(payload, '$.updated_at') DESC) AS rn
        FROM `{project}.{dataset}.canonical_objects`
        WHERE canonical_schema = 'iglu:org.canonical/contact/jsonschema/2-0-0'
          AND '1' IN UNNEST(SPLIT(JSON_VALUE(payload, '$.client_type_code'), ','))
          AND JSON_VALUE(payload, '$.lifecycleStage') = 'active'
          AND JSON_VALUE(payload, '$.email') IS NOT NULL
    )
    WHERE rn = 1
),

-- Stripe customers (for payment/refund contact resolution)
-- NOTE: Deduped by customer_id due to duplicate connections
stripe_customers AS (
    SELECT customer_id, email
    FROM (
        SELECT
            JSON_VALUE(payload, '$.customer_id') AS customer_id,
            LOWER(JSON_VALUE(payload, '$.email')) AS email,
            ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.customer_id') ORDER BY created_at DESC) AS rn
        FROM `{project}.{dataset}.canonical_objects`
        WHERE canonical_schema = 'iglu:org.canonical/customer/jsonschema/1-0-0'
    )
    WHERE rn = 1
),

-- My email addresses (for determining email direction)
my_emails AS (
    SELECT email FROM UNNEST([
        'ben@mensiomentalhealth.com',
        'ben@benthepsychologist.com',
        'benthepsychologist@gmail.com'
    ]) AS email
)

-- Source 1: measurement_events (forms)
-- event_subtype = binding_id (intake_01, followup, etc.)
SELECT
    c.client_id AS contact_id,
    m.subject_id AS contact_email,
    m.event_type AS event_type,
    m.event_subtype AS event_name,
    'google_forms' AS event_source,
    m.occurred_at AS event_timestamp,
    'measurement_events' AS source_system,
    m.measurement_event_id AS source_record_id,
    TO_JSON_STRING(STRUCT(
        m.binding_id,
        m.binding_version,
        m.form_id,
        m.source_id AS form_submission_id,
        m.canonical_object_id
    )) AS payload
FROM `{project}.{dataset}.measurement_events` m
LEFT JOIN therapy_clients c ON LOWER(m.subject_id) = c.email

UNION ALL

-- Source 2: dataverse_sessions (clinical sessions)
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
    'dataverse_sessions' AS source_system,
    s.session_id AS source_record_id,
    TO_JSON_STRING(STRUCT(
        s.session_num
    )) AS payload
FROM (
    SELECT
        JSON_VALUE(payload, '$.session_id') AS session_id,
        JSON_VALUE(payload, '$.contact_id') AS client_id,
        CAST(JSON_VALUE(payload, '$.session_num') AS INT64) AS session_num,
        JSON_VALUE(payload, '$.created_at') AS started_at,
        JSON_VALUE(payload, '$.updated_at') AS ended_at,
        JSON_VALUE(payload, '$.status') AS session_status
    FROM `{project}.{dataset}.canonical_objects`
    WHERE canonical_schema = 'iglu:org.canonical/clinical_session/jsonschema/2-0-0'
      AND JSON_VALUE(payload, '$.contact_id') IS NOT NULL
) s
LEFT JOIN therapy_clients c ON s.client_id = c.client_id

UNION ALL

-- Source 3: email_gmail (Gmail emails)
-- NOTE: Deduped by email_id due to legacy idem_key suffixes
SELECT
    c.client_id AS contact_id,
    e.contact_email,
    'email' AS event_type,
    CONCAT(e.direction, ': ', SUBSTR(COALESCE(e.subject, '(no subject)'), 1, 50)) AS event_name,
    'gmail' AS event_source,
    TIMESTAMP(e.sent_at) AS event_timestamp,
    'email_gmail' AS source_system,
    e.email_id AS source_record_id,
    TO_JSON_STRING(STRUCT(
        e.subject,
        e.thread_id
    )) AS payload
FROM (
    SELECT email_id, subject, sent_at, thread_id, direction, contact_email
    FROM (
        SELECT
            JSON_VALUE(payload, '$.id') AS email_id,
            JSON_VALUE(payload, '$.subject') AS subject,
            JSON_VALUE(payload, '$.sentAt') AS sent_at,
            JSON_VALUE(payload, '$.threadId') AS thread_id,
            -- Direction: outbound if from me, inbound otherwise
            CASE
                WHEN LOWER(JSON_VALUE(payload, '$.from[0].email')) IN (SELECT email FROM my_emails)
                THEN 'outbound'
                ELSE 'inbound'
            END AS direction,
            -- Contact email: to[0] if outbound, from[0] if inbound
            CASE
                WHEN LOWER(JSON_VALUE(payload, '$.from[0].email')) IN (SELECT email FROM my_emails)
                THEN LOWER(JSON_VALUE(payload, '$.to[0].email'))
                ELSE LOWER(JSON_VALUE(payload, '$.from[0].email'))
            END AS contact_email,
            ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.id') ORDER BY created_at DESC) AS rn
        FROM `{project}.{dataset}.canonical_objects`
        WHERE canonical_schema = 'iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0'
          AND source_system = 'gmail'
    )
    WHERE rn = 1
) e
LEFT JOIN therapy_clients c ON e.contact_email = c.email

UNION ALL

-- Source 4: email_exchange (Exchange emails)
-- NOTE: Deduped by email_id due to legacy idem_key suffixes
SELECT
    c.client_id AS contact_id,
    e.contact_email,
    'email' AS event_type,
    CONCAT(e.direction, ': ', SUBSTR(COALESCE(e.subject, '(no subject)'), 1, 50)) AS event_name,
    'exchange' AS event_source,
    TIMESTAMP(e.sent_at) AS event_timestamp,
    'email_exchange' AS source_system,
    e.email_id AS source_record_id,
    TO_JSON_STRING(STRUCT(
        e.subject,
        e.thread_id
    )) AS payload
FROM (
    SELECT email_id, subject, sent_at, thread_id, direction, contact_email
    FROM (
        SELECT
            JSON_VALUE(payload, '$.id') AS email_id,
            JSON_VALUE(payload, '$.subject') AS subject,
            JSON_VALUE(payload, '$.sentAt') AS sent_at,
            JSON_VALUE(payload, '$.threadId') AS thread_id,
            -- Direction: outbound if from me, inbound otherwise
            CASE
                WHEN LOWER(JSON_VALUE(payload, '$.from[0].email')) IN (SELECT email FROM my_emails)
                THEN 'outbound'
                ELSE 'inbound'
            END AS direction,
            -- Contact email: to[0] if outbound, from[0] if inbound
            CASE
                WHEN LOWER(JSON_VALUE(payload, '$.from[0].email')) IN (SELECT email FROM my_emails)
                THEN LOWER(JSON_VALUE(payload, '$.to[0].email'))
                ELSE LOWER(JSON_VALUE(payload, '$.from[0].email'))
            END AS contact_email,
            ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.id') ORDER BY created_at DESC) AS rn
        FROM `{project}.{dataset}.canonical_objects`
        WHERE canonical_schema = 'iglu:org.canonical/email_jmap_lite/jsonschema/1-0-0'
          AND source_system = 'exchange'
    )
    WHERE rn = 1
) e
LEFT JOIN therapy_clients c ON e.contact_email = c.email

UNION ALL

-- Source 5: stripe_invoice (Stripe invoices)
-- NOTE: Deduped by invoice_id due to duplicate connections
SELECT
    c.client_id AS contact_id,
    inv.customer_email AS contact_email,
    'invoice' AS event_type,
    inv.status AS event_name,
    'stripe' AS event_source,
    TIMESTAMP(inv.event_time) AS event_timestamp,
    'stripe_invoice' AS source_system,
    inv.invoice_id AS source_record_id,
    TO_JSON_STRING(STRUCT(
        inv.invoice_number AS invoice_number,
        inv.amount_due AS amount_due,
        inv.currency AS currency,
        inv.description AS description
    )) AS payload
FROM (
    SELECT
        JSON_VALUE(payload, '$.invoice_id') AS invoice_id,
        LOWER(JSON_VALUE(payload, '$.customer_email')) AS customer_email,
        JSON_VALUE(payload, '$.status') AS status,
        JSON_VALUE(payload, '$.event_time') AS event_time,
        JSON_VALUE(payload, '$.invoice_number') AS invoice_number,
        JSON_VALUE(payload, '$.amount_due') AS amount_due,
        JSON_VALUE(payload, '$.currency') AS currency,
        JSON_VALUE(payload, '$.description') AS description,
        ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.invoice_id') ORDER BY created_at DESC) AS rn
    FROM `{project}.{dataset}.canonical_objects`
    WHERE canonical_schema = 'iglu:org.canonical/invoice/jsonschema/1-0-0'
) inv
LEFT JOIN therapy_clients c ON inv.customer_email = c.email
WHERE inv.rn = 1

UNION ALL

-- Source 6: stripe_payment (Stripe payments)
-- NOTE: Deduped by payment_id due to duplicate connections + legacy idem_key suffixes
SELECT
    c.client_id AS contact_id,
    cust.email AS contact_email,
    'payment' AS event_type,
    pmt.status AS event_name,
    'stripe' AS event_source,
    TIMESTAMP(pmt.event_time) AS event_timestamp,
    'stripe_payment' AS source_system,
    pmt.payment_id AS source_record_id,
    TO_JSON_STRING(STRUCT(
        pmt.amount AS amount,
        pmt.currency AS currency,
        pmt.payment_method_type AS payment_method_type
    )) AS payload
FROM (
    SELECT
        JSON_VALUE(payload, '$.payment_id') AS payment_id,
        JSON_VALUE(payload, '$.customer_id') AS customer_id,
        JSON_VALUE(payload, '$.status') AS status,
        JSON_VALUE(payload, '$.event_time') AS event_time,
        JSON_VALUE(payload, '$.amount') AS amount,
        JSON_VALUE(payload, '$.currency') AS currency,
        JSON_VALUE(payload, '$.payment_method_type') AS payment_method_type,
        ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.payment_id') ORDER BY created_at DESC) AS rn
    FROM `{project}.{dataset}.canonical_objects`
    WHERE canonical_schema = 'iglu:org.canonical/payment/jsonschema/1-0-0'
) pmt
LEFT JOIN stripe_customers cust ON pmt.customer_id = cust.customer_id
LEFT JOIN therapy_clients c ON cust.email = c.email
WHERE pmt.rn = 1

UNION ALL

-- Source 7: stripe_refund (Stripe refunds)
-- NOTE: Deduped by refund_id due to duplicate connections
SELECT
    c.client_id AS contact_id,
    cust.email AS contact_email,
    'refund' AS event_type,
    ref.status AS event_name,
    'stripe' AS event_source,
    TIMESTAMP(ref.event_time) AS event_timestamp,
    'stripe_refund' AS source_system,
    ref.refund_id AS source_record_id,
    TO_JSON_STRING(STRUCT(
        ref.amount AS amount,
        ref.currency AS currency,
        ref.reason AS reason
    )) AS payload
FROM (
    SELECT
        JSON_VALUE(payload, '$.refund_id') AS refund_id,
        JSON_VALUE(payload, '$.status') AS status,
        JSON_VALUE(payload, '$.event_time') AS event_time,
        JSON_VALUE(payload, '$.amount') AS amount,
        JSON_VALUE(payload, '$.currency') AS currency,
        JSON_VALUE(payload, '$.reason') AS reason,
        JSON_VALUE(payload, '$.payment_intent_id') AS payment_intent_id,
        ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.refund_id') ORDER BY created_at DESC) AS rn
    FROM `{project}.{dataset}.canonical_objects`
    WHERE canonical_schema = 'iglu:org.canonical/refund/jsonschema/1-0-0'
) ref
LEFT JOIN (
    -- Get customer_id from associated payment_intent (also deduped)
    SELECT payment_id, customer_id
    FROM (
        SELECT
            JSON_VALUE(payload, '$.payment_id') AS payment_id,
            JSON_VALUE(payload, '$.customer_id') AS customer_id,
            ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.payment_id') ORDER BY created_at DESC) AS rn
        FROM `{project}.{dataset}.canonical_objects`
        WHERE canonical_schema = 'iglu:org.canonical/payment/jsonschema/1-0-0'
    )
    WHERE rn = 1
) pmt ON ref.payment_intent_id = pmt.payment_id
LEFT JOIN stripe_customers cust ON pmt.customer_id = cust.customer_id
LEFT JOIN therapy_clients c ON cust.email = c.email
WHERE ref.rn = 1
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


def get_projection_sql(name: str, project: str, dataset: str, **kwargs) -> str:
    """Get projection SQL with project and dataset substituted.

    Args:
        name: Projection name (e.g., 'proj_client_sessions')
        project: GCP project ID
        dataset: BigQuery dataset name
        **kwargs: Additional format parameters (e.g., wal_dataset for PM projections)

    Returns:
        SQL string with placeholders replaced

    Raises:
        KeyError: If projection name not found
    """
    if name not in PROJECTIONS:
        raise KeyError(f"Unknown projection: {name}. Available: {list(PROJECTIONS.keys())}")

    sql_template = PROJECTIONS[name]
    return sql_template.format(project=project, dataset=dataset, **kwargs)
