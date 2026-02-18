"""BigQuery SQL projections for PM entity views.

This module contains projection SQL that folds WAL events into current PM entity
state. Each view uses ARRAY_AGG with IGNORE NULLS and latest-wins semantics to
reconstruct entity state from the domain_events table.

Naming convention: view_pm_<entity_type>

Available projections:
    view_pm_projects     - Projects with direct fields
    view_pm_work_items   - Work items with hierarchy resolution (ADR-003)
    view_pm_deliverables - Deliverables with project FK
    view_pm_opsstreams   - Ops streams
    view_pm_artifacts    - Artifacts with container FKs (content truncated)

Data source: domain_events table (storacle WAL convention)
    aggregate_id, aggregate_type, event_type, payload (JSON), event_id, occurred_at

All views include:
    row_id      = aggregate_id
    row_version = latest event_id
    row_state   = 'synced'

Usage:
    from lorchestra.sql.pm_projections import PM_PROJECTIONS

    sql = PM_PROJECTIONS["view_pm_projects"].format(project="my-proj", dataset="canonical", wal_dataset="wal")
"""

# =============================================================================
# view_pm_projects — Projects with direct fields
# =============================================================================
VIEW_PM_PROJECTS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.view_pm_projects` AS
SELECT
    aggregate_id AS project_id,
    ARRAY_AGG(JSON_VALUE(payload, '$.name') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS name,
    ARRAY_AGG(JSON_VALUE(payload, '$.description') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS description,
    ARRAY_AGG(JSON_VALUE(payload, '$.owner') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS owner,
    ARRAY_AGG(JSON_VALUE(payload, '$.status') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS status,
    ARRAY_AGG(JSON_VALUE(payload, '$.target_end_date') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS target_end_date,
    aggregate_id AS row_id,
    ARRAY_AGG(event_id ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS row_version,
    'synced' AS row_state
FROM `{project}.{wal_dataset}.domain_events`
WHERE aggregate_type = 'project'
GROUP BY aggregate_id
"""

# =============================================================================
# view_pm_deliverables — Deliverables with project FK
# =============================================================================
VIEW_PM_DELIVERABLES = """
CREATE OR REPLACE VIEW `{project}.{dataset}.view_pm_deliverables` AS
SELECT
    aggregate_id AS deliverable_id,
    ARRAY_AGG(JSON_VALUE(payload, '$.project_id') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS project_id,
    ARRAY_AGG(JSON_VALUE(payload, '$.name') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS name,
    ARRAY_AGG(JSON_VALUE(payload, '$.description') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS description,
    ARRAY_AGG(JSON_VALUE(payload, '$.status') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS status,
    ARRAY_AGG(JSON_VALUE(payload, '$.acceptance_criteria') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS acceptance_criteria,
    ARRAY_AGG(JSON_VALUE(payload, '$.due_date') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS due_date,
    aggregate_id AS row_id,
    ARRAY_AGG(event_id ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS row_version,
    'synced' AS row_state
FROM `{project}.{wal_dataset}.domain_events`
WHERE aggregate_type = 'deliverable'
GROUP BY aggregate_id
"""

# =============================================================================
# view_pm_opsstreams — Ops streams
# =============================================================================
VIEW_PM_OPSSTREAMS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.view_pm_opsstreams` AS
SELECT
    aggregate_id AS opsstream_id,
    ARRAY_AGG(JSON_VALUE(payload, '$.name') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS name,
    ARRAY_AGG(JSON_VALUE(payload, '$.type') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS type,
    ARRAY_AGG(JSON_VALUE(payload, '$.owner') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS owner,
    ARRAY_AGG(JSON_VALUE(payload, '$.status') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS status,
    ARRAY_AGG(JSON_VALUE(payload, '$.description') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS description,
    aggregate_id AS row_id,
    ARRAY_AGG(event_id ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS row_version,
    'synced' AS row_state
FROM `{project}.{wal_dataset}.domain_events`
WHERE aggregate_type = 'opsstream'
GROUP BY aggregate_id
"""

# =============================================================================
# view_pm_work_items — Work items with hierarchy resolution (ADR-003)
#
# Direct fields come from WAL folding. Effective container fields are resolved
# at read time via LEFT JOINs to deliverables and projects views.
# =============================================================================
VIEW_PM_WORK_ITEMS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.view_pm_work_items` AS
WITH work_items_folded AS (
    SELECT
        aggregate_id AS work_item_id,
        ARRAY_AGG(JSON_VALUE(payload, '$.title') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS title,
        ARRAY_AGG(JSON_VALUE(payload, '$.kind') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS kind,
        ARRAY_AGG(JSON_VALUE(payload, '$.state') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS state,
        ARRAY_AGG(JSON_VALUE(payload, '$.priority') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS priority,
        ARRAY_AGG(JSON_VALUE(payload, '$.severity') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS severity,
        ARRAY_AGG(JSON_QUERY(payload, '$.assignees') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS assignees,
        ARRAY_AGG(JSON_QUERY(payload, '$.labels') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS labels,
        ARRAY_AGG(JSON_VALUE(payload, '$.due_at') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS due_at,
        ARRAY_AGG(JSON_VALUE(payload, '$.project_id') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS project_id,
        ARRAY_AGG(JSON_VALUE(payload, '$.deliverable_id') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS deliverable_id,
        ARRAY_AGG(JSON_VALUE(payload, '$.opsstream_id') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS opsstream_id,
        ARRAY_AGG(JSON_VALUE(payload, '$.parent_id') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS parent_id,
        aggregate_id AS row_id,
        ARRAY_AGG(event_id ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS row_version,
        'synced' AS row_state
    FROM `{project}.{wal_dataset}.domain_events`
    WHERE aggregate_type = 'work_item'
    GROUP BY aggregate_id
)
SELECT
    wi.work_item_id,
    wi.title,
    wi.kind,
    wi.state,
    wi.priority,
    wi.severity,
    wi.assignees,
    wi.labels,
    wi.due_at,
    wi.project_id,
    wi.deliverable_id,
    wi.opsstream_id,
    wi.parent_id,
    -- Effective project resolved via hierarchy (ADR-003)
    COALESCE(wi.project_id, del.project_id) AS effective_project_id,
    wi.row_id,
    wi.row_version,
    wi.row_state
FROM work_items_folded wi
LEFT JOIN `{project}.{dataset}.view_pm_deliverables` del
    ON wi.deliverable_id = del.deliverable_id
"""

# =============================================================================
# view_pm_artifacts — Artifacts with container FKs
#
# Content is truncated to 500 chars for Sheets cell limit.
# Uses JSON_QUERY for tags (array field).
# =============================================================================
VIEW_PM_ARTIFACTS = """
CREATE OR REPLACE VIEW `{project}.{dataset}.view_pm_artifacts` AS
SELECT
    aggregate_id AS artifact_id,
    ARRAY_AGG(JSON_VALUE(payload, '$.title') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS title,
    ARRAY_AGG(JSON_VALUE(payload, '$.kind') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS kind,
    ARRAY_AGG(JSON_VALUE(payload, '$.status') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS status,
    SUBSTR(
        ARRAY_AGG(JSON_VALUE(payload, '$.content') IGNORE NULLS
            ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)],
        1, 500
    ) AS content,
    ARRAY_AGG(JSON_VALUE(payload, '$.content_ref') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS content_ref,
    ARRAY_AGG(JSON_VALUE(payload, '$.delivered_via') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS delivered_via,
    ARRAY_AGG(JSON_QUERY(payload, '$.tags') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS tags,
    ARRAY_AGG(JSON_VALUE(payload, '$.work_item_id') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS work_item_id,
    ARRAY_AGG(JSON_VALUE(payload, '$.deliverable_id') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS deliverable_id,
    ARRAY_AGG(JSON_VALUE(payload, '$.project_id') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS project_id,
    ARRAY_AGG(JSON_VALUE(payload, '$.opsstream_id') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS opsstream_id,
    ARRAY_AGG(JSON_VALUE(payload, '$.contact_ref') IGNORE NULLS
        ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS contact_ref,
    aggregate_id AS row_id,
    ARRAY_AGG(event_id ORDER BY occurred_at DESC, event_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS row_version,
    'synced' AS row_state
FROM `{project}.{wal_dataset}.domain_events`
WHERE aggregate_type = 'artifact'
GROUP BY aggregate_id
"""

# Registry of all PM projections
PM_PROJECTIONS: dict[str, str] = {
    "view_pm_projects": VIEW_PM_PROJECTS,
    "view_pm_work_items": VIEW_PM_WORK_ITEMS,
    "view_pm_deliverables": VIEW_PM_DELIVERABLES,
    "view_pm_opsstreams": VIEW_PM_OPSSTREAMS,
    "view_pm_artifacts": VIEW_PM_ARTIFACTS,
}
