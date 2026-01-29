"""Tests for molt projection SQL templates."""

import pytest

from lorchestra.sql.molt_projections import (
    CONTEXT_ACTIONS,
    CONTEXT_CALENDAR,
    CONTEXT_EMAILS,
    MOLT_PROJECTIONS,
    get_molt_projection_sql,
)


class TestMoltProjectionSqlRegistry:
    """Tests for the molt projection SQL registry."""

    def test_all_three_projections_registered(self):
        """Registry contains all three context projections."""
        assert "context_emails" in MOLT_PROJECTIONS
        assert "context_calendar" in MOLT_PROJECTIONS
        assert "context_actions" in MOLT_PROJECTIONS
        assert len(MOLT_PROJECTIONS) == 3

    def test_get_unknown_projection_raises(self):
        """get_molt_projection_sql raises KeyError for unknown names."""
        with pytest.raises(KeyError, match="Unknown molt projection"):
            get_molt_projection_sql("nonexistent", "proj", "ds")


class TestContextEmailsSQL:
    """Tests for the context_emails curation query."""

    def test_resolves_project_and_dataset(self):
        """Placeholders are resolved to provided values."""
        sql = get_molt_projection_sql(
            "context_emails",
            project="local-orchestration",
            dataset="canonical",
        )
        assert "`local-orchestration.canonical.canonical_objects`" in sql

    def test_selects_email_fields(self):
        """Query selects expected email fields."""
        sql = CONTEXT_EMAILS
        for field in [
            "email_id",
            "thread_id",
            "sender_email",
            "sender_name",
            "subject",
            "snippet",
            "received_at",
            "is_flagged",
            "is_unread",
            "needs_response",
            "source_system",
            "connection_name",
            "idem_key",
            "projected_at",
        ]:
            assert field in sql, f"Missing field: {field}"

    def test_filters_to_email_jmap_lite_schema(self):
        """Query filters to email_jmap_lite canonical schema."""
        sql = CONTEXT_EMAILS
        assert "email_jmap_lite/jsonschema/1-0-0" in sql

    def test_filters_to_last_14_days(self):
        """Query filters to emails from the last 14 days."""
        sql = CONTEXT_EMAILS
        assert "INTERVAL 14 DAY" in sql

    def test_includes_read_status_fields(self):
        """Query includes flagged and unread status fields."""
        sql = CONTEXT_EMAILS
        assert "$flagged" in sql
        assert "$seen" in sql

    def test_excludes_noreply_from_needs_response(self):
        """needs_response heuristic excludes noreply addresses."""
        sql = CONTEXT_EMAILS
        assert "noreply" in sql
        assert "no-reply" in sql

    def test_no_limit(self):
        """Query has no LIMIT clause."""
        sql = CONTEXT_EMAILS
        assert "LIMIT" not in sql


class TestContextCalendarSQL:
    """Tests for the context_calendar curation query."""

    def test_resolves_project_and_dataset(self):
        """Placeholders are resolved to provided values."""
        sql = get_molt_projection_sql(
            "context_calendar",
            project="local-orchestration",
            dataset="canonical",
        )
        assert "`local-orchestration.canonical.canonical_objects`" in sql

    def test_selects_session_fields(self):
        """Query selects expected session fields."""
        sql = CONTEXT_CALENDAR
        for field in [
            "session_id",
            "title",
            "client_name",
            "session_num",
            "scheduled_start",
            "scheduled_end",
            "duration_minutes",
            "status",
            "session_type",
            "contact_id",
            "idem_key",
            "projected_at",
        ]:
            assert field in sql, f"Missing field: {field}"

    def test_filters_to_clinical_session_schema(self):
        """Query filters to clinical_session canonical schema."""
        sql = CONTEXT_CALENDAR
        assert "clinical_session/jsonschema/2-0-0" in sql

    def test_filters_to_14_day_window(self):
        """Query filters to sessions within 14 days past and future."""
        sql = CONTEXT_CALENDAR
        assert "INTERVAL 14 DAY" in sql
        assert "TIMESTAMP_SUB" in sql
        assert "TIMESTAMP_ADD" in sql

    def test_joins_contact_for_client_name(self):
        """Query joins contact table to get client name."""
        sql = CONTEXT_CALENDAR
        assert "contact/jsonschema/2-0-0" in sql
        assert "client_name" in sql

    def test_no_limit(self):
        """Query has no LIMIT clause."""
        sql = CONTEXT_CALENDAR
        assert "LIMIT" not in sql


class TestContextActionsSQL:
    """Tests for the context_actions curation query."""

    def test_reads_from_molt_chatbot_project(self):
        """Actions query reads from molt-chatbot.molt.actions directly."""
        sql = CONTEXT_ACTIONS
        assert "`molt-chatbot.molt.actions`" in sql

    def test_does_not_use_project_placeholder(self):
        """Actions query does not use {project} placeholder (same-project query)."""
        # Resolve should work without affecting the query
        sql = get_molt_projection_sql(
            "context_actions",
            project="local-orchestration",
            dataset="canonical",
        )
        # Should still reference molt-chatbot, not local-orchestration
        assert "`molt-chatbot.molt.actions`" in sql
        assert "local-orchestration" not in sql

    def test_selects_action_fields(self):
        """Query selects expected action fields."""
        sql = CONTEXT_ACTIONS
        for field in [
            "action_id",
            "user_id",
            "channel",
            "action_type",
            "context",
            "instruction",
            "created_at",
            "status",
            "idem_key",
            "projected_at",
        ]:
            assert field in sql, f"Missing field: {field}"

    def test_includes_pending_filter(self):
        """Query filters to pending and recently completed actions."""
        sql = CONTEXT_ACTIONS
        assert "status = 'pending'" in sql
        assert "INTERVAL 24 HOUR" in sql

    def test_no_limit(self):
        """Query has no LIMIT clause."""
        sql = CONTEXT_ACTIONS
        assert "LIMIT" not in sql

    def test_orders_pending_first(self):
        """Query orders pending actions before completed."""
        sql = CONTEXT_ACTIONS
        assert "CASE WHEN status = 'pending' THEN 0 ELSE 1 END" in sql
